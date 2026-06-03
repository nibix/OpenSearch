/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.engine;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.encryption.PmeContext;
import org.opensearch.parquet.encryption.PmeFileEncryptionInputs;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.parquet.writer.ParquetWriter;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Per-shard Parquet indexing execution engine.
 *
 * <p>Implements {@link IndexingExecutionEngine} to integrate with OpenSearch's data format
 * framework. Each shard gets its own engine instance, which manages:
 * <ul>
 *   <li>A shared {@link ArrowBufferPool} for Arrow memory allocation across all writers.</li>
 *   <li>Writer creation per writer generation, each producing a separate Parquet file.</li>
 *   <li>Native memory usage reporting (Arrow allocations + Rust-side allocations).</li>
 *   <li>Optional PME encryption via {@link PmeContext} (one per engine, null if unencrypted).</li>
 * </ul>
 */
public class ParquetIndexingEngine implements IndexingExecutionEngine<ParquetDataFormat, ParquetDocumentInput> {

    private static final Logger logger = LogManager.getLogger(ParquetIndexingEngine.class);

    public static final String FILE_NAME_PREFIX = "_parquet_file_generation";
    public static final String FILE_NAME_EXT = ".parquet";

    private final ParquetDataFormat dataFormat;
    private final ShardPath shardPath;
    private final Supplier<Schema> schemaSupplier;
    private final ArrowBufferPool bufferPool;
    private final Settings settings;
    private final ThreadPool threadPool;
    private final FormatChecksumStrategy checksumStrategy;
    /** Non-null when the index is configured for PME encryption; null otherwise. */
    private final PmeContext pmeContext;

    public ParquetIndexingEngine(
        Settings settings,
        ParquetDataFormat dataFormat,
        ShardPath shardPath,
        Supplier<Schema> schemaSupplier,
        IndexSettings indexSettings,
        ThreadPool threadPool
    ) {
        this(settings, dataFormat, shardPath, schemaSupplier, indexSettings, threadPool, new PrecomputedChecksumStrategy());
    }

    public ParquetIndexingEngine(
        Settings settings,
        ParquetDataFormat dataFormat,
        ShardPath shardPath,
        Supplier<Schema> schemaSupplier,
        IndexSettings indexSettings,
        ThreadPool threadPool,
        FormatChecksumStrategy checksumStrategy
    ) {
        this.dataFormat = dataFormat;
        this.shardPath = shardPath;
        this.schemaSupplier = schemaSupplier;
        this.bufferPool = new ArrowBufferPool(settings);
        this.settings = settings;
        this.threadPool = threadPool;
        this.checksumStrategy = checksumStrategy;
        this.pmeContext = initializePmeContext(indexSettings, shardPath);
        try {
            Files.createDirectory(shardPath.resolve("parquet"));
        } catch (FileAlreadyExistsException ex) {
            logger.warn("Directory already exists: {}", shardPath.resolve("parquet"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FormatChecksumStrategy getChecksumStrategy() {
        return checksumStrategy;
    }

    @Override
    public Writer<ParquetDocumentInput> createWriter(long writerGeneration) {
        Path filePath = Path.of(
            shardPath.getDataPath().toString(),
            dataFormat.name(),
            FILE_NAME_PREFIX + "_" + writerGeneration + FILE_NAME_EXT
        );
        PmeFileEncryptionInputs encryptionInputs = null;
        if (pmeContext != null) {
            try {
                encryptionInputs = pmeContext.createFileEncryptionInputs();
            } catch (IOException e) {
                throw new RuntimeException("Failed to create PME encryption inputs for " + filePath, e);
            }
        }
        return new ParquetWriter(
            filePath.toString(),
            writerGeneration,
            dataFormat,
            schemaSupplier.get(),
            bufferPool,
            settings,
            threadPool,
            checksumStrategy,
            encryptionInputs
        );
    }

    @Override
    public long getNativeBytesUsed() {
        return bufferPool.getTotalAllocatedBytes() + RustBridge.getFilteredNativeBytesUsed(shardPath.getDataPath().toString());
    }

    @Override
    public Merger getMerger() {
        return null;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        if (refreshInput == null) {
            return new RefreshResult(List.of());
        }
        List<Segment> segments = new ArrayList<>();
        segments.addAll(refreshInput.existingSegments());
        segments.addAll(refreshInput.writerFiles());
        return new RefreshResult(List.copyOf(segments));
    }

    @Override
    public long getNextWriterGeneration() {
        throw new UnsupportedOperationException("getNextWriterGeneration not supported");
    }

    @Override
    public ParquetDataFormat getDataFormat() {
        return dataFormat;
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
        Collection<String> parquetFiles = filesToDelete.get(dataFormat.name());
        if (parquetFiles == null) {
            return;
        }
        for (String fileName : parquetFiles) {
            Path filePath = Path.of(fileName);
            logger.debug("Deleting parquet file: {}", filePath);
            boolean deleted = Files.deleteIfExists(filePath);
            if (deleted == false) {
                logger.warn("Failed to delete parquet file: {}", filePath);
            }
        }
    }

    @Override
    public ParquetDocumentInput newDocumentInput() {
        return new ParquetDocumentInput();
    }

    @Override
    public IndexStoreProvider getProvider() {
        return null;
    }

    @Override
    public void close() throws IOException {
        bufferPool.close();
        if (pmeContext != null) {
            pmeContext.evict();
        }
    }

    /**
     * Initializes the per-engine {@link PmeContext} if the index is configured for encryption.
     * Returns {@code null} for unencrypted indices.
     *
     * <p>The index-level keyfile (at {@code <index-data-dir>/keyfile}) is created atomically
     * on first call; concurrent shard engines race on {@code CREATE_NEW}, the loser reads the
     * winner's file.
     */
    private static PmeContext initializePmeContext(IndexSettings indexSettings, ShardPath shardPath) {
        if (indexSettings == null) {
            return null;
        }
        // Index-level data directory: parent of all shard directories.
        // shardPath.getDataPath() = .../indices/{uuid}/{shardId}/index
        //   -> .getParent() = .../indices/{uuid}/{shardId}
        //   -> .getParent().getParent() = .../indices/{uuid}  (index-level dir)
        Path indexDataPath = shardPath.getDataPath().getParent().getParent();
        try {
            return PmeContext.create(indexSettings, indexDataPath, shardPath.getShardId().id());
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize PME context for index " + indexSettings.getIndex().getUUID(), e);
        }
    }
}
