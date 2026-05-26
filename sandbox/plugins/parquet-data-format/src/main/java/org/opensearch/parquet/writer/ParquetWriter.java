/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.encryption.PmeFileEncryptionInputs;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.vsr.VSRManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Parquet file writer integrating OpenSearch's {@link Writer} interface with the VSR batching layer.
 *
 * <p>Each instance corresponds to a single Parquet file for a given writer generation.
 * Documents are accepted via {@link #addDoc(ParquetDocumentInput)}, batched in Arrow vectors
 * by the {@link VSRManager}, and flushed to a Parquet file via the native Rust writer.
 *
 * <p>Writer-level settings (e.g., {@code parquet.max_rows_per_vsr}) are extracted from
 * the {@link Settings} passed at construction time and propagated to the VSR layer.
 *
 * <p>The returned {@link FileInfos} from {@link #flush()} contains the file path, writer
 * generation, and row count for downstream commit tracking.
 */
public class ParquetWriter implements Writer<ParquetDocumentInput> {

    private final String file;
    private final long writerGeneration;
    private final ParquetDataFormat dataFormat;
    private final VSRManager vsrManager;
    private final FormatChecksumStrategy checksumStrategy;
    private final boolean encrypted;

    /**
     * Creates a new ParquetWriter for an unencrypted file.
     */
    public ParquetWriter(
        String file,
        long writerGeneration,
        ParquetDataFormat dataFormat,
        Schema schema,
        ArrowBufferPool bufferPool,
        Settings settings,
        ThreadPool threadPool,
        FormatChecksumStrategy checksumStrategy
    ) {
        this(file, writerGeneration, dataFormat, schema, bufferPool, settings, threadPool, checksumStrategy, null);
    }

    /**
     * Creates a new ParquetWriter with optional PME encryption.
     *
     * <p>If {@code encryptionInputs} is non-null, the footer key is zeroed immediately after
     * the native writer is initialized inside {@link VSRManager}.
     *
     * @param encryptionInputs per-file PME inputs; {@code null} writes an unencrypted file
     */
    public ParquetWriter(
        String file,
        long writerGeneration,
        ParquetDataFormat dataFormat,
        Schema schema,
        ArrowBufferPool bufferPool,
        Settings settings,
        ThreadPool threadPool,
        FormatChecksumStrategy checksumStrategy,
        PmeFileEncryptionInputs encryptionInputs
    ) {
        this.file = file;
        this.writerGeneration = writerGeneration;
        this.dataFormat = dataFormat;
        this.vsrManager = new VSRManager(
            file, schema, bufferPool,
            ParquetSettings.MAX_ROWS_PER_VSR.get(settings),
            threadPool, encryptionInputs
        );
        this.checksumStrategy = checksumStrategy;
        this.encrypted = encryptionInputs != null;
    }

    @Override
    public WriteResult addDoc(ParquetDocumentInput d) throws IOException {
        vsrManager.addDocument(d);
        return new WriteResult.Success(1L, 1L, 1L);
    }

    @Override
    public FileInfos flush() throws IOException {
        ParquetFileMetadata metadata = vsrManager.flush();
        if (file == null || metadata == null || metadata.numRows() == 0) {
            return FileInfos.empty();
        }
        Path filePath = Path.of(file);
        String fileName = filePath.getFileName().toString();

        if (checksumStrategy != null && metadata.crc32() != 0) {
            checksumStrategy.registerChecksum(fileName, metadata.crc32(), writerGeneration);
        }

        WriterFileSet.Builder builder = WriterFileSet.builder()
            .directory(filePath.getParent().getFileName())
            .writerGeneration(writerGeneration)
            .addFile(fileName)
            .addNumRows(metadata.numRows());

        if (encrypted) {
            builder = builder.addFileMetadata(fileName, java.util.Map.of("opensearch.pme.encrypted", "true"));
        }

        return FileInfos.builder().putWriterFileSet(dataFormat, builder.build()).build();
    }

    @Override
    public void sync() throws IOException {
        vsrManager.sync();
    }

    @Override
    public long generation() {
        return writerGeneration;
    }

    @Override
    public void lock() {}

    @Override
    public boolean tryLock() {
        return false;
    }

    @Override
    public void unlock() {}

    @Override
    public void close() throws IOException {
        vsrManager.close();
    }
}
