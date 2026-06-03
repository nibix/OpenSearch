/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.index.IndexSettings;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Per-engine PME (Parquet Modular Encryption) context.
 *
 * <p>This is the public entry point for callers. Each {@code ParquetIndexingEngine}
 * holds one instance (or {@code null} for unencrypted indices). The context ties
 * together:
 * <ul>
 *   <li>The index-level {@link CryptoMetadata} (key provider type and name).</li>
 *   <li>The index data path, which is the cache key in {@link PmeDataKeyCache} and
 *       the directory that holds the {@code keyfile}.</li>
 * </ul>
 *
 * <p>The data key itself lives in the node-level {@link PmeDataKeyCache}; this class
 * never holds raw key bytes directly.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #create(IndexSettings, Path)} — called once per engine at shard open time.</li>
 *   <li>{@link #createFileEncryptionInputs()} — called once per Parquet file to be written.</li>
 *   <li>{@link #evict()} — called when the engine is closed; zeroes cached key material.</li>
 * </ol>
 */
public final class PmeContext {

    private static final Logger logger = LogManager.getLogger(PmeContext.class);

    private final CryptoMetadata cryptoMetadata;
    private final Path indexDataPath;
    /** Cache key used in {@link PmeDataKeyCache}: the absolute index data path string. */
    private final String cacheKey;

    private PmeContext(CryptoMetadata cryptoMetadata, Path indexDataPath) {
        this.cryptoMetadata = cryptoMetadata;
        this.indexDataPath = indexDataPath;
        this.cacheKey = indexDataPath.toString();
    }

    /**
     * Package-private factory for unit tests: accepts a pre-built {@link MasterKeyProvider}
     * so that {@link org.opensearch.crypto.CryptoHandlerRegistry} need not be initialised.
     */
    static PmeContext createForTest(IndexSettings indexSettings, Path indexDataPath, MasterKeyProvider provider) throws IOException {
        if (indexSettings == null) {
            return null;
        }
        CryptoMetadata cryptoMetadata = CryptoMetadata.fromIndexSettings(indexSettings.getSettings());
        if (cryptoMetadata == null) {
            return null;
        }
        PmeContext ctx = new PmeContext(cryptoMetadata, indexDataPath);
        PmeDataKeyCache.getOrLoad(ctx.cacheKey, () -> PmeKeyfileManager.initOrLoad(indexDataPath, provider));
        return ctx;
    }

    /**
     * Creates a {@link PmeContext} if the index is configured for PME encryption, or
     * returns {@code null} for unencrypted indices.
     *
     * <p>On first call for a given index, the keyfile is created via
     * {@link PmeKeyfileManager#initOrLoad} and the data key is loaded into
     * {@link PmeDataKeyCache}. Subsequent calls (from other shards of the same index)
     * hit the cache directly.
     *
     * @param indexSettings the index settings; {@code null} → returns {@code null}
     * @param indexDataPath index-level data directory (parent of shard directories)
     * @return context, or {@code null} if encryption is not configured
     * @throws IOException if keyfile initialisation or key loading fails
     */
    public static PmeContext create(IndexSettings indexSettings, Path indexDataPath) throws IOException {
        if (indexSettings == null) {
            return null;
        }
        CryptoMetadata cryptoMetadata = CryptoMetadata.fromIndexSettings(indexSettings.getSettings());
        if (cryptoMetadata == null) {
            return null;
        }
        PmeContext ctx = new PmeContext(cryptoMetadata, indexDataPath);
        // Eagerly populate the cache so errors surface at shard-open time, not at write time.
        PmeDataKeyCache.getOrLoad(ctx.cacheKey, () -> PmeKeyfileManager.initOrLoad(indexDataPath, cryptoMetadata));
        logger.debug("PME context initialised for [{}]", indexDataPath);
        return ctx;
    }

    /**
     * Creates per-file PME encryption inputs for a new Parquet file.
     *
     * <p>Retrieves the data key from {@link PmeDataKeyCache} (cache hit in the normal case),
     * then delegates to {@link PmeFileEncryptionInputs#create(PmeDataKey)}.
     * The returned inputs hold derived key material; the caller (
     * {@code NativeParquetWriter}) is responsible for calling
     * {@link PmeFileEncryptionInputs#zero()} immediately after the native writer
     * has consumed them.
     *
     * @return single-use per-file encryption inputs
     * @throws IOException if the data key cannot be loaded
     */
    public PmeFileEncryptionInputs createFileEncryptionInputs() throws IOException {
        PmeDataKey dataKey = PmeDataKeyCache.getOrLoad(
            cacheKey,
            () -> PmeKeyfileManager.initOrLoad(indexDataPath, cryptoMetadata)
        );
        return PmeFileEncryptionInputs.create(dataKey);
    }

    /**
     * Evicts this index's data key from the node-level cache, zeroing key material.
     * Must be called when the engine is closed (i.e. from {@code ParquetIndexingEngine#close}).
     */
    public void evict() {
        PmeDataKeyCache.evict(cacheKey);
        logger.debug("PME data key evicted for [{}]", indexDataPath);
    }
}
