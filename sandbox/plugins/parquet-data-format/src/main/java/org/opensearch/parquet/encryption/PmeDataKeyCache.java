/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-level cache for PME index data keys.
 *
 * <p>Keyed by the absolute index data path string (same convention as
 * {@code DatafusionReaderManager}). On eviction, the internal key material is
 * zeroed via {@link PmeDataKey#zero()}.
 *
 * <p>The cache is a static singleton initialised by {@link #initialize()}, which is called
 * once from {@link org.opensearch.parquet.ParquetDataFormatPlugin#createComponents}.
 *
 * <p>TODO: The Lucene storage-encryption plugin ({@code NodeLevelKeyCache} /
 * {@code MasterKeyHealthMonitor}) adds proactive KMS health monitoring on top of a
 * similar cache: it runs a periodic background check for all encrypted indices, applies
 * index-level read/write blocks when the KMS is unreachable, and removes them
 * automatically once the KMS recovers. Consider adding equivalent functionality here:
 * <ul>
 *   <li>A configurable TTL ({@code expireAfterWrite}) so that a KMS outage is detected
 *       at the next background refresh rather than only on the next cold cache miss.</li>
 *   <li>A background health-check thread that proactively re-loads all cached data keys
 *       at the configured interval.</li>
 *   <li>Block management ({@code index.blocks.read} / {@code index.blocks.write}) when
 *       the KMS cannot be reached and the cached key has expired.</li>
 *   <li>Automatic block removal and shard-retry trigger upon KMS recovery.</li>
 * </ul>
 */
final class PmeDataKeyCache {

    private static final ConcurrentHashMap<String, PmeDataKey> CACHE = new ConcurrentHashMap<>();

    private PmeDataKeyCache() {}

    /**
     * Called once during plugin startup. Currently a no-op because the cache
     * is statically initialised, but kept as an explicit lifecycle hook so
     * future configuration (e.g. TTL) can be wired in here.
     */
    static void initialize() {
        // intentionally empty – static cache is already ready
    }

    /**
     * Returns the cached {@link PmeDataKey} for {@code cacheKey}, or loads it via
     * {@code loader}, caches it, and returns it.
     *
     * <p>The raw key bytes returned by {@code loader} are defensively zeroed immediately
     * after the {@link PmeDataKey} is constructed.
     *
     * @param cacheKey index data path string
     * @param loader   called exactly once on cache miss
     * @return the cached or freshly loaded data key
     * @throws IOException if the loader fails
     */
    static PmeDataKey getOrLoad(String cacheKey, DataKeyLoader loader) throws IOException {
        PmeDataKey existing = CACHE.get(cacheKey);
        if (existing != null) {
            return existing;
        }
        synchronized (CACHE) {
            existing = CACHE.get(cacheKey);
            if (existing != null) {
                return existing;
            }
            byte[] rawKey = loader.load();
            try {
                PmeDataKey key = new PmeDataKey(rawKey);
                CACHE.put(cacheKey, key);
                return key;
            } finally {
                Arrays.fill(rawKey, (byte) 0);
            }
        }
    }

    /**
     * Removes the data key for {@code cacheKey} from the cache and zeros its
     * internal key material.  Should be called when the engine (shard) is closed.
     *
     * @param cacheKey index data path string
     */
    static void evict(String cacheKey) {
        PmeDataKey removed = CACHE.remove(cacheKey);
        if (removed != null) {
            removed.zero();
        }
    }

    /**
     * Supplier that may throw {@link IOException}, used for loading key material
     * from the keyfile on a cache miss.
     */
    @FunctionalInterface
    interface DataKeyLoader {
        byte[] load() throws IOException;
    }
}


