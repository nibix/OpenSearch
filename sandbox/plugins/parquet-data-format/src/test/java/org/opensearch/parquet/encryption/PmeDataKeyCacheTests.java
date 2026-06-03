/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class PmeDataKeyCacheTests extends OpenSearchTestCase {

    private static final int KEY_LEN = PmeKeyDerivation.DATA_KEY_BYTES;

    @Override
    public void tearDown() throws Exception {
        // Evict any keys inserted by the test so the static cache doesn't leak.
        PmeDataKeyCache.evict("test-key-a");
        PmeDataKeyCache.evict("test-key-b");
        super.tearDown();
    }

    // ---- getOrLoad ----

    public void testGetOrLoadCallsLoaderOnCacheMiss() throws IOException {
        byte[] rawKey = new byte[KEY_LEN];
        Arrays.fill(rawKey, (byte) 7);

        PmeDataKey result = PmeDataKeyCache.getOrLoad("test-key-a", () -> rawKey.clone());

        // The returned key should contain the same bytes.
        byte[] got = result.bytes();
        try {
            assertArrayEquals(rawKey, got);
        } finally {
            Arrays.fill(got, (byte) 0);
        }
    }

    public void testGetOrLoadReturnsCachedValueOnSecondCall() throws IOException {
        AtomicInteger loaderCalls = new AtomicInteger(0);
        byte[] rawKey = new byte[KEY_LEN];

        PmeDataKey first = PmeDataKeyCache.getOrLoad("test-key-a", () -> {
            loaderCalls.incrementAndGet();
            return rawKey.clone();
        });

        PmeDataKey second = PmeDataKeyCache.getOrLoad("test-key-a", () -> {
            loaderCalls.incrementAndGet();
            return rawKey.clone();
        });

        assertEquals("loader must be called exactly once", 1, loaderCalls.get());
        assertSame("must return the identical cached instance", first, second);
    }

    public void testGetOrLoadZerosRawKeyAfterConstruction() throws IOException {
        // We pass in a sentinel array; after getOrLoad the cache must have zeroed it.
        byte[] sentinel = new byte[KEY_LEN];
        Arrays.fill(sentinel, (byte) 0x55);
        byte[] copy = sentinel.clone();

        PmeDataKeyCache.getOrLoad("test-key-a", () -> sentinel);

        // sentinel should be zeroed by the cache.
        byte[] zeros = new byte[KEY_LEN];
        assertArrayEquals("loader array must be zeroed after caching", zeros, sentinel);

        // But the cached key should still hold the original bytes.
        PmeDataKey cached = PmeDataKeyCache.getOrLoad("test-key-a", () -> { throw new IOException("should not be called"); });
        byte[] got = cached.bytes();
        try {
            assertArrayEquals(copy, got);
        } finally {
            Arrays.fill(got, (byte) 0);
        }
    }

    // ---- evict ----

    public void testEvictZerosKeyMaterialAndForcesReload() throws IOException {
        byte[] rawKey = new byte[KEY_LEN];
        Arrays.fill(rawKey, (byte) 42);

        PmeDataKey inserted = PmeDataKeyCache.getOrLoad("test-key-a", () -> rawKey.clone());
        PmeDataKeyCache.evict("test-key-a");

        // After eviction the internal bytes should be zeroed.
        byte[] afterEviction = inserted.bytes();
        try {
            byte[] zeros = new byte[KEY_LEN];
            assertArrayEquals("evicted key must be zeroed", zeros, afterEviction);
        } finally {
            Arrays.fill(afterEviction, (byte) 0);
        }

        // A subsequent getOrLoad should invoke the loader again.
        AtomicInteger reloadCount = new AtomicInteger(0);
        PmeDataKeyCache.getOrLoad("test-key-a", () -> {
            reloadCount.incrementAndGet();
            return rawKey.clone();
        });
        assertEquals("loader must be called again after eviction", 1, reloadCount.get());
    }

    public void testEvictNonExistentKeyIsNoOp() {
        // Must not throw.
        PmeDataKeyCache.evict("does-not-exist");
    }

    // ---- isolation ----

    public void testDifferentCacheKeysAreIndependent() throws IOException {
        byte[] keyA = new byte[KEY_LEN];
        Arrays.fill(keyA, (byte) 1);
        byte[] keyB = new byte[KEY_LEN];
        Arrays.fill(keyB, (byte) 2);

        PmeDataKey a = PmeDataKeyCache.getOrLoad("test-key-a", () -> keyA.clone());
        PmeDataKey b = PmeDataKeyCache.getOrLoad("test-key-b", () -> keyB.clone());

        assertNotSame(a, b);

        byte[] gotA = a.bytes();
        byte[] gotB = b.bytes();
        try {
            assertArrayEquals(keyA, gotA);
            assertArrayEquals(keyB, gotB);
        } finally {
            Arrays.fill(gotA, (byte) 0);
            Arrays.fill(gotB, (byte) 0);
        }

        // Evicting A must not affect B.
        PmeDataKeyCache.evict("test-key-a");
        byte[] bAfter = b.bytes();
        try {
            assertArrayEquals("evicting A must not zero B", keyB, bAfter);
        } finally {
            Arrays.fill(bAfter, (byte) 0);
        }
    }
}

