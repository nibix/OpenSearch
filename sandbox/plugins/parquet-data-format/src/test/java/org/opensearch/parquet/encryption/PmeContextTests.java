/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PmeContext}.
 *
 * <p>Because {@link PmeContext} normally calls {@link PmeKeyfileManager#initOrLoad(Path, org.opensearch.cluster.metadata.CryptoMetadata)}
 * which requires a live {@link org.opensearch.crypto.CryptoHandlerRegistry}, we use
 * {@link PmeContext#createForTest} (a package-private factory) that injects a
 * {@link MasterKeyProvider} directly, bypassing the registry.
 */
public class PmeContextTests extends OpenSearchTestCase {

    private static final int KEY_LEN = PmeKeyDerivation.DATA_KEY_BYTES;

    private Path indexDataPath;
    private MasterKeyProvider provider;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexDataPath = createTempDir();
        provider = mockProvider(makeRawKey((byte) 0x55), new byte[] { 1, 2, 3 });
    }

    @Override
    public void tearDown() throws Exception {
        PmeDataKeyCache.evict(indexDataPath.toString());
        super.tearDown();
    }

    // ---- helpers ----

    private static byte[] makeRawKey(byte fill) {
        byte[] k = new byte[KEY_LEN];
        Arrays.fill(k, fill);
        return k;
    }

    private static MasterKeyProvider mockProvider(byte[] rawKey, byte[] encryptedKey) throws IOException {
        MasterKeyProvider p = mock(MasterKeyProvider.class);
        when(p.generateDataPair()).thenReturn(new DataKeyPair(rawKey.clone(), encryptedKey.clone()));
        when(p.decryptKey(any())).thenReturn(rawKey.clone());
        return p;
    }

    private static IndexSettings encryptedIndexSettings() {
        Settings settings = Settings.builder()
            .put("index.store.crypto.key_provider", "test-provider")
            .put("index.store.crypto.key_provider_type", "mock")
            .build();
        return IndexSettingsModule.newIndexSettings("test-index", settings);
    }

    private static IndexSettings plainIndexSettings() {
        return IndexSettingsModule.newIndexSettings("test-index", Settings.EMPTY);
    }

    // ---- create ----

    public void testCreateReturnsNullForNullIndexSettings() throws IOException {
        assertNull(PmeContext.create(null, indexDataPath));
    }

    public void testCreateReturnsNullWhenEncryptionNotConfigured() throws IOException {
        assertNull(PmeContext.create(plainIndexSettings(), indexDataPath));
    }

    public void testCreateForTestReturnsContextAndPopulatesCache() throws IOException {
        PmeContext ctx = PmeContext.createForTest(encryptedIndexSettings(), indexDataPath, provider);
        assertNotNull(ctx);

        // Subsequent getOrLoad must be a cache hit — loader must NOT be called.
        AtomicBoolean loaderCalled = new AtomicBoolean(false);
        PmeDataKeyCache.getOrLoad(indexDataPath.toString(), () -> {
            loaderCalled.set(true);
            return makeRawKey((byte) 0);
        });
        assertFalse("cache must already contain the key after create()", loaderCalled.get());
    }

    // ---- createFileEncryptionInputs ----

    public void testCreateFileEncryptionInputsReturns32ByteFooterKey() throws IOException {
        PmeContext ctx = PmeContext.createForTest(encryptedIndexSettings(), indexDataPath, provider);
        assertNotNull(ctx);

        PmeFileEncryptionInputs inputs = ctx.createFileEncryptionInputs();
        assertNotNull(inputs);

        byte[] footerKey = inputs.footerKey();
        try {
            assertEquals(KEY_LEN, footerKey.length);
        } finally {
            Arrays.fill(footerKey, (byte) 0);
            inputs.zero();
        }
    }

    // ---- evict ----

    public void testEvictZerosCachedKeyMaterial() throws IOException {
        PmeContext ctx = PmeContext.createForTest(encryptedIndexSettings(), indexDataPath, provider);
        assertNotNull(ctx);

        PmeDataKey keyBefore = PmeDataKeyCache.getOrLoad(indexDataPath.toString(), () -> makeRawKey((byte) 0));
        ctx.evict();

        byte[] afterEviction = keyBefore.bytes();
        try {
            assertArrayEquals("evicted key must be zeroed", new byte[KEY_LEN], afterEviction);
        } finally {
            Arrays.fill(afterEviction, (byte) 0);
        }
    }

    public void testEvictThenCreateReInitializesContext() throws IOException {
        PmeContext ctx = PmeContext.createForTest(encryptedIndexSettings(), indexDataPath, provider);
        assertNotNull(ctx);
        ctx.evict();

        MasterKeyProvider provider2 = mockProvider(makeRawKey((byte) 0x66), new byte[] { 9 });
        PmeContext ctx2 = PmeContext.createForTest(encryptedIndexSettings(), indexDataPath, provider2);
        assertNotNull(ctx2);

        PmeFileEncryptionInputs inputs = ctx2.createFileEncryptionInputs();
        assertNotNull(inputs);
        inputs.zero();
    }
}

