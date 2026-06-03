/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.crypto.CryptoHandlerRegistry;
import org.opensearch.crypto.CryptoRegistryException;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.CryptoKeyProviderPlugin;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Manages {@link DatafusionReader} instances per shard.
 * <p>
 * On refresh, a new reader is created from the updated catalog snapshot.
 * File lifecycle events (add/delete) are delegated to the node-level
 * {@link DataFusionService} for cache management.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader> {

    private static final Logger logger = LogManager.getLogger(DatafusionReaderManager.class);

    private final Map<CatalogSnapshot, DatafusionReader> readers = new HashMap<>();
    private final DataFormat dataFormat;
    private final String directoryPath;
    private final DataFusionService dataFusionService;
    private final Optional<CryptoMetadata> cryptoMetadata;

    private static final String PME_ENCRYPTED = "opensearch.pme.encrypted";
    private static final String PME_WRAPPED_KEY_B64 = "opensearch.pme.wrapped_footer_key_b64";

    /**
     * Creates a reader manager.
     * @param dataFormat the data format for this reader
     * @param shardPath the shard path to read data from
     * @param dataFusionService node-level service for cache management
     */
    public DatafusionReaderManager(
        DataFormat dataFormat,
        ShardPath shardPath,
        DataFusionService dataFusionService,
        Optional<CryptoMetadata> cryptoMetadata
    ) {
        this.dataFormat = dataFormat;
        this.directoryPath = shardPath.getDataPath().resolve(dataFormat.name()).toString();
        this.dataFusionService = dataFusionService;
        this.cryptoMetadata = cryptoMetadata;
    }

    @Override
    public DatafusionReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        if (readers.containsKey(catalogSnapshot)) {
            return readers.get(catalogSnapshot);
        }
        throw new IOException("No DataFusion reader available");
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        DatafusionReader removed = readers.remove(catalogSnapshot);
        if (removed != null) {
            removed.close();
        }
    }

    @Override
    public void onFilesDeleted(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        dataFusionService.onFilesDeleted(toAbsolutePaths(files));
    }

    @Override
    public void onFilesAdded(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        dataFusionService.onFilesAdded(toAbsolutePaths(files));
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh == false) return;
        if (readers.containsKey(catalogSnapshot)) return;
        Collection<WriterFileSet> fileSets = catalogSnapshot.getSearchableFiles(dataFormat.name());
        Map<String, byte[]> fileFooterKeys = new HashMap<>();
        Map<String, byte[]> fileAadPrefixes = new HashMap<>();
        rehydrateEncryptionMaterial(fileSets, fileFooterKeys, fileAadPrefixes);
        DatafusionReader reader = new DatafusionReader(directoryPath, fileSets, fileFooterKeys, fileAadPrefixes);
        readers.put(catalogSnapshot, reader);
    }

    private void rehydrateEncryptionMaterial(
        Collection<WriterFileSet> fileSets,
        Map<String, byte[]> fileFooterKeys,
        Map<String, byte[]> fileAadPrefixes
    ) throws IOException {
        if (fileSets == null || fileSets.isEmpty()) {
            return;
        }

        try (MasterKeyProvider keyProvider = createKeyProviderForRead()) {
            for (WriterFileSet fileSet : fileSets) {
                for (String file : fileSet.files()) {
                    Map<String, String> metadata = fileSet.metadataForFile(file);
                    boolean encrypted = "true".equals(metadata.get(PME_ENCRYPTED));
                    if (encrypted == false) {
                        continue;
                    }

                    String wrappedKeyB64 = metadata.get(PME_WRAPPED_KEY_B64);
                    if (wrappedKeyB64 == null || wrappedKeyB64.isBlank()) {
                        throw new IOException("Missing PME wrapped footer key metadata for file: " + file);
                    }
                    if (keyProvider == null) {
                        throw new IOException("PME metadata present but index crypto settings are not configured");
                    }

                    byte[] wrappedKey;
                    try {
                        wrappedKey = Base64.getDecoder().decode(wrappedKeyB64);
                    } catch (IllegalArgumentException e) {
                        throw new IOException("Invalid base64 wrapped footer key for file: " + file, e);
                    }
                    byte[] decryptedKey = keyProvider.decryptKey(wrappedKey);
                    if (decryptedKey == null || decryptedKey.length == 0) {
                        throw new IOException("Failed to decrypt wrapped footer key for file: " + file);
                    }
                    fileFooterKeys.put(file, decryptedKey);

                    // Derive the AAD prefix from the PME key metadata stored in the file.
                    // Mirrors PmeKeyDerivation.buildAadPrefix() — inlined here to avoid a
                    // compile-scope dependency on parquet-data-format (which is test-only).
                    String keyMetadataJson = metadata.get("opensearch.pme.key_metadata_json");
                    if (keyMetadataJson != null && keyMetadataJson.isBlank() == false) {
                        byte[] messageId = parseMessageIdFromKeyMetadataJson(file, keyMetadataJson);
                        fileAadPrefixes.put(file, buildAadPrefix(messageId));
                    }
                }
            }
        }
    }

    // TODO: The two helpers below (parseMessageIdFromKeyMetadataJson, buildAadPrefix) duplicate
    //  logic that lives canonically in PmeFileKeyMetadata and PmeKeyDerivation inside the
    //  parquet-data-format plugin. They are inlined here because parquet-data-format is only on
    //  the testImplementation classpath of this module — adding it as a compile/implementation
    //  dependency would create a plugin-to-plugin compile dependency that causes jar-hell at
    //  runtime (both plugins load in separate classloaders under the same OpenSearch node).
    //  The right long-term fix is to extract the shared PME crypto primitives into a dedicated
    //  library module (e.g. sandbox:libs:pme-crypto) that both plugins can depend on without
    //  classloader conflicts. Until then, keep these helpers in sync with the originals manually.

    /**
     * Parses the {@code message_id} field from a minimal v1 PME key-metadata JSON string.
     * Inlined from {@code PmeFileKeyMetadata.parse()} — see TODO above.
     */
    private static byte[] parseMessageIdFromKeyMetadataJson(String file, String json) throws IOException {
        // Minimal hand-rolled extraction: locate "message_id":"<value>" without pulling in Jackson.
        String marker = "\"message_id\":\"";
        int start = json.indexOf(marker);
        if (start < 0) {
            throw new IOException("PME key metadata missing message_id for file: " + file);
        }
        start += marker.length();
        int end = json.indexOf('"', start);
        if (end < 0) {
            throw new IOException("PME key metadata malformed message_id for file: " + file);
        }
        String b64 = json.substring(start, end);
        byte[] messageId;
        try {
            messageId = Base64.getUrlDecoder().decode(b64);
        } catch (IllegalArgumentException e) {
            throw new IOException("PME key metadata invalid base64url message_id for file: " + file, e);
        }
        if (messageId.length != 16) {
            throw new IOException("PME key metadata message_id must be 16 bytes for file: " + file);
        }
        return messageId;
    }

    /**
     * Builds the v1 binary AAD prefix for the given 16-byte message_id.
     * Inlined from {@code PmeKeyDerivation.buildAadPrefix()} — see TODO above.
     *
     * <pre>
     * u16_be(domain.len) || domain || u8(1) || u16_be(dataKeyId.len) || dataKeyId || messageId[16]
     * </pre>
     */
    private static byte[] buildAadPrefix(byte[] messageId) {
        byte[] domain = "opensearch/parquet-pme/file/v1".getBytes(StandardCharsets.UTF_8);
        byte[] dataKeyId = "default".getBytes(StandardCharsets.UTF_8);
        byte[] aad = new byte[2 + domain.length + 1 + 2 + dataKeyId.length + 16];
        int pos = 0;
        aad[pos++] = (byte) (domain.length >>> 8);
        aad[pos++] = (byte) domain.length;
        System.arraycopy(domain, 0, aad, pos, domain.length);
        pos += domain.length;
        aad[pos++] = 0x01; // version
        aad[pos++] = (byte) (dataKeyId.length >>> 8);
        aad[pos++] = (byte) dataKeyId.length;
        System.arraycopy(dataKeyId, 0, aad, pos, dataKeyId.length);
        pos += dataKeyId.length;
        System.arraycopy(messageId, 0, aad, pos, 16);
        return aad;
    }

    private MasterKeyProvider createKeyProviderForRead() {
        if (cryptoMetadata.isPresent() == false) {
            return null;
        }

        CryptoMetadata metadata = cryptoMetadata.get();
        CryptoHandlerRegistry cryptoHandlerRegistry = CryptoHandlerRegistry.getInstance();
        if (cryptoHandlerRegistry == null) {
            throw new IllegalStateException("CryptoHandlerRegistry is not initialized");
        }
        if (cryptoHandlerRegistry.fetchCryptoHandler(metadata) == null) {
            throw new CryptoRegistryException(
                metadata.keyProviderName(),
                metadata.keyProviderType(),
                "Crypto handler not found for DataFusion PME read"
            );
        }

        CryptoKeyProviderPlugin keyProviderPlugin = cryptoHandlerRegistry.getCryptoKeyProviderPlugin(metadata.keyProviderType());
        if (keyProviderPlugin == null) {
            throw new CryptoRegistryException(
                metadata.keyProviderName(),
                metadata.keyProviderType(),
                "Crypto key provider plugin not found for DataFusion PME read"
            );
        }
        return keyProviderPlugin.createKeyProvider(metadata);
    }

    private Collection<String> toAbsolutePaths(Collection<String> fileNames) {
        return fileNames.stream().map(f -> directoryPath + "/" + f).collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        for (DatafusionReader reader : readers.values()) {
            reader.close();
        }
        readers.clear();
    }
}
