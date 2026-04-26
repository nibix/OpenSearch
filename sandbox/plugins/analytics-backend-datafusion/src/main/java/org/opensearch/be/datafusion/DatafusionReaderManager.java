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
        Map<String, byte[]> fileFooterKeys = rehydrateFooterKeys(fileSets);
        DatafusionReader reader = new DatafusionReader(directoryPath, fileSets, fileFooterKeys);
        readers.put(catalogSnapshot, reader);
    }

    private Map<String, byte[]> rehydrateFooterKeys(Collection<WriterFileSet> fileSets) throws IOException {
        Map<String, byte[]> decryptedKeys = new HashMap<>();
        if (fileSets == null || fileSets.isEmpty()) {
            return decryptedKeys;
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
                    decryptedKeys.put(file, decryptedKey);
                }
            }
        }

        return decryptedKeys;
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
