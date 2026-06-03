/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * Node-scoped settings for the Parquet data format plugin.
 *
 * <p>All settings are registered with OpenSearch via
 * {@link ParquetDataFormatPlugin#getSettings()} and can be configured in
 * {@code opensearch.yml} or via cluster settings API.
 *
 * <ul>
 *   <li>{@link #MAX_NATIVE_ALLOCATION} — Maximum native memory allocation for Arrow buffers,
 *       expressed as a percentage of available non-heap system memory (default {@code "10%"}).</li>
 *   <li>{@link #MAX_ROWS_PER_VSR} — Row count threshold that triggers VectorSchemaRoot rotation
 *       during document ingestion (default {@code 50000}).</li>
 *   <li>{@link #CRYPTO_KEY_PROVIDER} — Index-scoped: name of the PME master key provider.</li>
 *   <li>{@link #CRYPTO_KEY_PROVIDER_TYPE} — Index-scoped: type of the PME master key provider.</li>
 * </ul>
 */
public final class ParquetSettings {

    private ParquetSettings() {}

    /** Default maximum native memory allocation as a percentage of available non-heap memory. */
    public static final String DEFAULT_MAX_NATIVE_ALLOCATION = "10%";
    /** Default maximum number of rows per VectorSchemaRoot before rotation. */
    public static final int DEFAULT_MAX_ROWS_PER_VSR = 50000;

    /** Maximum native memory allocation for Arrow buffers, as a percentage of non-heap memory. */
    public static final Setting<String> MAX_NATIVE_ALLOCATION = Setting.simpleString(
        "parquet.max_native_allocation",
        DEFAULT_MAX_NATIVE_ALLOCATION,
        Setting.Property.NodeScope
    );

    /** Maximum number of rows per VectorSchemaRoot before rotation is triggered. */
    public static final Setting<Integer> MAX_ROWS_PER_VSR = Setting.intSetting(
        "parquet.max_rows_per_vsr",
        DEFAULT_MAX_ROWS_PER_VSR,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Index-scoped setting: name of the PME master key provider.
     *
     * <p>The key uses a Parquet-specific prefix ({@code index.store.parquet.crypto.*}) rather
     * than the Lucene-plugin prefix ({@code index.store.crypto.*}) to avoid a setting-name
     * collision: two plugins cannot register an {@code IndexScope} setting with the same name.
     * The opensearch-storage-encryption Lucene plugin registers
     * {@code index.store.crypto.key_provider} with {@code IndexScope + NodeScope + InternalIndex}.
     *
     * <p>TODO: Once there is a core framework that lets multiple plugins share a common
     * {@code index.store.crypto.*} namespace (e.g. via a server-registered prefix setting or
     * a merged crypto-metadata registry), consolidate both the Lucene and Parquet settings
     * under a single unified {@code index.store.crypto.key_provider} setting.
     */
    public static final Setting<String> CRYPTO_KEY_PROVIDER = Setting.simpleString(
        "index.store.parquet.crypto.key_provider",
        Setting.Property.NodeScope,
        Setting.Property.IndexScope,
        Setting.Property.InternalIndex
    );

    /**
     * Index-scoped setting: type of the PME master key provider (e.g. {@code "mock-pme"}).
     *
     * <p>The Lucene storage-encryption plugin uses a single {@code index.store.crypto.key_provider}
     * setting that carries only the provider <em>name</em>; the provider <em>type</em> is resolved
     * out-of-band through the {@link org.opensearch.crypto.CryptoHandlerRegistry}.
     * Parquet PME exposes {@code index.store.parquet.crypto.key_provider_type} as a separate
     * index setting so that the correct {@link org.opensearch.plugins.CryptoKeyProviderPlugin}
     * can be selected without requiring an additional registry lookup.
     *
     * <p>Uses Parquet-specific prefix for the same collision-avoidance reason as
     * {@link #CRYPTO_KEY_PROVIDER}.
     */
    public static final Setting<String> CRYPTO_KEY_PROVIDER_TYPE = Setting.simpleString(
        "index.store.parquet.crypto.key_provider_type",
        "aws-kms",
        Setting.Property.NodeScope,
        Setting.Property.IndexScope,
        Setting.Property.InternalIndex
    );

    /** Returns all settings defined by the Parquet plugin. */
    public static List<Setting<?>> getSettings() {
        return List.of(MAX_NATIVE_ALLOCATION, MAX_ROWS_PER_VSR, CRYPTO_KEY_PROVIDER, CRYPTO_KEY_PROVIDER_TYPE);
    }
}
