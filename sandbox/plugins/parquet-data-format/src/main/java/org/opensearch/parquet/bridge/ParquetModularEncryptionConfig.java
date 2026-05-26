/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import java.util.Arrays;
import java.util.Objects;

/**
 * Prototypische PME-Konfiguration fuer den nativen Parquet-Writer.
 *
 * <p>Diese Struktur transportiert nur die minimalen Informationen, die wir fuer den ersten
 * Integrationsschritt benoetigen:
 * KMS-Identitaet + optionalen KMS-Kontext + Footer-Key-Material.
 *
 * <p>TODO PME key derivation: {@code footerKey} is currently the raw KMS data key. Production
 * code should pass an already-derived per-file Parquet key, v1 footer_key_metadata JSON bytes
 * ({@code version}, {@code data_key_id}, {@code message_id}), and the binary AAD prefix. The
 * root data key should be hydrated from the Lucene-style index-level keyfile and kept in a scoped
 * cache, not transported to Rust through this config.
 *
 * <p>TODO PME key lifecycle: this object currently stores raw key bytes directly, and callers may
 * retain it as an engine field for the full shard lifetime. Replace this with a scoped key handle
 * or cache entry that has explicit expiry, eviction, and best-effort zeroing semantics comparable
 * to Lucene storage encryption's NodeLevelKeyCache lifecycle.
 */
public final class ParquetModularEncryptionConfig {

    private final String kmsInstanceId;
    private final String kmsInstanceType;
    private final String kmsKeyArn;
    private final String kmsEncryptionContext;
    private final byte[] footerKey;
    private final byte[] wrappedFooterKey;

    public ParquetModularEncryptionConfig(
        String kmsInstanceId,
        String kmsInstanceType,
        String kmsKeyArn,
        String kmsEncryptionContext,
        byte[] footerKey,
        byte[] wrappedFooterKey
    ) {
        this.kmsInstanceId = requireNonBlank(kmsInstanceId, "kmsInstanceId");
        this.kmsInstanceType = requireNonBlank(kmsInstanceType, "kmsInstanceType");
        this.kmsKeyArn = Objects.requireNonNull(kmsKeyArn, "kmsKeyArn must not be null");
        this.kmsEncryptionContext = Objects.requireNonNull(kmsEncryptionContext, "kmsEncryptionContext must not be null");
        this.footerKey = requireNonEmptyBytes(footerKey, "footerKey");
        this.wrappedFooterKey = requireNonEmptyBytes(wrappedFooterKey, "wrappedFooterKey");
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static byte[] requireNonEmptyBytes(byte[] value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.length == 0) {
            throw new IllegalArgumentException(fieldName + " must not be empty");
        }
        return Arrays.copyOf(value, value.length);
    }

    public String kmsInstanceId() {
        return kmsInstanceId;
    }

    public String kmsInstanceType() {
        return kmsInstanceType;
    }

    public String kmsKeyArn() {
        return kmsKeyArn;
    }

    public String kmsEncryptionContext() {
        return kmsEncryptionContext;
    }

    public byte[] footerKey() {
        return Arrays.copyOf(footerKey, footerKey.length);
    }

    public byte[] wrappedFooterKey() {
        return Arrays.copyOf(wrappedFooterKey, wrappedFooterKey.length);
    }
}
