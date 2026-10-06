/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.index.mapper.FieldValueTransformation;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Map;

/**
 * Encodes locally resolved field transformations for the in-process Java-to-Rust FFM boundary.
 * This is not an OpenSearch transport wire format and the encoded bytes are not sent between
 * nodes. {@link ShardScanInstructionHandler}, {@link ShardScanWithDelegationHandler}, and the
 * fetch-by-row-ID path in {@link DataFusionAnalyticsBackendPlugin} pass the result to
 * {@link NativeBridge} through {@code createSessionContext},
 * {@code createSessionContextForIndexedExecution}, or {@code fetchByRowIds}; Rust consumes all
 * three in {@code masking::decode}.
 *
 * <p>All counts and lengths are 32-bit big-endian values. Strings contain a byte length followed
 * by UTF-8 bytes, while byte arrays contain a byte length followed by raw bytes. The complete
 * format is:
 * <pre>
 * transformations := entry_count:u32 entry[entry_count]
 * entry           := field_name:string kind:u8 payload
 * hash payload    := algorithm:u8 salt:bytes
 * regex payload   := replacement_count:u32 (pattern:string replacement:string)[replacement_count]
 * string          := byte_length:u32 utf8_bytes[byte_length]
 * bytes           := byte_length:u32 raw_bytes[byte_length]
 * </pre>
 * Kind {@code 1} selects the hash payload and kind {@code 2} the regex payload. Hash algorithms
 * {@code 0} through {@code 3} denote BLAKE2b-256 personalized, BLAKE2b-256 salted, SHA-256, and
 * SHA-512 respectively. Entries are sorted by field name to make the encoding deterministic.
 * Because both sides run in the same plugin installation, this private format has no transport
 * version.
 */
final class FieldMaskingFfmCodec {

    private static final int HASH = 1;
    private static final int REGEX_REPLACE = 2;

    private FieldMaskingFfmCodec() {}

    static byte[] encode(Map<String, FieldValueTransformation> transformations) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(transformations.size());
            for (Map.Entry<String, FieldValueTransformation> entry : transformations.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                .toList()) {
                writeString(out, entry.getKey());
                switch (entry.getValue()) {
                    case FieldValueTransformation.Hash hash -> {
                        out.writeByte(HASH);
                        out.writeByte(hashTag(hash.algorithm()));
                        byte[] salt = hash.salt();
                        out.writeInt(salt.length);
                        out.write(salt);
                    }
                    case FieldValueTransformation.RegexReplace regex -> {
                        out.writeByte(REGEX_REPLACE);
                        out.writeInt(regex.replacements().size());
                        for (FieldValueTransformation.RegexReplacement replacement : regex.replacements()) {
                            writeString(out, replacement.pattern());
                            writeString(out, replacement.replacement());
                        }
                    }
                }
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("failed to encode field value transformations", e);
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static int hashTag(FieldValueTransformation.HashAlgorithm algorithm) {
        return switch (algorithm) {
            case BLAKE2B_256_PERSONALIZED -> 0;
            case BLAKE2B_256_SALTED -> 1;
            case SHA_256 -> 2;
            case SHA_512 -> 3;
        };
    }
}
