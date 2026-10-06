/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.index.mapper.FieldValueTransformation;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Map;

/** Encodes locally resolved field transformations for the in-process Java/native boundary. */
final class FieldMaskingWireCodec {

    private static final int HASH = 1;
    private static final int REGEX_REPLACE = 2;

    private FieldMaskingWireCodec() {}

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
