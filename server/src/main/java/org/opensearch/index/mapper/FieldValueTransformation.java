/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Describes a deterministic transformation that a payload-data reader must apply to a mapped
 * field before making its value available to query operators or returning it to a caller.
 *
 * <p>The descriptor contains no request identity. The Security plugin resolves it from the
 * current request context for a concrete index and field through
 * {@link org.opensearch.plugins.FieldValueTransformationProvider#getFieldValueTransformations()}.
 *
 * <p>Only string-valued {@code keyword} and {@code text} leaves are initially supported. Readers
 * must apply the same transformation to every string element of a multi-valued field, preserve
 * nulls, and fail closed when a descriptor targets any other mapped or physical type.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public sealed interface FieldValueTransformation permits FieldValueTransformation.Hash, FieldValueTransformation.RegexReplace {

    /** The deliberately small set of hash modes whose output is part of the masking contract. */
    enum HashAlgorithm {
        /** BLAKE2b-256 with the supplied bytes in the personalization parameter. */
        BLAKE2B_256_PERSONALIZED,
        /** BLAKE2b-256 with the supplied bytes in the salt parameter. */
        BLAKE2B_256_SALTED,
        /** SHA-256. The salt must be empty. */
        SHA_256,
        /** SHA-512. The salt must be empty. */
        SHA_512
    }

    /** Hashes the UTF-8 representation and returns lower-case hexadecimal text. */
    record Hash(HashAlgorithm algorithm, byte[] salt) implements FieldValueTransformation {
        public Hash {
            Objects.requireNonNull(algorithm, "algorithm");
            salt = Objects.requireNonNull(salt, "salt").clone();
            if ((algorithm == HashAlgorithm.SHA_256 || algorithm == HashAlgorithm.SHA_512) && salt.length != 0) {
                throw new IllegalArgumentException("salt is only supported by BLAKE2b field-value transformations");
            }
            if (salt.length > 16
                && (algorithm == HashAlgorithm.BLAKE2B_256_PERSONALIZED || algorithm == HashAlgorithm.BLAKE2B_256_SALTED)) {
                throw new IllegalArgumentException("BLAKE2b salt or personalization must contain at most 16 bytes");
            }
        }

        @Override
        public byte[] salt() {
            return salt.clone();
        }
    }

    /** Applies the supplied replacements in list order. */
    record RegexReplace(List<RegexReplacement> replacements) implements FieldValueTransformation {
        public RegexReplace {
            replacements = List.copyOf(Objects.requireNonNull(replacements, "replacements"));
            if (replacements.isEmpty()) {
                throw new IllegalArgumentException("regex replacement list must not be empty");
            }
        }
    }

    /**
     * One Java-style regular-expression replacement from the portable masking subset.
     *
     * <p>The initial subset supports literals and escaped literals, character classes, {@code .},
     * {@code ^}, {@code $}, capturing and non-capturing groups, alternation, greedy quantifiers,
     * and numbered replacement captures ({@code $1}, ...). Lookaround, backreferences in the
     * pattern, named groups, inline flags, Unicode properties, shorthand character classes,
     * possessive or lazy quantifiers, atomic groups, and conditionals are rejected. This
     * conservative subset is chosen so Java and native analytics readers produce identical text.
     * Replacement strings support literal text and valid numbered captures only; literal dollar
     * signs, backslash escapes, capture zero, and named captures are not supported.
     */
    record RegexReplacement(String pattern, String replacement) {
        public RegexReplacement {
            Objects.requireNonNull(pattern, "pattern");
            Objects.requireNonNull(replacement, "replacement");
            Pattern compiled = validatePortableRegex(pattern);
            validatePortableReplacement(replacement, compiled.matcher("").groupCount());
        }

        private static Pattern validatePortableRegex(String expression) {
            Pattern compiled;
            try {
                compiled = Pattern.compile(expression);
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("invalid field-masking regular expression [" + expression + "]", e);
            }
            String[] unsupported = { "(?=", "(?!", "(?<=", "(?<!", "(?<", "(?>", "\\p{", "\\P{", "\\Q", "\\E", "&&" };
            for (String token : unsupported) {
                if (expression.contains(token)) {
                    throw new IllegalArgumentException(
                        "field-masking regular expression [" + expression + "] uses unsupported construct [" + token + "]"
                    );
                }
            }
            for (int i = expression.indexOf("(?"); i >= 0; i = expression.indexOf("(?", i + 2)) {
                if (expression.startsWith("(?:", i) == false) {
                    throw new IllegalArgumentException(
                        "field-masking regular expression [" + expression + "] uses an unsupported special group"
                    );
                }
            }
            if (expression.matches(".*(?:\\*|\\+|\\?|\\})[?+].*")) {
                throw new IllegalArgumentException("lazy and possessive regex quantifiers are not supported in [" + expression + "]");
            }
            for (int i = 0; i < expression.length(); i++) {
                if (expression.charAt(i) == '\\') {
                    if (++i == expression.length() || Character.isLetterOrDigit(expression.charAt(i))) {
                        throw new IllegalArgumentException(
                            "field-masking regular expression [" + expression + "] contains an unsupported escape"
                        );
                    }
                }
            }
            return compiled;
        }

        private static void validatePortableReplacement(String replacement, int captureCount) {
            for (int i = 0; i < replacement.length(); i++) {
                char character = replacement.charAt(i);
                if (character == '\\') {
                    throw new IllegalArgumentException("backslash escapes are not supported in field-masking regex replacements");
                }
                if (character == '$') {
                    int start = ++i;
                    while (i < replacement.length() && Character.isDigit(replacement.charAt(i))) {
                        i++;
                    }
                    if (start == i) {
                        throw new IllegalArgumentException("literal or named '$' is not supported in field-masking regex replacements");
                    }
                    int group = Integer.parseInt(replacement.substring(start, i));
                    if (group == 0 || group > captureCount) {
                        throw new IllegalArgumentException(
                            "field-masking regex replacement references capture $"
                                + group
                                + " but the pattern has "
                                + captureCount
                                + " captures"
                        );
                    }
                    i--;
                }
            }
        }
    }
}
