/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.FieldValueTransformation;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Field-value transformations for one concrete index and read request. Resolution must use the authorization
 * state captured when the provider created this policy; neither resolve method may depend on the calling thread's context.
 */
@ExperimentalApi
public interface FieldValueTransformationPolicy {

    /** A policy with no field masking rules. */
    static FieldValueTransformationPolicy unrestricted() {
        return new FieldValueTransformationPolicy() {
            @Override
            public FieldValueTransformation resolve(String mappedLeafField) {
                return FieldValueTransformation.none();
            }

            @Override
            public Map<String, FieldValueTransformation> resolve(Collection<String> mappedLeafFields) {
                Map<String, FieldValueTransformation> resolved = new HashMap<>();
                for (String field : mappedLeafFields) {
                    resolved.put(field, FieldValueTransformation.none());
                }
                return Map.copyOf(resolved);
            }
        };
    }

    /**
     * Resolves the transformation for a fully qualified mapped leaf field, including any dotted object path.
     * Returns {@link FieldValueTransformation.None} when the field is not masked.
     */
    FieldValueTransformation resolve(String mappedLeafField);

    /**
     * Resolves every supplied fully qualified mapped leaf field. The result contains exactly one entry for every
     * distinct element in {@code mappedLeafFields}, including unmasked fields with a
     * {@link FieldValueTransformation.None} value. Each value is non-null and represents the same transformation
     * as {@link #resolve(String)} for that field. Duplicate input fields appear once as map keys.
     */
    Map<String, FieldValueTransformation> resolve(Collection<String> mappedLeafFields);
}
