/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.FieldValueTransformation;

import java.util.Optional;

/**
 * Plugin component exposing request-context-aware field-value transformations.
 *
 * <p>The Security plugin is the sole producer of this component. It returns the component from
 * {@link Plugin#createComponents} to make its effective field-masking access controls available to
 * plugins initialized after it. If no component is registered, consumers assume that Security is
 * absent and use {@link #NOOP}.
 *
 * <p>Consumers call {@link #forIndex(String)} with a concrete index name while the request's
 * authorization context is installed, then resolve transformations for mapped leaf fields through
 * the returned {@link FieldTransformations}. A payload-data reader for a non-Lucene format must
 * resolve the policy locally on the node performing the raw read and apply a returned
 * transformation before query operators or callers can observe the value. Already transformed
 * intermediate data must not be transformed again.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
@FunctionalInterface
public interface FieldValueTransformationProvider {

    /** Resolver used when no security plugin supplies field-masking policy. */
    FieldValueTransformationProvider NOOP = concreteIndex -> mappedLeafField -> Optional.empty();

    /**
     * Resolves the request-scoped field transformations for one concrete index.
     *
     * <p>The returned resolver may capture authorization state from the current request and must not
     * be cached or reused by another request.
     *
     * @param concreteIndex the concrete index name; never an alias, data stream, or wildcard expression
     * @return the request-scoped transformation resolver for mapped leaf fields in that index
     */
    FieldTransformations forIndex(String concreteIndex);

    /** Resolves field-value transformations within one concrete index and request. */
    @ExperimentalApi
    @FunctionalInterface
    interface FieldTransformations {

        /**
         * Resolves the transformation for one mapped leaf field.
         *
         * @param mappedLeafField the fully qualified mapped leaf-field name, including any dotted object path
         * @return the field's transformation, or an empty optional when its value is not masked
         */
        Optional<FieldValueTransformation> resolve(String mappedLeafField);
    }
}
