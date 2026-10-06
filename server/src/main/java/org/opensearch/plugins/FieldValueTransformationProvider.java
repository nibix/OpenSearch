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

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Plugin component exposing request-context-aware field-value transformations.
 *
 * <p>The Security plugin is the sole producer of this component. It returns the component from
 * {@link Plugin#createComponents} to make its effective field-masking access controls available to
 * plugins initialized after it. If no component is registered, consumers assume that Security is
 * absent and use {@link #NOOP}.
 *
 * <p>Consumers resolve the outer function with a concrete index name and the inner function with a
 * mapped leaf field name while the request's authorization context is installed. A payload-data
 * reader for a non-Lucene format must resolve the policy locally on the node performing the raw
 * read and apply a returned transformation before query operators or callers can observe the
 * value. Already transformed intermediate data must not be transformed again.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FieldValueTransformationProvider {

    /** Resolver used when no security plugin supplies field-masking policy. */
    public static final Function<String, Function<String, Optional<FieldValueTransformation>>> NOOP = index -> field -> Optional.empty();

    private final Function<String, Function<String, Optional<FieldValueTransformation>>> fieldValueTransformations;

    public FieldValueTransformationProvider(
        Function<String, Function<String, Optional<FieldValueTransformation>>> fieldValueTransformations
    ) {
        this.fieldValueTransformations = Objects.requireNonNull(fieldValueTransformations);
    }

    /** Returns the request-context-aware field-value transformation resolver. */
    public Function<String, Function<String, Optional<FieldValueTransformation>>> getFieldValueTransformations() {
        return fieldValueTransformations;
    }
}
