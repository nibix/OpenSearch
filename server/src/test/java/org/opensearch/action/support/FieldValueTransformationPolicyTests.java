/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.index.mapper.FieldValueTransformation;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class FieldValueTransformationPolicyTests extends OpenSearchTestCase {

    public void testUnrestrictedPolicyAndTransformationPresence() {
        FieldValueTransformation hash = new FieldValueTransformation.Hash(FieldValueTransformation.HashAlgorithm.SHA_256, new byte[0]);
        FieldValueTransformation regex = new FieldValueTransformation.RegexReplace(
            List.of(new FieldValueTransformation.RegexReplacement("x", "y"))
        );
        FieldValueTransformationPolicy policy = FieldValueTransformationPolicy.unrestricted();

        assertFalse(FieldValueTransformation.none().isPresent());
        assertTrue(hash.isPresent());
        assertTrue(regex.isPresent());
        assertSame(FieldValueTransformation.none(), policy.resolve("visible"));
        assertEquals(
            Map.of("visible", FieldValueTransformation.none(), "secret", FieldValueTransformation.none()),
            policy.resolve(List.of("visible", "secret", "visible"))
        );
        assertEquals(Map.of(), policy.resolve(List.of()));
    }
}
