/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for the deprecated {@link ParquetModularEncryptionConfig} placeholder.
 * The actual PME tests live in {@code org.opensearch.parquet.encryption} package.
 */
public class ParquetModularEncryptionConfigTests extends OpenSearchTestCase {

    public void testClassIsDeprecated() {
        // ParquetModularEncryptionConfig is a deprecated empty stub.
        // Actual PME logic is covered by PmeContext, PmeDataKeyCache, etc.
        assertTrue("deprecated class exists as stub", true);
    }
}
