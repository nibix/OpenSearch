/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.test.OpenSearchTestCase;

public class ParquetModularEncryptionConfigTests extends OpenSearchTestCase {

    public void testFooterKeyIsDefensivelyCopied() {
        byte[] original = new byte[] { 1, 2, 3, 4 };
        byte[] wrapped = new byte[] { 9, 8, 7, 6 };
        ParquetModularEncryptionConfig config = new ParquetModularEncryptionConfig("kmsA", "aws-kms", "arn:1", "ctx", original, wrapped);

        original[0] = 99;
        byte[] fromConfig = config.footerKey();
        assertEquals(1, fromConfig[0]);

        fromConfig[1] = 88;
        assertEquals(2, config.footerKey()[1]);

        wrapped[0] = 11;
        byte[] wrappedFromConfig = config.wrappedFooterKey();
        assertEquals(9, wrappedFromConfig[0]);
    }

    public void testRejectsBlankKmsIdentity() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ParquetModularEncryptionConfig("   ", "aws-kms", "arn:1", "ctx", new byte[] { 1 }, new byte[] { 2 })
        );
        assertTrue(ex.getMessage().contains("kmsInstanceId"));
    }

    public void testRejectsEmptyWrappedFooterKey() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ParquetModularEncryptionConfig("kmsA", "aws-kms", "arn:1", "ctx", new byte[] { 1 }, new byte[] {})
        );
        assertTrue(ex.getMessage().contains("wrappedFooterKey"));
    }
}

