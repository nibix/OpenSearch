/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.plugins.AccessPolicyProviderPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public class ReadAccessPolicyServiceTests extends OpenSearchTestCase {

    public void testReturnsUnrestrictedPolicyWithoutPlugin() {
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of());
        service.initialize();

        assertSame(DocumentLevelReadAccessPolicy.unrestricted(), service.getDocumentLevelReadAccessPolicy(context()));
        assertFalse(service.getFieldValueTransformationPolicy("logs").resolve("secret").isPresent());
    }

    public void testDelegatesToInstalledProvider() {
        DocumentLevelReadAccessPolicy expected = mock(DocumentLevelReadAccessPolicy.class);
        FieldValueTransformationPolicy expectedFieldPolicy = mock(FieldValueTransformationPolicy.class);
        AccessPolicyProviderPlugin plugin = () -> new ReadAccessPolicyProvider() {
            @Override
            public DocumentLevelReadAccessPolicy getDocumentLevelReadAccessPolicy(ReadAccessContext context) {
                return expected;
            }

            @Override
            public FieldValueTransformationPolicy getFieldValueTransformationPolicy(String concreteIndex) {
                assertEquals("logs", concreteIndex);
                return expectedFieldPolicy;
            }
        };
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of(plugin));
        service.initialize();

        assertSame(expected, service.getDocumentLevelReadAccessPolicy(context()));
        assertSame(expectedFieldPolicy, service.getFieldValueTransformationPolicy("logs"));
    }

    public void testProviderIsResolvedAfterComponentCreation() {
        DocumentLevelReadAccessPolicy expected = mock(DocumentLevelReadAccessPolicy.class);
        AtomicReference<ReadAccessPolicyProvider> availableProvider = new AtomicReference<>();
        AccessPolicyProviderPlugin plugin = availableProvider::get;
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of(plugin));

        assertEquals(
            "ReadAccessPolicyService is not initialized",
            expectThrows(IllegalStateException.class, () -> service.getDocumentLevelReadAccessPolicy(context())).getMessage()
        );
        assertEquals(
            "ReadAccessPolicyService is not initialized",
            expectThrows(IllegalStateException.class, () -> service.getFieldValueTransformationPolicy("logs")).getMessage()
        );
        availableProvider.set(provider(expected, FieldValueTransformationPolicy.unrestricted()));
        service.initialize();
        assertSame(expected, service.getDocumentLevelReadAccessPolicy(context()));
        assertFalse(service.getFieldValueTransformationPolicy("logs").resolve("secret").isPresent());
    }

    public void testRejectsMultipleProviders() {
        AccessPolicyProviderPlugin first = () -> provider(
            DocumentLevelReadAccessPolicy.unrestricted(),
            FieldValueTransformationPolicy.unrestricted()
        );
        AccessPolicyProviderPlugin second = () -> provider(
            DocumentLevelReadAccessPolicy.unrestricted(),
            FieldValueTransformationPolicy.unrestricted()
        );

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> new ReadAccessPolicyService(List.of(first, second))
        );
        assertEquals("Only one AccessPolicyProviderPlugin may be installed, found [2]", exception.getMessage());
    }

    public void testRejectsNullPolicy() {
        AccessPolicyProviderPlugin plugin = () -> provider(null, FieldValueTransformationPolicy.unrestricted());
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of(plugin));
        service.initialize();

        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> service.getDocumentLevelReadAccessPolicy(context())
        );
        assertEquals("ReadAccessPolicyProvider returned null", exception.getMessage());
    }

    public void testRejectsNullFieldValueTransformationPolicy() {
        AccessPolicyProviderPlugin plugin = () -> provider(DocumentLevelReadAccessPolicy.unrestricted(), null);
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of(plugin));
        service.initialize();

        NullPointerException exception = expectThrows(NullPointerException.class, () -> service.getFieldValueTransformationPolicy("logs"));
        assertEquals("ReadAccessPolicyProvider returned null field-value transformation policy", exception.getMessage());
    }

    public void testRejectsNullProvider() {
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of(() -> null));

        NullPointerException exception = expectThrows(NullPointerException.class, service::initialize);
        assertEquals("AccessPolicyProviderPlugin returned null", exception.getMessage());
    }

    private ReadAccessContext context() {
        return ReadAccessContext.of(List.of("logs"));
    }

    private static ReadAccessPolicyProvider provider(
        DocumentLevelReadAccessPolicy documentPolicy,
        FieldValueTransformationPolicy fieldPolicy
    ) {
        return new ReadAccessPolicyProvider() {
            @Override
            public DocumentLevelReadAccessPolicy getDocumentLevelReadAccessPolicy(ReadAccessContext context) {
                return documentPolicy;
            }

            @Override
            public FieldValueTransformationPolicy getFieldValueTransformationPolicy(String concreteIndex) {
                return fieldPolicy;
            }
        };
    }
}
