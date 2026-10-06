/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ReadAccessContext;
import org.opensearch.action.support.ReadAccessPolicy;
import org.opensearch.action.support.ReadAccessPolicyProvider;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.dsl.DslQueryExecutorPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.AccessPolicyProviderPlugin;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Verifies that query execution obtains any ReadAccessPolicy provided by the cluster and properly applies it.
 * In a full cluster, the ReadAccessPolicy is provided by the security plugin. In this case, this is provided by a
 * mock plugin.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class ReadAccessPolicyIntegrationIT extends OpenSearchIntegTestCase {

    private static final String RESTRICTED_INDEX = "analytics-policy-restricted";
    private static final String UNRESTRICTED_INDEX = "analytics-policy-unrestricted";
    private static final String FIELD_RESTRICTED_INDEX = "analytics-fields-restricted";
    private static final String OBJECT_FIELD_RESTRICTED_INDEX = "analytics-object-fields-restricted";
    private static final String FIELD_UNRESTRICTED_ALIAS_BACKING = "analytics-fields-alias-backing";
    private static final String FIELD_RESTRICTED_ALIAS = "analytics-fields-alias";
    private static final String FIELD_RESTRICTED_DATA_STREAM = "analytics-fields-stream";
    private static final String FIELD_RESTRICTED_DATA_STREAM_TEMPLATE = "analytics-fields-stream-template";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            CompositeDataFormatPlugin.class,
            MockCommitterEnginePlugin.class,
            TestPPLPlugin.class,
            DslQueryExecutorPlugin.class,
            TestAccessPolicyProviderPlugin.class
        );
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetOnlyDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (indexExists(RESTRICTED_INDEX) == false) {
            createAnalyticsIndex(RESTRICTED_INDEX);
            client().prepareIndex(RESTRICTED_INDEX).setSource("tenant", "blue", "category", "blue-visible").get();
            client().prepareIndex(RESTRICTED_INDEX).setSource("tenant", "red", "category", "red-hidden").get();
            refresh(RESTRICTED_INDEX);
            client().admin().indices().prepareFlush(RESTRICTED_INDEX).get();
        }
        if (indexExists(UNRESTRICTED_INDEX) == false) {
            createAnalyticsIndex(UNRESTRICTED_INDEX);
            client().prepareIndex(UNRESTRICTED_INDEX).setSource("tenant", "blue", "category", "blue-visible").get();
            client().prepareIndex(UNRESTRICTED_INDEX).setSource("tenant", "red", "category", "red-visible").get();
            refresh(UNRESTRICTED_INDEX);
            client().admin().indices().prepareFlush(UNRESTRICTED_INDEX).get();
        }
        if (indexExists(FIELD_RESTRICTED_INDEX) == false) {
            createFieldRestrictedAnalyticsIndex(FIELD_RESTRICTED_INDEX);
            client().prepareIndex(FIELD_RESTRICTED_INDEX).setSource("visible", "shown", "secret", "hidden").get();
            refresh(FIELD_RESTRICTED_INDEX);
            client().admin().indices().prepareFlush(FIELD_RESTRICTED_INDEX).get();
        }
        if (indexExists(OBJECT_FIELD_RESTRICTED_INDEX) == false) {
            createObjectFieldRestrictedAnalyticsIndex();
            client().prepareIndex(OBJECT_FIELD_RESTRICTED_INDEX)
                .setSource("details", Map.of("visible", "object-shown", "secret", "object-hidden"))
                .get();
            refresh(OBJECT_FIELD_RESTRICTED_INDEX);
            client().admin().indices().prepareFlush(OBJECT_FIELD_RESTRICTED_INDEX).get();
        }
        if (indexExists(FIELD_UNRESTRICTED_ALIAS_BACKING) == false) {
            createFieldRestrictedAnalyticsIndex(FIELD_UNRESTRICTED_ALIAS_BACKING);
            client().prepareIndex(FIELD_UNRESTRICTED_ALIAS_BACKING).setSource("visible", "also-shown", "secret", "also-hidden").get();
            refresh(FIELD_UNRESTRICTED_ALIAS_BACKING);
            client().admin().indices().prepareFlush(FIELD_UNRESTRICTED_ALIAS_BACKING).get();
        }
        if (clusterService().state().metadata().getIndicesLookup().containsKey(FIELD_RESTRICTED_ALIAS) == false) {
            assertTrue(
                client().admin()
                    .indices()
                    .prepareAliases()
                    .addAlias(FIELD_RESTRICTED_INDEX, FIELD_RESTRICTED_ALIAS)
                    .addAlias(FIELD_UNRESTRICTED_ALIAS_BACKING, FIELD_RESTRICTED_ALIAS)
                    .get()
                    .isAcknowledged()
            );
        }
        if (clusterService().state().metadata().getIndicesLookup().containsKey(FIELD_RESTRICTED_DATA_STREAM) == false) {
            createFieldRestrictedDataStream();
            client().prepareIndex(FIELD_RESTRICTED_DATA_STREAM)
                .setCreate(true)
                .setSource("@timestamp", "2026-10-05T00:00:00Z", "visible", "stream-shown", "secret", "stream-hidden")
                .get();
            refresh(FIELD_RESTRICTED_DATA_STREAM);
            client().admin().indices().prepareFlush(FIELD_RESTRICTED_DATA_STREAM).get();
        }
    }

    public void testPolicyRestrictsAnalyticsResults() {
        PPLResponse response = executePpl("source = " + RESTRICTED_INDEX + " | fields category | sort category");

        assertEquals(1, response.getRows().size());
        assertEquals("blue-visible", response.getRows().getFirst()[0]);
    }

    public void testPolicyLeavesUnrestrictedIndexUnchanged() {
        PPLResponse response = executePpl("source = " + UNRESTRICTED_INDEX + " | fields category | sort category");

        assertEquals(2, response.getRows().size());
        assertEquals("blue-visible", response.getRows().get(0)[0]);
        assertEquals("red-visible", response.getRows().get(1)[0]);
    }

    public void testFieldFilterRemovesDeniedFieldFromWildcardProjection() {
        PPLResponse response = executePpl("source = " + FIELD_RESTRICTED_INDEX);

        assertEquals(List.of("visible"), response.getColumns());
        assertEquals(1, response.getRows().size());
        assertArrayEquals(new Object[] { "shown" }, response.getRows().getFirst());
    }

    public void testFieldFilterRedactsDeniedFieldFromSearchHitSource() {
        SearchHit hit = executeSearch(FIELD_RESTRICTED_INDEX, new SearchSourceBuilder());

        assertEquals(Map.of("visible", "shown"), hit.getSourceAsMap());
    }

    public void testSourceIncludesCannotReintroduceDeniedField() {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(new String[] { "visible", "secret" }, null);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> executeSearch(FIELD_RESTRICTED_INDEX, source));

        assertEquals("Field 'secret' not found in schema", exception.getMessage());
    }

    public void testFieldFilterRedactsDeniedObjectLeafFromSearchHitSource() {
        SearchHit hit = executeSearch(OBJECT_FIELD_RESTRICTED_INDEX, new SearchSourceBuilder());

        assertEquals(Map.of("details", Map.of("visible", "object-shown")), hit.getSourceAsMap());
    }

    public void testFieldFilterRejectsDeniedFieldReferences() {
        assertDeniedFieldFailure("source = " + FIELD_RESTRICTED_INDEX + " | fields secret");
    }

    /**
     * The PPL search predicate lowers to QUERY_STRING(MAP('query', 'secret:hidden')) with no
     * RexInputRef for secret. No backend in this test cluster supplies a QUERY_STRING serializer,
     * so this exercises TextRelevanceFieldExtractor's planner fallback and verifies that it resolves
     * the in-string field against the FLS-filtered schema.
     */
    public void testFieldFilterRejectsDeniedFieldInsideQueryString() {
        assertDeniedFieldFailure("search source=" + FIELD_RESTRICTED_INDEX + " secret=\"hidden\"");
    }

    public void testFieldFilterRejectsDeniedFieldInWhereCriterion() {
        assertDeniedFieldFailure("source = " + FIELD_RESTRICTED_INDEX + " | where secret = 'hidden' | fields visible");
    }

    public void testFieldFilterRejectsDeniedFieldInAnalyticsOperators() {
        assertDeniedFieldFailure("source = " + FIELD_RESTRICTED_INDEX + " | stats count() as c by secret");
        assertDeniedFieldFailure("source = " + FIELD_RESTRICTED_INDEX + " | stats dc(secret) as c");
        assertDeniedFieldFailure("source = " + FIELD_RESTRICTED_INDEX + " | sort secret | fields visible");
        assertDeniedFieldFailure("source = " + FIELD_RESTRICTED_INDEX + " | eval derived = upper(secret) | fields derived");
    }

    public void testFieldFilterAppliesToObjectLeavesByDottedPath() {
        PPLResponse response = executePpl("source = " + OBJECT_FIELD_RESTRICTED_INDEX + " | fields details.visible");

        assertEquals(List.of("details.visible"), response.getColumns());
        assertEquals(1, response.getRows().size());
        assertArrayEquals(new Object[] { "object-shown" }, response.getRows().getFirst());
        assertDeniedFieldFailure("source = " + OBJECT_FIELD_RESTRICTED_INDEX + " | fields details.secret", "details.secret");
        assertDeniedFieldFailure(
            "source = " + OBJECT_FIELD_RESTRICTED_INDEX + " | where details.secret = 'object-hidden' | fields details.visible",
            "details.secret"
        );
        assertDeniedFieldFailure("source = " + OBJECT_FIELD_RESTRICTED_INDEX + " | stats dc(details.secret) as c", "details.secret");
    }

    /** An alias hides a field when any concrete backing containing that field denies it. */
    public void testFieldFilterAppliesToAliasBackings() {
        PPLResponse response = executePpl("source = " + FIELD_RESTRICTED_ALIAS);

        assertEquals(List.of("visible"), response.getColumns());
        assertEquals(2, response.getRows().size());
        assertDeniedFieldFailure("source = " + FIELD_RESTRICTED_ALIAS + " | fields secret");
    }

    /** A data-stream query applies the field filter to its concrete hidden backing index. */
    public void testFieldFilterAppliesToDataStreamBackings() {
        PPLResponse response = executePpl("source = " + FIELD_RESTRICTED_DATA_STREAM);

        assertEquals(Set.of("@timestamp", "visible"), Set.copyOf(response.getColumns()));
        assertEquals(1, response.getRows().size());
        assertDeniedFieldFailure("source = " + FIELD_RESTRICTED_DATA_STREAM + " | fields secret");
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    private void createAnalyticsIndex(String indexName) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(settings)
            .setMapping(
                "tenant",
                "type=keyword,low_cardinality=true",
                "category",
                "type=keyword,low_cardinality=true"
            )
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);
    }

    private void createFieldRestrictedAnalyticsIndex(String indexName) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(settings)
            .setMapping("visible", "type=keyword,low_cardinality=true", "secret", "type=keyword,low_cardinality=true")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);
    }

    private void createObjectFieldRestrictedAnalyticsIndex() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(OBJECT_FIELD_RESTRICTED_INDEX)
            .setSettings(settings)
            .setMapping(
                "{\"properties\":{"
                    + "\"details\":{\"properties\":{"
                    + "\"visible\":{\"type\":\"keyword\",\"low_cardinality\":true},"
                    + "\"secret\":{\"type\":\"keyword\",\"low_cardinality\":true}}}}}"
            )
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(OBJECT_FIELD_RESTRICTED_INDEX);
    }

    private void createFieldRestrictedDataStream() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
        ComposableIndexTemplate template = new ComposableIndexTemplate(
            List.of(FIELD_RESTRICTED_DATA_STREAM),
            new Template(
                settings,
                new CompressedXContent(
                    "{\"properties\":{"
                        + "\"@timestamp\":{\"type\":\"date\"},"
                        + "\"visible\":{\"type\":\"keyword\",\"low_cardinality\":true},"
                        + "\"secret\":{\"type\":\"keyword\",\"low_cardinality\":true}}}"
                ),
                null
            ),
            null,
            null,
            null,
            null,
            new ComposableIndexTemplate.DataStreamTemplate(new DataStream.TimestampField("@timestamp"))
        );
        PutComposableIndexTemplateAction.Request templateRequest = new PutComposableIndexTemplateAction.Request(
            FIELD_RESTRICTED_DATA_STREAM_TEMPLATE
        ).indexTemplate(template);
        assertTrue(client().execute(PutComposableIndexTemplateAction.INSTANCE, templateRequest).get().isAcknowledged());
        assertTrue(
            client().admin()
                .indices()
                .createDataStream(new CreateDataStreamAction.Request(FIELD_RESTRICTED_DATA_STREAM))
                .get()
                .isAcknowledged()
        );
        ensureGreen(FIELD_RESTRICTED_DATA_STREAM);
    }

    private PPLResponse executePpl(String query) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(query)).actionGet();
    }

    private SearchHit executeSearch(String index, SearchSourceBuilder source) {
        SearchResponse response = client().search(new SearchRequest(index).source(source)).actionGet();
        assertEquals(1, response.getHits().getHits().length);
        return response.getHits().getAt(0);
    }

    private void assertDeniedFieldFailure(String query) {
        assertDeniedFieldFailure(query, "secret");
    }

    private void assertDeniedFieldFailure(String query, String field) {
        Exception exception = expectThrows(Exception.class, () -> executePpl(query));
        StringBuilder messages = new StringBuilder();
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null) {
                messages.append(cause.getMessage()).append('\n');
            }
        }
        String message = messages.toString();
        String expectedMessage = "Field [" + field + "] not found.";
        assertTrue(
            "expected error [" + expectedMessage + "] for query [" + query + "], but exception chain was: " + message,
            message.contains(expectedMessage)
        );
    }

    public static class TestAccessPolicyProviderPlugin extends Plugin implements AccessPolicyProviderPlugin, MapperPlugin {
        @Override
        public ReadAccessPolicyProvider getReadAccessPolicyProvider() {
            return new TestReadAccessPolicyProvider();
        }

        @Override
        public Function<String, Predicate<String>> getFieldFilter() {
            return index -> {
                boolean restricted = FIELD_RESTRICTED_INDEX.equals(index)
                    || OBJECT_FIELD_RESTRICTED_INDEX.equals(index)
                    || index.startsWith(DataStream.BACKING_INDEX_PREFIX + FIELD_RESTRICTED_DATA_STREAM + "-");
                return restricted
                    ? field -> "secret".equals(field) == false && "details.secret".equals(field) == false
                    : MapperPlugin.NOOP_FIELD_PREDICATE;
            };
        }
    }

    private static class TestReadAccessPolicyProvider implements ReadAccessPolicyProvider {
        @Override
        public ReadAccessPolicy getReadAccessPolicy(ReadAccessContext context) {
            if (context.concreteIndices().contains(RESTRICTED_INDEX)) {
                return new TestReadAccessPolicy(
                    new TestIndexGroup(Set.of(RESTRICTED_INDEX), Optional.of(QueryBuilders.termQuery("tenant", "blue")))
                );
            }
            return ReadAccessPolicy.unrestricted();
        }
    }

    private static class TestReadAccessPolicy implements ReadAccessPolicy {
        private final List<IndexGroup> indexGroups;

        TestReadAccessPolicy(IndexGroup indexGroup) {
            this.indexGroups = List.of(indexGroup);
        }

        @Override
        public boolean hasRestrictions() {
            return true;
        }

        @Override
        public Set<String> coveredConcreteIndices() {
            return indexGroups.getFirst().concreteIndices();
        }

        @Override
        public Optional<QueryBuilder> restrictionsForIndex(String concreteIndex) {
            for (IndexGroup indexGroup : indexGroups) {
                if (indexGroup.concreteIndices().contains(concreteIndex)) {
                    return indexGroup.restrictions();
                }
            }
            return Optional.empty();
        }

        @Override
        public Collection<IndexGroup> indexGroups() {
            return indexGroups;
        }
    }

    private static class TestIndexGroup implements ReadAccessPolicy.IndexGroup {
        private final Set<String> concreteIndices;
        private final Optional<QueryBuilder> restrictions;

        TestIndexGroup(Set<String> concreteIndices, Optional<QueryBuilder> restrictions) {
            this.concreteIndices = Set.copyOf(concreteIndices);
            this.restrictions = restrictions;
        }

        @Override
        public Set<String> concreteIndices() {
            return concreteIndices;
        }

        @Override
        public Optional<QueryBuilder> restrictions() {
            return restrictions;
        }
    }
}
