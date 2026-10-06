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
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.dsl.DslQueryExecutorPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.index.mapper.FieldValueTransformation;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.node.Node;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.AccessPolicyProviderPlugin;
import org.opensearch.plugins.FieldValueTransformationProvider;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Verifies that query execution obtains any ReadAccessPolicy provided by the cluster and properly applies it.
 * In a full cluster, the ReadAccessPolicy is provided by the security plugin. In this case, this is provided by a
 * mock plugin.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class ReadAccessPolicyIntegrationIT extends OpenSearchIntegTestCase {

    private static final String RESTRICTED_INDEX = "analytics-policy-restricted";
    private static final String UNRESTRICTED_INDEX = "analytics-policy-unrestricted";
    private static final String FIELD_RESTRICTED_INDEX = "analytics-fields-restricted";
    private static final String OBJECT_FIELD_RESTRICTED_INDEX = "analytics-object-fields-restricted";
    private static final String FIELD_UNRESTRICTED_ALIAS_BACKING = "analytics-fields-alias-backing";
    private static final String FIELD_RESTRICTED_ALIAS = "analytics-fields-alias";
    private static final String FIELD_RESTRICTED_DATA_STREAM = "analytics-fields-stream";
    private static final String FIELD_RESTRICTED_DATA_STREAM_TEMPLATE = "analytics-fields-stream-template";
    private static final String MASKED_INDEX = "analytics-fields-masked";
    private static final String MASKED_ALIAS = "analytics-fields-masked-alias";
    private static final String MASKED_DATA_STREAM = "analytics-fields-masked-stream";
    private static final String MASKED_DATA_STREAM_TEMPLATE = "analytics-fields-masked-stream-template";
    private static final String MASKED_UNSUPPORTED_INDEX = "analytics-fields-masked-unsupported";
    private static final String REMOTE_MASKED_INDEX = "analytics-fields-masked-remote";
    private static final String MASKING_HEADER = "x-test-field-masking";
    private static final String MASKING_HEADER_VALUE = "enabled";

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
        if (indexExists(MASKED_INDEX) == false) {
            createMaskedAnalyticsIndex(MASKED_INDEX, 2, null);
            indexMaskedDocuments(MASKED_INDEX);
        }
        if (clusterService().state().metadata().getIndicesLookup().containsKey(MASKED_ALIAS) == false) {
            assertTrue(client().admin().indices().prepareAliases().addAlias(MASKED_INDEX, MASKED_ALIAS).get().isAcknowledged());
        }
        if (clusterService().state().metadata().getIndicesLookup().containsKey(MASKED_DATA_STREAM) == false) {
            createMaskedDataStream();
            indexMaskedDocuments(MASKED_DATA_STREAM);
        }
        if (indexExists(MASKED_UNSUPPORTED_INDEX) == false) {
            createMaskedUnsupportedIndex();
            client().prepareIndex(MASKED_UNSUPPORTED_INDEX).setSource("masked_number", 42).get();
            refresh(MASKED_UNSUPPORTED_INDEX);
            client().admin().indices().prepareFlush(MASKED_UNSUPPORTED_INDEX).get();
        }
        if (indexExists(REMOTE_MASKED_INDEX) == false) {
            String pinnedNode = dataNodeNames().get(1);
            createMaskedAnalyticsIndex(REMOTE_MASKED_INDEX, 1, pinnedNode);
            indexMaskedDocuments(REMOTE_MASKED_INDEX);
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

    public void testFieldMaskingAppliesAtOrdinaryShardScanAndSupportsValueOperators() {
        PPLResponse projected = executeMaskedPpl("source = " + MASKED_INDEX + " | fields rank, secret | sort secret, rank");
        assertEquals(List.of("rank", "secret"), projected.getColumns());
        assertEquals("MASK_A", projected.getRows().get(0)[1]);
        assertEquals("MASK_A", projected.getRows().get(1)[1]);
        assertEquals("MASK_B", projected.getRows().get(2)[1]);
        assertEquals(1, ((Number) projected.getRows().get(0)[0]).intValue());
        assertEquals(3, ((Number) projected.getRows().get(1)[0]).intValue());
        assertEquals(2, ((Number) projected.getRows().get(2)[0]).intValue());

        PPLResponse grouped = executeMaskedPpl("source = " + MASKED_INDEX + " | stats count() as c by secret | sort secret");
        int secretColumn = grouped.getColumns().indexOf("secret");
        int countColumn = grouped.getColumns().indexOf("c");
        assertEquals(2, grouped.getRows().size());
        assertEquals("MASK_A", grouped.getRows().get(0)[secretColumn]);
        assertEquals(2L, ((Number) grouped.getRows().get(0)[countColumn]).longValue());
        assertEquals("MASK_B", grouped.getRows().get(1)[secretColumn]);
        assertEquals(1L, ((Number) grouped.getRows().get(1)[countColumn]).longValue());

        PPLResponse distinct = executeMaskedPpl("source = " + MASKED_INDEX + " | stats dc(secret) as c");
        assertEquals(2L, ((Number) distinct.getRows().getFirst()[0]).longValue());

        PPLResponse scalar = executeMaskedPpl(
            "source = " + MASKED_INDEX + " | sort rank | eval normalized=lower(secret) | fields normalized"
        );
        assertEquals("mask_a", scalar.getRows().get(0)[0]);
        assertEquals("mask_b", scalar.getRows().get(1)[0]);

        PPLResponse compared = executeMaskedPpl(
            "source = " + MASKED_INDEX + " | where secret = 'MASK_A' | fields rank | sort rank"
        );
        assertEquals(2, compared.getRows().size());
        assertEquals(1, ((Number) compared.getRows().get(0)[0]).intValue());
        assertEquals(3, ((Number) compared.getRows().get(1)[0]).intValue());
        assertTrue(executeMaskedPpl("source = " + MASKED_INDEX + " | where secret = 'alice' | fields rank").getRows().isEmpty());
    }

    public void testFieldMaskingAppliesToListElements() {
        PPLResponse response = executeMaskedPpl("source = " + MASKED_INDEX + " | where rank = 1 | fields tags");
        assertEquals(List.of("MASK_A", "MASK_B"), response.getRows().getFirst()[0]);
    }

    public void testFieldMaskingPreservesNullAndSupportsTextAndObjectLeaves() {
        PPLResponse response = executeMaskedPpl(
            "source = " + MASKED_INDEX + " | where rank = 1 | fields nullable, message, details.visible, details.secret"
        );

        assertNull(response.getRows().getFirst()[0]);
        assertEquals("MASK_A", response.getRows().getFirst()[1]);
        assertEquals("shown", response.getRows().getFirst()[2]);
        assertEquals("MASK_A", response.getRows().getFirst()[3]);
    }

    public void testFieldMaskingSupportsAllHashModes() {
        PPLResponse response = executeMaskedPpl(
            "source = "
                + MASKED_INDEX
                + " | where rank = 1 | fields hash_personalized, hash_salted, hash_sha256, hash_sha512"
        );

        assertArrayEquals(
            new Object[] {
                "4ea22c69e3064517f269cfe53906f6492e99f19337b3ce659f1c49f5af3c5d34",
                "8353eae0d97a4b105e7c2af5735d8bb84a85d07498fb4681fd139c56b84a2472",
                "2bd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90",
                "04d2411562af0ca5edeab8e6a44d9ce53ffc44fed60242e7f797f62e4abab3f9e0f2adf6a36627f5021d6b3662b6be79f5452cbcc4d865685903e949589bdd71" },
            response.getRows().getFirst()
        );
    }

    public void testFieldMaskingAppliesThroughAliasAndDataStream() {
        PPLResponse alias = executeMaskedPpl("source = " + MASKED_ALIAS + " | where rank = 1 | fields secret");
        assertEquals("MASK_A", alias.getRows().getFirst()[0]);

        PPLResponse dataStream = executeMaskedPpl("source = " + MASKED_DATA_STREAM + " | where rank = 1 | fields secret");
        assertEquals("MASK_A", dataStream.getRows().getFirst()[0]);
    }

    public void testFieldMaskingIsNotRepeatedForGroupedIntermediateData() {
        PPLResponse response = executeMaskedPpl("source = " + MASKED_INDEX + " | stats count() as c by once | sort once");

        int onceColumn = response.getColumns().indexOf("once");
        assertEquals("masked-alice", response.getRows().get(0)[onceColumn]);
        assertEquals("masked-bob", response.getRows().get(1)[onceColumn]);
    }

    public void testFieldMaskingFailsClosedForUnsupportedMappedType() {
        Exception exception = expectThrows(
            Exception.class,
            () -> executeMaskedPpl("source = " + MASKED_UNSUPPORTED_INDEX + " | fields masked_number")
        );
        String messages = exceptionMessages(exception);
        assertTrue(
            messages,
            messages.contains(
                "Field value transformation for ["
                    + MASKED_UNSUPPORTED_INDEX
                    + "][masked_number] requires a string or keyword field but mapping type is [integer]"
            )
        );
    }

    public void testFieldMaskingRejectsPplSearchCriteria() {
        Exception exception = expectThrows(
            Exception.class,
            () -> executeMaskedPpl("search source=" + MASKED_INDEX + " secret=\"MASK_A\"")
        );
        assertTrue(exceptionMessages(exception).contains("PPL search criteria are not supported for masked field [secret]"));
    }

    public void testFieldMaskingRejectsTextRelevanceFunction() {
        Exception exception = expectThrows(
            Exception.class,
            () -> executeMaskedPpl("source=" + MASKED_INDEX + " | where match(secret, 'MASK_A') | fields rank")
        );
        String messages = exceptionMessages(exception);
        assertTrue(
            messages,
            messages.contains("Text-relevance and PPL search criteria are not supported for masked field [secret]")
        );
    }

    public void testFieldMaskingRejectsJoinAndWindowUse() {
        Exception join = expectThrows(
            Exception.class,
            () -> executeMaskedPpl(
                "source="
                    + MASKED_INDEX
                    + " as left_side | inner join left=left_side right=right_side on left_side.secret = right_side.secret "
                    + MASKED_INDEX
                    + " as right_side | head 1"
            )
        );
        assertTrue(exceptionMessages(join).contains("Joins are not supported for masked field [secret]"));

        Exception window = expectThrows(
            Exception.class,
            () -> executeMaskedPpl("source=" + MASKED_INDEX + " | sort rank | streamstats count() as c by secret")
        );
        assertTrue(exceptionMessages(window).contains("Window functions are not supported for masked field [secret]"));
    }

    public void testFieldMaskingAppliesDuringQueryThenFetch() {
        TestAccessPolicyProviderPlugin.OBSERVATIONS.clear();
        PPLResponse response = executeMaskedPpl("source = " + MASKED_INDEX + " | sort rank | head 1 | fields secret");
        assertEquals(1, response.getRows().size());
        assertEquals(
            "query-then-fetch masking observations=" + TestAccessPolicyProviderPlugin.OBSERVATIONS,
            "MASK_A",
            response.getRows().getFirst()[0]
        );
        assertTrue(
            "query and fetch phases must each resolve masking locally, observations=" + TestAccessPolicyProviderPlugin.OBSERVATIONS,
            TestAccessPolicyProviderPlugin.OBSERVATIONS.stream().filter(o -> "secret".equals(o.field())).count() >= 3
        );
    }

    public void testFieldMaskingPolicyIsResolvedOnRemoteShardSearchExecutor() {
        List<String> nodes = dataNodeNames();
        String coordinator = nodes.get(0);
        String shardNode = nodes.get(1);
        TestAccessPolicyProviderPlugin.OBSERVATIONS.clear();

        PPLResponse response = executeMaskedPpl(coordinator, "source = " + REMOTE_MASKED_INDEX + " | fields secret");
        assertEquals("MASK_A", response.getRows().getFirst()[0]);
        assertTrue(
            "expected local masking policy resolution on remote shard node ["
                + shardNode
                + "], observations="
                + TestAccessPolicyProviderPlugin.OBSERVATIONS,
            TestAccessPolicyProviderPlugin.OBSERVATIONS.stream()
                .anyMatch(observation -> shardNode.equals(observation.node()) && "secret".equals(observation.field()))
        );
        assertTrue(
            "expected masking policy resolution after asynchronous handoff to SEARCH executor, observations="
                + TestAccessPolicyProviderPlugin.OBSERVATIONS,
            TestAccessPolicyProviderPlugin.OBSERVATIONS.stream()
                .anyMatch(
                    observation -> shardNode.equals(observation.node())
                        && observation.thread().contains("[search]")
                        && MASKING_HEADER_VALUE.equals(observation.authorizationHeader())
                )
        );
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

    private void createMaskedAnalyticsIndex(String indexName, int shards, String requiredNode) {
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(maskedIndexSettings(shards, requiredNode))
            .setMapping("{\"properties\":{" + maskedFieldMappings() + "}}")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);
    }

    private void indexMaskedDocuments(String indexName) {
        boolean dataStream = indexName.equals(MASKED_DATA_STREAM);
        client().prepareIndex(indexName)
            .setCreate(dataStream)
            .setSource(
                "@timestamp",
                "2026-10-05T00:00:00Z",
                "rank",
                1,
                "secret",
                "alice",
                "tags",
                List.of("alice", "bob"),
                "message",
                "alice",
                "once",
                "alice",
                "hash_personalized",
                "alice",
                "hash_salted",
                "Grüße 東京",
                "hash_sha256",
                "alice",
                "hash_sha512",
                "Grüße 東京",
                "details",
                Map.of("visible", "shown", "secret", "alice")
            )
            .get();
        client().prepareIndex(indexName)
            .setCreate(dataStream)
            .setSource(
                "@timestamp",
                "2026-10-05T00:00:01Z",
                "rank",
                2,
                "secret",
                "bob",
                "tags",
                List.of("bob"),
                "message",
                "bob",
                "once",
                "bob",
                "details",
                Map.of("visible", "shown", "secret", "bob")
            )
            .get();
        client().prepareIndex(indexName)
            .setCreate(dataStream)
            .setSource(
                "@timestamp",
                "2026-10-05T00:00:02Z",
                "rank",
                3,
                "secret",
                "alice",
                "tags",
                List.of("alice"),
                "message",
                "alice",
                "once",
                "alice",
                "details",
                Map.of("visible", "shown", "secret", "alice")
            )
            .get();
        refresh(indexName);
        client().admin().indices().prepareFlush(indexName).get();
    }

    private void createMaskedDataStream() throws Exception {
        ComposableIndexTemplate template = new ComposableIndexTemplate(
            List.of(MASKED_DATA_STREAM),
            new Template(
                maskedIndexSettings(1, null).build(),
                new CompressedXContent("{\"properties\":{" + maskedFieldMappings() + "}}"),
                null
            ),
            null,
            null,
            null,
            null,
            new ComposableIndexTemplate.DataStreamTemplate(new DataStream.TimestampField("@timestamp"))
        );
        assertTrue(
            client().execute(
                PutComposableIndexTemplateAction.INSTANCE,
                new PutComposableIndexTemplateAction.Request(MASKED_DATA_STREAM_TEMPLATE).indexTemplate(template)
            ).get().isAcknowledged()
        );
        assertTrue(
            client().admin().indices().createDataStream(new CreateDataStreamAction.Request(MASKED_DATA_STREAM)).get().isAcknowledged()
        );
        ensureGreen(MASKED_DATA_STREAM);
    }

    private void createMaskedUnsupportedIndex() {
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(MASKED_UNSUPPORTED_INDEX)
            .setSettings(maskedIndexSettings(1, null))
            .setMapping("masked_number", "type=integer")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(MASKED_UNSUPPORTED_INDEX);
    }

    private static Settings.Builder maskedIndexSettings(int shards, String requiredNode) {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats");
        if (requiredNode != null) {
            settings.put("index.routing.allocation.require._name", requiredNode);
        }
        return settings;
    }

    private static String maskedFieldMappings() {
        return "\"@timestamp\":{\"type\":\"date\"},"
            + "\"rank\":{\"type\":\"integer\"},"
            + "\"secret\":{\"type\":\"keyword\",\"low_cardinality\":true},"
            + "\"tags\":{\"type\":\"keyword\",\"low_cardinality\":true,\"multi_value\":true},"
            + "\"nullable\":{\"type\":\"keyword\",\"low_cardinality\":true},"
            + "\"message\":{\"type\":\"text\",\"low_cardinality\":true},"
            + "\"once\":{\"type\":\"keyword\",\"low_cardinality\":true},"
            + "\"hash_personalized\":{\"type\":\"keyword\",\"low_cardinality\":true},"
            + "\"hash_salted\":{\"type\":\"keyword\",\"low_cardinality\":true},"
            + "\"hash_sha256\":{\"type\":\"keyword\",\"low_cardinality\":true},"
            + "\"hash_sha512\":{\"type\":\"keyword\",\"low_cardinality\":true},"
            + "\"details\":{\"properties\":{"
            + "\"visible\":{\"type\":\"keyword\",\"low_cardinality\":true},"
            + "\"secret\":{\"type\":\"keyword\",\"low_cardinality\":true}}}";
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

    private PPLResponse executeMaskedPpl(String query) {
        return executeMaskedPpl(dataNodeNames().get(0), query);
    }

    private List<String> dataNodeNames() {
        List<String> nodes = internalCluster().getDataNodeNames().stream().sorted().toList();
        assertEquals("field-masking remote execution tests require two data nodes", 2, nodes.size());
        return nodes;
    }

    private PPLResponse executeMaskedPpl(String nodeName, String query) {
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeName);
        try (var ignored = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(MASKING_HEADER, MASKING_HEADER_VALUE);
            return internalCluster().client(nodeName)
                .execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(query))
                .actionGet();
        }
    }

    private void assertDeniedFieldFailure(String query) {
        assertDeniedFieldFailure(query, "secret");
    }

    private void assertDeniedFieldFailure(String query, String field) {
        Exception exception = expectThrows(Exception.class, () -> executePpl(query));
        String message = exceptionMessages(exception);
        String expectedMessage = "Field [" + field + "] not found.";
        assertTrue(
            "expected error [" + expectedMessage + "] for query [" + query + "], but exception chain was: " + message,
            message.contains(expectedMessage)
        );
    }

    private static String exceptionMessages(Exception exception) {
        StringBuilder messages = new StringBuilder();
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null) {
                messages.append(cause.getMessage()).append('\n');
            }
        }
        return messages.toString();
    }

    public static class TestAccessPolicyProviderPlugin extends Plugin implements AccessPolicyProviderPlugin, MapperPlugin {
        static final ConcurrentLinkedQueue<MaskingObservation> OBSERVATIONS = new ConcurrentLinkedQueue<>();
        private static final FieldValueTransformation MASK = new FieldValueTransformation.RegexReplace(
            List.of(
                new FieldValueTransformation.RegexReplacement("^alice$", "MASK_A"),
                new FieldValueTransformation.RegexReplacement("^bob$", "MASK_B")
            )
        );
        private static final FieldValueTransformation MASK_ONCE = new FieldValueTransformation.RegexReplace(
            List.of(new FieldValueTransformation.RegexReplacement("^(.+)$", "masked-$1"))
        );
        private static final Map<String, FieldValueTransformation> MASKED_FIELDS = Map.ofEntries(
            Map.entry("secret", MASK),
            Map.entry("tags", MASK),
            Map.entry("nullable", MASK),
            Map.entry("message", MASK),
            Map.entry("details.secret", MASK),
            Map.entry("once", MASK_ONCE),
            Map.entry(
                "hash_personalized",
                new FieldValueTransformation.Hash(
                    FieldValueTransformation.HashAlgorithm.BLAKE2B_256_PERSONALIZED,
                    "0123456789abcdef".getBytes(StandardCharsets.UTF_8)
                )
            ),
            Map.entry(
                "hash_salted",
                new FieldValueTransformation.Hash(
                    FieldValueTransformation.HashAlgorithm.BLAKE2B_256_SALTED,
                    "0123456789abcdef".getBytes(StandardCharsets.UTF_8)
                )
            ),
            Map.entry(
                "hash_sha256",
                new FieldValueTransformation.Hash(FieldValueTransformation.HashAlgorithm.SHA_256, new byte[0])
            ),
            Map.entry(
                "hash_sha512",
                new FieldValueTransformation.Hash(FieldValueTransformation.HashAlgorithm.SHA_512, new byte[0])
            )
        );

        private volatile ThreadPool threadPool;
        private volatile String nodeName;

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            this.threadPool = threadPool;
            this.nodeName = Node.NODE_NAME_SETTING.get(environment.settings());
            return List.of(new FieldValueTransformationProvider(index -> field -> resolveFieldValueTransformation(index, field)));
        }

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

        private Optional<FieldValueTransformation> resolveFieldValueTransformation(String index, String field) {
            boolean maskedIndex = MASKED_INDEX.equals(index)
                || REMOTE_MASKED_INDEX.equals(index)
                || index.startsWith(DataStream.BACKING_INDEX_PREFIX + MASKED_DATA_STREAM + "-");
            FieldValueTransformation transformation = maskedIndex ? MASKED_FIELDS.get(field) : null;
            if (MASKED_UNSUPPORTED_INDEX.equals(index) && "masked_number".equals(field)) {
                transformation = new FieldValueTransformation.Hash(FieldValueTransformation.HashAlgorithm.SHA_256, new byte[0]);
            }
            if (transformation != null) {
                ThreadPool currentThreadPool = threadPool;
                String authorization = currentThreadPool == null
                    ? null
                    : currentThreadPool.getThreadContext().getHeader(MASKING_HEADER);
                OBSERVATIONS.add(new MaskingObservation(nodeName, field, Thread.currentThread().getName(), authorization));
                if (MASKING_HEADER_VALUE.equals(authorization)) {
                    return Optional.of(transformation);
                }
            }
            return Optional.empty();
        }
    }

    record MaskingObservation(String node, String field, String thread, String authorizationHeader) {}

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
