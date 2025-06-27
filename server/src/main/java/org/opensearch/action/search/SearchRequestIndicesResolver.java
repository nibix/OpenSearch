/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.OriginalIndices;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.transport.RemoteClusterAware;
import org.opensearch.transport.RemoteClusterService;

import java.util.Map;

import static org.opensearch.action.search.TransportSearchAction.getIndicesFromSearchContexts;

public class SearchRequestIndicesResolver {
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final RemoteClusterService remoteClusterService;

    public SearchRequestIndicesResolver(
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedWriteableRegistry namedWriteableRegistry,
        RemoteClusterService remoteClusterService
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.remoteClusterService = remoteClusterService;
    }

    public ResolvedIndices resolveIndices(SearchRequest searchRequest) {
        ClusterState clusterState = clusterService.state();
        TransportSearchAction.OriginalIndicesAndSearchContextId requestedIndices = extractRequestedIndices(searchRequest, clusterState);
        Index[] localConcreteIndices = resolveLocalIndices(
            requestedIndices.localOriginalIndices(),
            clusterState,
            new TransportSearchAction.SearchTimeProvider(searchRequest.getOrCreateAbsoluteStartMillis(), System.nanoTime(), System::nanoTime)
        );

        return ResolvedIndices.of(localConcreteIndices).withRemoteIndices(requestedIndices.remoteClusterIndices());
    }

    TransportSearchAction.OriginalIndicesAndSearchContextId extractRequestedIndices(SearchRequest searchRequest, ClusterState clusterState) {
        final SearchContextId searchContext;
        final Map<String, OriginalIndices> remoteClusterIndices;
        if (searchRequest.pointInTimeBuilder() != null) {
            searchContext = SearchContextId.decode(namedWriteableRegistry, searchRequest.pointInTimeBuilder().getId());
            remoteClusterIndices = getIndicesFromSearchContexts(searchContext, searchRequest.indicesOptions());
        } else {
            searchContext = null;
            remoteClusterIndices = remoteClusterService.groupIndices(
                searchRequest.indicesOptions(),
                searchRequest.indices(),
                idx -> indexNameExpressionResolver.hasIndexAbstraction(idx, clusterState)
            );
        }
        OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        return new TransportSearchAction.OriginalIndicesAndSearchContextId(localIndices, remoteClusterIndices, searchContext);
    }

    Index[] resolveLocalIndices(OriginalIndices localIndices, ClusterState clusterState, TransportSearchAction.SearchTimeProvider timeProvider) {
        if (localIndices == null) {
            return Index.EMPTY_ARRAY; // don't search on any local index (happens when only remote indices were specified)
        }
        return indexNameExpressionResolver.concreteIndices(clusterState, localIndices, timeProvider.getAbsoluteStartMillis());
    }

}
