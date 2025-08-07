/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.OriginalIndices;
import org.opensearch.core.index.Index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class that encapsulates resolved indices. Resolved indices do not any wildcards or date math expressions.
 * However, in contrast to the concept of "concrete indices", resolved indices might not exist yet, or might
 * refer to aliases or data streams.
 * <p>
 * ResolvedIndices classes are primarily created by the resolveIndices() method in TransportIndicesResolvingAction.
 * <p>
 * How resolved indices are obtained depends on the respective action and the associated requests:
 * <ul>
 *     <li>If a request carries an index expression (i.e, might contain patterns or date math expressions), the index
 *     expression must be resolved using the appropriate index options; these might be request-specific or action-specific.</li>
 *     <li>Some requests already carry concrete indices; in these cases, the names of the concrete indices can be
 *     just taken without further evaluation</li>
 * </ul>
 */
public class ResolvedIndices {
    public static ResolvedIndices of(String... indices) {
        return new ResolvedIndices(
            new Local(Collections.unmodifiableSet(new HashSet<>(Arrays.asList(indices))), null, false),
            Collections.emptyMap()
        );
    }

    public static ResolvedIndices of(Index... indices) {
        return new ResolvedIndices(
            new Local(Stream.of(indices).map(Index::getName).collect(Collectors.toUnmodifiableSet()), null, false),
            Collections.emptyMap()
        );
    }

    public static ResolvedIndices of(Collection<String> indices) {
        return new ResolvedIndices(new Local(Collections.unmodifiableSet(new HashSet<>(indices)), null, false), Collections.emptyMap());
    }

    public static ResolvedIndices all() {
        return ALL;
    }

    public static ResolvedIndices ofNonNull(String... indices) {
        Set<String> indexSet = new HashSet<>(indices.length);

        for (String index : indices) {
            if (index != null) {
                indexSet.add(index);
            }
        }

        return new ResolvedIndices(new Local(Collections.unmodifiableSet(indexSet), null, false), Collections.emptyMap());
    }

    private static final ResolvedIndices ALL = new ResolvedIndices(new Local(Set.of(Metadata.ALL), null, true), Collections.emptyMap());

    private final Local local;
    private final Map<String, OriginalIndices> remote;

    private ResolvedIndices(Local local, Map<String, OriginalIndices> remote) {
        this.local = local;
        this.remote = remote;
    }

    public Local local() {
        return this.local;
    }

    public Map<String, OriginalIndices> remote() {
        return this.remote;
    }

    public ResolvedIndices withRemoteIndices(Map<String, OriginalIndices> remoteIndices) {
        if (remoteIndices.isEmpty()) {
            return this;
        }

        Map<String, OriginalIndices> newRemoteIndices = new HashMap<>(remoteIndices);
        newRemoteIndices.putAll(this.remote);

        return new ResolvedIndices(this.local, Collections.unmodifiableMap(newRemoteIndices));
    }

    public ResolvedIndices withLocalOriginalIndices(OriginalIndices originalIndices) {
        return new ResolvedIndices(new Local(this.local.names, originalIndices, this.local.isAll), this.remote);
    }

    public boolean isEmpty() {
        return this.local.isEmpty() && this.remote.isEmpty();
    }

    /**
     * Encapsulates the local (i.e., non-remote) indices referenced by the respective request.
     */
    public static class Local {
        private final Set<String> names;
        private final OriginalIndices originalIndices;
        private final boolean isAll;

        private Local(Set<String> names, OriginalIndices originalIndices, boolean isAll) {
            this.names = names;
            this.originalIndices = originalIndices;
            this.isAll = isAll;
        }

        public Set<String> names() {
            return this.names;
        }

        public String[] namesAsArray() {
            return this.names.toArray(new String[0]);
        }

        public OriginalIndices originalIndices() {
            return this.originalIndices;
        }

        public boolean isEmpty() {
            if (this.isAll) {
                return false;
            } else {
                return this.names.isEmpty();
            }
        }

        public boolean contains(String index) {
            if (this.isAll) {
                return true;
            } else {
                return this.names.contains(index);
            }
        }
    }

}
