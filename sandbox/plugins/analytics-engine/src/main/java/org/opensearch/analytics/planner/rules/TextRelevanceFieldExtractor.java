/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.opensearch.analytics.spi.FieldReferences;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Finds field references that full-text predicates encode as string literals instead of Calcite
 * input references.
 *
 * <p>For an ordinary predicate such as {@code status = 'open'}, Calcite represents {@code status}
 * as a {@code RexInputRef}, which the planner can resolve against the scan schema directly.
 * Full-text predicates have a different shape: their fields can occur in literal {@code field} or
 * {@code fields} options, or inside a query-string literal. For example, PPL
 * {@code search source=logs status=error} can produce
 * {@code QUERY_STRING(MAP('query', 'status:error'))}; that expression contains no
 * {@code RexInputRef} for {@code status}.
 *
 * <p>{@link OpenSearchFilterRule} uses the references returned here to look up each explicitly
 * named field in the scan schema before selecting an execution backend. This provides ordinary
 * unknown-field validation when a backend serializer does not report references. Separately, a
 * field denied by field-level security has already been removed from that schema, so the same
 * lookup rejects a query-string reference to the denied field.
 *
 * @opensearch.internal
 */
final class TextRelevanceFieldExtractor {

    private static final Logger LOGGER = LogManager.getLogger(TextRelevanceFieldExtractor.class);

    /**
     * Synthetic default field passed to Lucene's {@link QueryParser}. The parser assigns this name
     * to unqualified terms such as {@code error}; {@link #collectQueryStringFields} then ignores it
     * while retaining explicit references such as {@code status:error}. The NUL prefix prevents the
     * marker from colliding with a mapped field. This matches the Lucene backend's relevance-field
     * extraction, but is defined locally because the planner cannot depend on a backend plugin.
     */
    private static final String DEFAULT_FIELD_SENTINEL = "\u0000__analytics_default_field__";
    private static final String EXISTS_FIELD = "_exists_";

    private TextRelevanceFieldExtractor() {}

    /** Extracts literal and pattern field references from a text-relevance predicate. */
    static FieldReferences referencedFields(ScalarFunction function, RexCall predicate) {
        Set<String> tokens = new LinkedHashSet<>();
        String query = null;
        boolean lenient = false;

        for (RexNode operand : predicate.getOperands()) {
            if (operand instanceof RexCall == false) {
                continue;
            }
            RexCall mapCall = (RexCall) operand;
            if (mapCall.getOperands().size() < 2) {
                continue;
            }
            String key = stringLiteral(mapCall.getOperands().get(0));
            RexNode value = mapCall.getOperands().get(1);
            if ("fields".equals(key)) {
                collectFields(value, tokens);
            } else if ("field".equals(key)) {
                String field = stringLiteral(value);
                if (field != null) {
                    tokens.add(field);
                }
            } else if ("query".equals(key)) {
                query = stringLiteral(value);
            } else if ("lenient".equals(key)) {
                lenient = Boolean.parseBoolean(stringLiteral(value));
            }
        }

        if (function == ScalarFunction.QUERY_STRING && query != null) {
            collectQueryStringFields(query, tokens);
        }

        List<String> literalFields = new ArrayList<>();
        List<String> patternTokens = new ArrayList<>();
        for (String token : tokens) {
            if (Regex.isSimpleMatchPattern(token)) {
                patternTokens.add(token);
            } else {
                literalFields.add(token);
            }
        }
        return new FieldReferences(literalFields, patternTokens, lenient);
    }

    private static void collectFields(RexNode value, Set<String> tokens) {
        if (value instanceof RexCall fieldsMap) {
            List<RexNode> operands = fieldsMap.getOperands();
            for (int i = 0; i < operands.size(); i += 2) {
                String field = stringLiteral(operands.get(i));
                if (field != null) {
                    tokens.add(field);
                }
            }
        } else {
            String field = stringLiteral(value);
            if (field != null) {
                tokens.add(field);
            }
        }
    }

    private static void collectQueryStringFields(String queryString, Set<String> tokens) {
        QueryParser parser = new QueryParser(DEFAULT_FIELD_SENTINEL, Lucene.KEYWORD_ANALYZER);
        parser.setAllowLeadingWildcard(true);
        Query query;
        try {
            query = parser.parse(queryString);
        } catch (ParseException | RuntimeException e) {
            LOGGER.debug("Skipping plan-time query_string field extraction for [{}]: {}", queryString, e.getMessage());
            return;
        }
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                if (DEFAULT_FIELD_SENTINEL.equals(field) == false && EXISTS_FIELD.equals(field) == false) {
                    tokens.add(field);
                }
                return true;
            }

            @Override
            public void consumeTerms(Query query, Term... terms) {
                for (Term term : terms) {
                    if (EXISTS_FIELD.equals(term.field())) {
                        tokens.add(term.text());
                    }
                }
            }
        });
    }

    private static String stringLiteral(RexNode node) {
        return node instanceof RexLiteral literal ? literal.getValueAs(String.class) : null;
    }
}
