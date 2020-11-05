/*
 * Copyright 2020 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.accenture.trac.svc.meta.dal.jdbc;

import com.accenture.trac.common.metadata.BasicType;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.search.*;
import com.accenture.trac.common.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;


class JdbcSearchQueryBuilder {

    private final Logger log;

    JdbcSearchQueryBuilder() {
        log = LoggerFactory.getLogger(getClass());
    }

    JdbcSearchQuery buildPriorSearchQuery(short tenantId, SearchParameters searchParameters) {

        // Base query template selects for tenant and object type

        var baseQueryTemplate = "select od%1$d.object_fk, max(od%1d.object_version) as object_version, max(t%1$d.tag_version) as tag_version\n" +
                "from tag t%1$d\n" +
                // Join clause
                "%3$s" +
                "where t%1$d.tenant_id = ?\n" +
                "  and t%1$d.object_type = ?\n" +
                "  and %4$s\n" +
                "group by od%1$d.object_fk\n" +
                "order by max(t%1$d.tag_timestamp) desc";

        // Stream of params for the base query

        var baseParams = Stream.of(
                wrapErrors((stmt, pIndex) -> stmt.setShort(pIndex, tenantId)),
                wrapErrors((stmt, pIndex) -> stmt.setString(pIndex, searchParameters.getObjectType().name())));

        // Build query parts for the main search expression and version / temporal handling

        var queryParts = new JdbcSearchQuery(0, 0, List.of());
        queryParts = buildSearchExpr(queryParts, searchParameters.getSearch());
        queryParts = buildAsOfCondition(queryParts, searchParameters);
        queryParts = buildNoPriorVersions(queryParts, searchParameters);
        queryParts = buildNoPriorTags(queryParts, searchParameters);

        // Stream of params assembled from the query parts

        var partsParams =  queryParts.getFragments().stream().flatMap(
                frag -> frag.getParams().stream());

        // Combine base and sub parts to make the final query

        var allParams = Stream.concat(baseParams, partsParams);

        return buildSearchQueryFromTemplate(baseQueryTemplate, 0, queryParts, allParams);
    }

    JdbcSearchQuery buildSearchQuery(short tenantId, SearchParameters searchParameters) {

        // Base query template selects for tenant and object type

        var baseQueryTemplate = "select t%1$d.tag_pk\n" +
                "from tag t%1$d\n" +
                // Join clause
                "%3$s" +
                "where t%1$d.tenant_id = ?\n" +
                "  and t%1$d.object_type = ?\n" +
                "  and %4$s\n" +
                "group by t%1$d.tag_pk\n" +
                "order by t%1$d.tag_timestamp desc";

        // Stream of params for the base query

        var baseParams = Stream.of(
                wrapErrors((stmt, pIndex) -> stmt.setShort(pIndex, tenantId)),
                wrapErrors((stmt, pIndex) -> stmt.setString(pIndex, searchParameters.getObjectType().name())));

        // Build query parts for the main search expression and version / temporal handling

        var queryParts = new JdbcSearchQuery(0, 0, List.of());
        queryParts = buildSearchExpr(queryParts, searchParameters.getSearch());
        queryParts = buildAsOfCondition(queryParts, searchParameters);
        queryParts = buildNoPriorVersions(queryParts, searchParameters);
        queryParts = buildNoPriorTags(queryParts, searchParameters);

        // Stream of params assembled from the query parts

        var partsParams =  queryParts.getFragments().stream().flatMap(
                frag -> frag.getParams().stream());

        // Combine base and sub parts to make the final query

        var allParams = Stream.concat(baseParams, partsParams);

        return buildSearchQueryFromTemplate(baseQueryTemplate, 0, queryParts, allParams);
    }

    JdbcSearchQuery buildSearchQueryFromTemplate(
            String queryTemplate, int baseQueryNumber,
            JdbcSearchQuery queryParts,
            Stream<JdbcSearchQuery.ParamSetter> params) {

        var queryNumber = queryParts.getSubQueryNumber();

        var joinClause = queryParts.getFragments().stream()
                .map(JdbcSearchQuery.Fragment::getJoinClause)
                .filter(clause -> !clause.isBlank())
                .collect(Collectors.joining("\n"));

        var joinClauseLf = joinClause.isBlank() ? joinClause : joinClause + "\n";

        var whereClause = queryParts.getFragments().stream()
                .map(JdbcSearchQuery.Fragment::getWhereClause)
                .filter(clause -> !clause.isBlank())
                .collect(Collectors.joining(" and "));

        var query = String.format(queryTemplate,
                queryNumber, baseQueryNumber,
                joinClauseLf, whereClause);

        return new JdbcSearchQuery(query, params.collect(Collectors.toList()));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // SEARCH EXPRESSIONS
    // -----------------------------------------------------------------------------------------------------------------

    JdbcSearchQuery buildSearchExpr(JdbcSearchQuery baseQuery, SearchExpression searchExpr) {

        switch (searchExpr.getExprCase()) {

            case LOGICAL:
                return buildLogicalExpr(baseQuery, searchExpr.getLogical());

            case TERM:
                return buildSearchTerm(baseQuery, searchExpr.getTerm());

            default:

                // Internal error - invalid searches should be picked up in the validation layer
                var message = "Invalid search expression (expression is missing or not recognised)";
                log.error(message);

                throw new EValidationGap(message);
        }
    }

    JdbcSearchQuery buildLogicalExpr(JdbcSearchQuery baseQuery, LogicalExpression logicalExpr) {

        switch (logicalExpr.getOperator()) {

            case AND:
                return buildLogicalAndOr(baseQuery, logicalExpr, "and");

            case OR:
                return buildLogicalAndOr(baseQuery, logicalExpr, "or");

            case NOT:
                return buildLogicalNot(baseQuery, logicalExpr);

            default:

                // Internal error - invalid searches should be picked up in the validation layer
                var message = "Invalid logical expression (operator is missing or not recognised)";
                log.error(message);

                throw new EValidationGap(message);
        }
    }

    JdbcSearchQuery buildSearchTerm(JdbcSearchQuery baseQuery, SearchTerm searchTerm) {

        switch (searchTerm.getOperator()) {

            case EQ:
                return buildEqualsTerm(baseQuery, searchTerm);

            case NE:
                return buildNotEqualsTerm(baseQuery, searchTerm);

            case GT:
            case GE:
            case LT:
            case LE:
                return buildInequalityTerm(baseQuery, searchTerm);

            case IN:
                return buildInTerm(baseQuery, searchTerm);

            default:

                // Internal error - invalid searches should be picked up in the validation layer
                var message = "Invalid search term (operator is missing or not recognised)";
                log.error(message);

                throw new EValidationGap(message);
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // LOGICAL OPERATORS
    // -----------------------------------------------------------------------------------------------------------------

    JdbcSearchQuery buildLogicalAndOr(JdbcSearchQuery baseQuery, LogicalExpression logicalExpr, String opWord) {

        BinaryOperator<JdbcSearchQuery> SEQUENTIAL_REDUCE = (a, b) -> {throw new RuntimeException("");};

        // Recursively create fragments for each sub expression in this logical operation
        // Fragments are added to the query stack
        var branchQuery = logicalExpr.getExprList()
                .stream()
                .reduce(baseQuery, this::buildSearchExpr, SEQUENTIAL_REDUCE);

        // Find the new fragments for the logical operation, i.e. fragments that did not exist in baseQuery
        var branchFragments = branchQuery.getFragments().subList(
                baseQuery.getFragments().size(),
                branchQuery.getFragments().size());

        // Now create a single fragment for this logical operation
        // The new fragment combines all the sub-fragments

        var joinClause = branchFragments.stream()
                .map(JdbcSearchQuery.Fragment::getJoinClause)
                .filter(clause -> !clause.isEmpty())
                .reduce((expr1, expr2) -> expr1 + "\n" + expr2)
                .orElse("");

        var whereClause = "(" + branchFragments.stream()
                .map(JdbcSearchQuery.Fragment::getWhereClause)
                .reduce((expr1, expr2) -> expr1 + "\n  " + opWord + " " + expr2)
                .orElse("1 = 1")
                + ")";

        var params = branchFragments.stream()
                .map(JdbcSearchQuery.Fragment::getParams)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        var fragment = new JdbcSearchQuery.Fragment(joinClause, whereClause, params);

        // The result of this operation is a new SearchQuery,
        // which contains the fragments from baseQuery and one new fragment for this operation
        // The new query also reflects any attr or sub-query indices that have been consumed

        return replaceFragments(branchQuery, fragment, logicalExpr.getExprCount());
    }

    JdbcSearchQuery buildLogicalNot(JdbcSearchQuery baseQuery, LogicalExpression logicalExpr) {

        // TODO: IMPORTANT: Are latest version / tag and as-of joins needed on negative sub-queries?
        // I.e. should each negative sub-query have the same version/as-of selectors as the base query?
        // Current test cases are passing without this constraint
        // Revisit this when implementing full solution for metadata temporality and as-of searching
        // If the criteria need to match, maybe refactor to re-use code from main build search function

        var subQueryTemplate =
                "select t%1$d.tag_pk\n" +
                "from tag t%1$d\n" +
                "%3$s" +  // Join clause
                "where t%1$d.tenant_id = t%2$d.tenant_id\n" +
                "  and t%1$d.object_type = t%2$d.object_type\n" +
                "  and %4$s";

        var subQueryNumber = baseQuery.getNextSubQueryNumber();
        var baseQueryNumber = baseQuery.getSubQueryNumber();
        var nextAttrNumber = baseQuery.getNextAttrNumber();

        // A logical NOT expression must have precisely one sub-expression
        if (logicalExpr.getExprCount() != 1) {

            // Internal error - invalid searches should be picked up in the validation layer
            var message = "Invalid logical expression (NOT operator requires exactly one sub-expression)";
            log.error(message);

            throw new EValidationGap(message);
        }

        // Build the query parts for the sub expression
        var subExpr = logicalExpr.getExpr(0);
        var blankQueryParts = new JdbcSearchQuery(subQueryNumber, nextAttrNumber, List.of());
        var subQueryParts = buildSearchExpr(blankQueryParts, subExpr);

        // Assemble params from all the sub-query fragments
        var subQueryParams = subQueryParts.getFragments().stream().flatMap(
                frag -> frag.getParams().stream());

        // Create a query object for the sub-query
        var subQuery = buildSearchQueryFromTemplate(subQueryTemplate, baseQueryNumber, subQueryParts, subQueryParams);

        // Now build a fragment using the sub-query in the where clause
        var whereClauseTemplate = "t%1$d.tag_pk not in (\n%2$s)\n";
        var whereClause = String.format(whereClauseTemplate, baseQueryNumber, subQuery.getQuery());
        var fragment = new JdbcSearchQuery.Fragment("", whereClause, subQuery.getParams());

        // Add the new fragment to the list

        var combinedFragments = Stream.concat(
                baseQuery.getFragments().stream(),
                Stream.of(fragment))
                .collect(Collectors.toList());

        // Update next sub-query and attr numbers to allow for values used up by the sub-query

        return new JdbcSearchQuery(
                baseQueryNumber,
                subQuery.getNextSubQueryNumber(),
                subQuery.getNextAttrNumber(),
                combinedFragments);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // SEARCH TERMS
    // -----------------------------------------------------------------------------------------------------------------

    JdbcSearchQuery buildEqualsTerm(JdbcSearchQuery baseQuery, SearchTerm searchTerm) {

        var joinTemplate = "join tag_attr ta%1$d\n" +
                "  on ta%1$d.tenant_id = t%2$d.tenant_id\n" +
                "  and ta%1$d.tag_fk = t%2$d.tag_pk";

        var whereTemplate = "ta%1$d.attr_name = ? " +
                "and ta%1$d.attr_value_%2$s = ?";

        // Match attr name
        var paramNameSetter = wrapErrors((stmt, pIndex) ->
                stmt.setString(pIndex, searchTerm.getAttrName()));

        // Condition for attr value
        var paramValueSetter = wrapErrors((stmt, pIndex) ->
                JdbcAttrHelpers.setAttrValue(
                stmt, pIndex, searchTerm.getAttrType(), searchTerm.getSearchValue()));

        return buildSearchTermFromTemplates(baseQuery, searchTerm, joinTemplate, whereTemplate,
                Stream.of(paramNameSetter, paramValueSetter));
    }

    JdbcSearchQuery buildNotEqualsTerm(JdbcSearchQuery baseQuery, SearchTerm searchTerm) {

        // Replace NE search term with a logical NOT operator around an EQ search term
        // t.A != x  <=>  ! (t.A == x)

        var negativeExpr = SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                .setOperator(LogicalOperator.NOT)
                .addExpr(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                .setAttrName(searchTerm.getAttrName())
                .setAttrType(searchTerm.getAttrType())
                .setOperator(SearchOperator.EQ)
                .setSearchValue(searchTerm.getSearchValue()))))
                .build();

        return buildSearchExpr(baseQuery, negativeExpr);
    }

    JdbcSearchQuery buildInequalityTerm(JdbcSearchQuery baseQuery, SearchTerm searchTerm) {

        var joinTemplate = "join tag_attr ta%1$d\n" +
                "  on ta%1$d.tenant_id = t%2$d.tenant_id\n" +
                "  and ta%1$d.tag_fk = t%2$d.tag_pk";

        var whereTemplate = "ta%1$d.attr_name = ? " +
                "and ta%1$d.attr_index = ? " +
                "and ta%1$d.attr_value_%2$s %3$s ?";

        // Match attr name
        var paramNameSetter = wrapErrors((stmt, pIndex) ->
                stmt.setString(pIndex, searchTerm.getAttrName()));

        // Match attr index - for inequality comparisons, only single-valued attrs are allowed
        var paramIndexSetter = wrapErrors((stmt, pIndex) ->
                stmt.setInt(pIndex, SINGLE_VALUED_ATTR_INDEX));

        // Condition for attr value
        var paramValueSetter = wrapErrors((stmt, pIndex) ->
                JdbcAttrHelpers.setAttrValue(
                stmt, pIndex, searchTerm.getAttrType(), searchTerm.getSearchValue()));

        return buildSearchTermFromTemplates(baseQuery, searchTerm, joinTemplate, whereTemplate,
                Stream.of(paramNameSetter, paramIndexSetter, paramValueSetter));
    }

    JdbcSearchQuery buildInTerm(JdbcSearchQuery baseQuery, SearchTerm searchTerm) {

        var nItems = searchTerm.getSearchValue().getArrayValue().getItemCount();
        var itemPlaceholders = String.join(", ", Collections.nCopies(nItems, "?"));

        var joinTemplate = "join tag_attr ta%1$d\n" +
                "  on ta%1$d.tenant_id = t%2$d.tenant_id\n" +
                "  and ta%1$d.tag_fk = t%2$d.tag_pk";

        var whereTemplate = "ta%1$d.attr_name = ? " +
                "and ta%1$d.attr_value_%2$s in (" + itemPlaceholders + ")";

        // Match attr name
        var paramNameSetter = wrapErrors((stmt, pIndex) ->
                stmt.setString(pIndex, searchTerm.getAttrName()));

        // Condition for attr value
        var paramValueSetters = searchTerm.getSearchValue().getArrayValue()
                .getItemList().stream().map(item -> wrapErrors((stmt, pIndex) ->
                JdbcAttrHelpers.setAttrValue(stmt, pIndex, searchTerm.getAttrType(), item)));

        return buildSearchTermFromTemplates(baseQuery, searchTerm, joinTemplate, whereTemplate,
                Stream.concat(Stream.of(paramNameSetter), paramValueSetters));
    }

    JdbcSearchQuery buildSearchTermFromTemplates(
            JdbcSearchQuery baseQuery, SearchTerm searchTerm,
            String joinTemplate, String whereTemplate,
            Stream<JdbcSearchQuery.ParamSetter> params) {

        // Fill out the join and where clause templates

        var queryNumber = baseQuery.getSubQueryNumber();
        var attrNumber = baseQuery.getNextAttrNumber();
        var attrTypeSuffix = ATTR_TYPE_COLUMN_SUFFIX.getOrDefault(searchTerm.getAttrType(), null);
        var searchOperator = SQL_INEQUALITY_OPERATORS.getOrDefault(searchTerm.getOperator(), null);

        // Internal error - invalid searches should be picked up in the validation layer
        if (attrTypeSuffix == null || searchOperator == null) {

            var message = "Invalid search term (attr type or search operator not recognised)";
            log.error(message);

            throw new EValidationGap(message);
        }

        var joinClause = String.format(joinTemplate, attrNumber, queryNumber);
        var whereClause = "(" + String.format(whereTemplate, attrNumber, attrTypeSuffix, searchOperator) + ")";

        // Create a new fragment for this search term
        var fragment = new JdbcSearchQuery.Fragment(joinClause, whereClause, params.collect(Collectors.toList()));

        // Add this new fragment to the list of fragments
        var fragments = Stream.concat(
                baseQuery.getFragments().stream(),
                Stream.of(fragment))
                .collect(Collectors.toList());

        // Each search term queries a single attr, so increment nextAttrNumber by 1
        // Adding a search term does not require a new sub-query, so subQueryNumber does not change

        return new JdbcSearchQuery(
                baseQuery.getSubQueryNumber(),
                baseQuery.getNextAttrNumber() + 1,
                fragments);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // TEMPORAL QUERIES AND VERSIONING
    // -----------------------------------------------------------------------------------------------------------------

    JdbcSearchQuery buildAsOfCondition(JdbcSearchQuery baseQuery, SearchParameters searchParams) {

        // If there is no as-of condition, do not add any conditions to the base query

        if (searchParams.getSearchAsOf() == null || searchParams.getSearchAsOf().isEmpty())
            return baseQuery;

        // The as-of condition only sets an upper bound on timestamps that are considered
        // The lower bound may or may not be set, depending on the prior version/tag flags

        // To create the upper bound, we need only look at the tag timestamp
        // Any objects or object versions created after this time have all their tags with a later timestamp

        var whereClauseTemplate = "t%1$d.tag_timestamp <= ?";
        var whereClause = String.format(whereClauseTemplate, baseQuery.getSubQueryNumber());

        var asOfTime = MetadataCodec.parseDatetime(searchParams.getSearchAsOf()).toInstant();
        var asOfSql = java.sql.Timestamp.from(asOfTime);

        var fragment = new JdbcSearchQuery.Fragment("", whereClause, List.of(
                (stmt, pIndex) -> stmt.setTimestamp(pIndex, asOfSql)));

        var allFragments = Stream.concat(
                baseQuery.getFragments().stream(),
                Stream.of(fragment))
                .collect(Collectors.toList());

        return new JdbcSearchQuery(
                baseQuery.getSubQueryNumber(),
                baseQuery.getNextAttrNumber() + 1,
                allFragments);
    }


    JdbcSearchQuery buildNoPriorVersions(JdbcSearchQuery baseQuery, SearchParameters searchParams) {

        var joinClauseTemplate =
                "join object_definition od%1$d\n" +
                "  on od%1$d.tenant_id = t%1$d.tenant_id\n" +
                "  and od%1$d.definition_pk = t%1$d.definition_fk";

        var joinClause = String.format(joinClauseTemplate, baseQuery.getSubQueryNumber());

        if (searchParams.getPriorVersions()) {

            var fragment = new JdbcSearchQuery.Fragment(joinClause, "", List.of());
            return buildNoPriorFragment(baseQuery, fragment);
        }

        var whereClauseLatestTemplate = "od%1$d.object_is_latest = ?";
        var whereClauseAsOfTemplate = "(od%1$d.object_superseded is null or od%1$d.object_superseded > ?)";

        if (searchParams.getSearchAsOf().isEmpty()) {

            var whereClause = String.format(whereClauseLatestTemplate, baseQuery.getSubQueryNumber());
            var fragment = new JdbcSearchQuery.Fragment(joinClause, whereClause, List.of(
                    (stmt, pIndex) -> stmt.setBoolean(pIndex, true)));

            return buildNoPriorFragment(baseQuery, fragment);
        }
        else {

            var asOfTime = MetadataCodec.parseDatetime(searchParams.getSearchAsOf()).toInstant();
            var asOfSql = java.sql.Timestamp.from(asOfTime);

            var whereClause = String.format(whereClauseAsOfTemplate, baseQuery.getSubQueryNumber());
            var fragment = new JdbcSearchQuery.Fragment(joinClause, whereClause, List.of(
                    (stmt, pIndex) -> stmt.setTimestamp(pIndex, asOfSql)));

            return buildNoPriorFragment(baseQuery, fragment);
        }
    }

    JdbcSearchQuery buildNoPriorTags(JdbcSearchQuery baseQuery, SearchParameters searchParams) {

        if (searchParams.getPriorTags())
            return baseQuery;

        var whereClauseLatestTemplate = "t%1$d.tag_is_latest = ?";
        var whereClauseAsOfTemplate = "(t%1$d.tag_superseded is null or t%1$d.tag_superseded > ?)";

        if (searchParams.getSearchAsOf().isEmpty()) {

            var whereClause = String.format(whereClauseLatestTemplate, baseQuery.getSubQueryNumber());
            var fragment = new JdbcSearchQuery.Fragment("", whereClause, List.of(
                    (stmt, pIndex) -> stmt.setBoolean(pIndex, true)));

            return buildNoPriorFragment(baseQuery, fragment);
        }
        else {

            var asOfTime = MetadataCodec.parseDatetime(searchParams.getSearchAsOf()).toInstant();
            var asOfSql = java.sql.Timestamp.from(asOfTime);

            var whereClause = String.format(whereClauseAsOfTemplate, baseQuery.getSubQueryNumber());
            var fragment = new JdbcSearchQuery.Fragment("", whereClause, List.of(
                    (stmt, pIndex) -> stmt.setTimestamp(pIndex, asOfSql)));

            return buildNoPriorFragment(baseQuery, fragment);
        }
    }

    JdbcSearchQuery buildNoPriorFragment(JdbcSearchQuery baseQuery, JdbcSearchQuery.Fragment fragment) {

        var allFragments = Stream.concat(
                baseQuery.getFragments().stream(),
                Stream.of(fragment))
                .collect(Collectors.toList());

        return new JdbcSearchQuery(
                baseQuery.getSubQueryNumber(),
                baseQuery.getNextSubQueryNumber(),
                baseQuery.getNextAttrNumber(),
                allFragments);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS
    // -----------------------------------------------------------------------------------------------------------------

    private JdbcSearchQuery replaceFragments(JdbcSearchQuery baseQuery, JdbcSearchQuery.Fragment fragment, int nReplaced) {

        var fragments = Stream.concat(
                baseQuery.getFragments().stream().limit(baseQuery.getFragments().size() - nReplaced),
                Stream.of(fragment))
                .collect(Collectors.toList());

        // The new query makes reference to all the existing attrs, so nextAttrNumber does not change

        return new JdbcSearchQuery(
                baseQuery.getSubQueryNumber(),
                baseQuery.getNextAttrNumber(),
                fragments);
    }

    private JdbcSearchQuery.ParamSetter wrapErrors(JdbcSearchQuery.ParamSetter setter) {

        return (stmt, pIndex) -> {
            try {
                setter.accept(stmt, pIndex);
            }
            catch (SQLException e) {

                // Internal error - this shouldn't happen at runtime
                // Any problems setting up the query should show up during the dev/test phase
                var message = "Failed to set SQL query parameter: " + e.getMessage();
                log.error(message);

                throw new ETracInternal(message, e);
            }
        };
    }

    private final Map<BasicType, String> ATTR_TYPE_COLUMN_SUFFIX = Map.ofEntries(
            Map.entry(BasicType.BOOLEAN, "boolean"),
            Map.entry(BasicType.INTEGER, "integer"),
            Map.entry(BasicType.FLOAT, "float"),
            Map.entry(BasicType.STRING, "string"),
            Map.entry(BasicType.DECIMAL, "decimal"),
            Map.entry(BasicType.DATE, "date"),
            Map.entry(BasicType.DATETIME, "datetime"));

    private final Map<SearchOperator, String> SQL_INEQUALITY_OPERATORS = Map.ofEntries(
            Map.entry(SearchOperator.EQ, "="),
            Map.entry(SearchOperator.NE, "!="),
            Map.entry(SearchOperator.GT, ">"),
            Map.entry(SearchOperator.GE, ">="),
            Map.entry(SearchOperator.LT, "<"),
            Map.entry(SearchOperator.LE, "<="),
            Map.entry(SearchOperator.IN, "IN"));

    private static final int SINGLE_VALUED_ATTR_INDEX = -1;
}
