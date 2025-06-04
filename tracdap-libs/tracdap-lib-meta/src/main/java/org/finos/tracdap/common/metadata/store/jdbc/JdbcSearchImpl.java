/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.metadata.store.jdbc;

import org.finos.tracdap.metadata.SearchParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;


class JdbcSearchImpl {

    private static final int MAX_SEARCH_RESULT = 100;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JdbcSearchQueryBuilder queryBuilder = new JdbcSearchQueryBuilder();

    long[] search(Connection conn, short tenantId, SearchParameters searchParameters) throws SQLException {

        var query = (searchParameters.getPriorVersions() || searchParameters.getPriorTags())
                ? queryBuilder.buildPriorSearchQuery(tenantId, searchParameters)
                : queryBuilder.buildSearchQuery(tenantId, searchParameters);

        if (log.isDebugEnabled())
            log.debug("Running search query: \n{}", query.getQuery());

        try (var stmt = conn.prepareStatement(query.getQuery())) {

            for (int pIndex = 0; pIndex < query.getParams().size(); pIndex++)
                query.getParams().get(pIndex).accept(stmt, pIndex + 1);

            return readPks(stmt, "tag_pk");
        }
    }

    long[] searchConfigKeys(Connection conn, short tenantId, String configClass, boolean includeDeleted) throws SQLException {

        var query =
                "select config_pk\n" +
                "from config_entry\n" +
                "where tenant_id = ?\n" +
                "and config_class = ?\n" +
                "and config_is_latest = ?\n" +
                // Optional filter on deleted items
                (includeDeleted ? "" : "  and config_deleted = ?\n") +
                // Use alphabetical ordering by key
                "order by config_key";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setInt(1, tenantId);
            stmt.setString(2, configClass);
            stmt.setBoolean(3, true);

            if (!includeDeleted)
                stmt.setBoolean(4, false);

            return readPks(stmt, "config_pk");
        }
    }

    private long[] readPks(PreparedStatement stmt, String columnName) throws SQLException {

        long[] pks = new long[MAX_SEARCH_RESULT];
        int i = 0;

        try (var rs = stmt.executeQuery()) {

            while (rs.next() && i < MAX_SEARCH_RESULT) {
                pks[i] = rs.getLong(columnName);
                i++;
            }

            if (i < MAX_SEARCH_RESULT)
                return Arrays.copyOfRange(pks, 0, i);
            else
                return pks;
        }
    }
}
