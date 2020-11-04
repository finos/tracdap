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


import com.accenture.trac.common.metadata.search.SearchParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

class JdbcSearchImpl {

    private static final int MAX_SEARCH_RESULT = 100;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JdbcSearchQueryBuilder queryBuilder = new JdbcSearchQueryBuilder();
    private final JdbcReadBatchImpl readBatch;

    JdbcSearchImpl(JdbcReadBatchImpl readBatch) {
        this.readBatch = readBatch;
    }

    long[] search(Connection conn, short tenantId, SearchParameters searchParameters) throws SQLException {

        if (searchParameters.getPriorVersions() || searchParameters.getPriorTags())
            return searchPrior(conn, tenantId, searchParameters);

        var query = queryBuilder.buildSearchQuery(tenantId, searchParameters);

        log.info("Running search query: \n{}", query.getQuery());


        var pks = new long[MAX_SEARCH_RESULT];

        try (var stmt = conn.prepareStatement(query.getQuery())) {

            for (int pIndex = 0; pIndex < query.getParams().size(); pIndex++)
                query.getParams().get(pIndex).accept(stmt, pIndex + 1);

            int i = 0;

            try (var rs = stmt.executeQuery()) {

                while (rs.next() && i < MAX_SEARCH_RESULT) {
                    pks[i] = rs.getLong("tag_pk");
                    i++;
                }

                if (i < MAX_SEARCH_RESULT)
                    return Arrays.copyOfRange(pks, 0, i);
                else
                    return pks;
            }
        }
    }

    long[] searchPrior(Connection conn, short tenantId, SearchParameters searchParameters) throws SQLException {

        var query = queryBuilder.buildPriorSearchQuery(tenantId, searchParameters);

        log.info("Running search query: \n{}", query.getQuery());

        var oPks = new long[MAX_SEARCH_RESULT];
        var oVers = new int[MAX_SEARCH_RESULT];
        var tVers = new int[MAX_SEARCH_RESULT];

        try (var stmt = conn.prepareStatement(query.getQuery())) {

            for (int pIndex = 0; pIndex < query.getParams().size(); pIndex++)
                query.getParams().get(pIndex).accept(stmt, pIndex + 1);

            int i = 0;

            try (var rs = stmt.executeQuery()) {

                while (rs.next() && i < MAX_SEARCH_RESULT) {
                    oPks[i] = rs.getLong("object_fk");
                    oVers[i] = rs.getInt("object_version");
                    tVers[i] = rs.getInt("tag_version");
                    i++;
                }
            }

            if (i < MAX_SEARCH_RESULT) {
                oPks = Arrays.copyOfRange(oPks, 0, i);
                oVers = Arrays.copyOfRange(oVers, 0, i);
                tVers = Arrays.copyOfRange(tVers, 0, i);
            }
        }

        var defPks = readBatch.lookupDefinitionPk(conn, tenantId, oPks, oVers);
        return readBatch.lookupTagPk(conn, tenantId, defPks, tVers);
    }
}
