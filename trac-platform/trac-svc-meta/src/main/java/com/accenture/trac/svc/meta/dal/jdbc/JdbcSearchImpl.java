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

    long[] search(Connection conn, short tenantId, SearchParameters searchParameters) throws SQLException {

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
}
