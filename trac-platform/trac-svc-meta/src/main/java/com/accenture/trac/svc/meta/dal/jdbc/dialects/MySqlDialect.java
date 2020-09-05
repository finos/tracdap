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

package com.accenture.trac.svc.meta.dal.jdbc.dialects;

import com.accenture.trac.svc.meta.dal.jdbc.JdbcDialect;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcErrorCode;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class MySqlDialect extends Dialect {

    private static final String DROP_KEY_MAPPING_DDL = "drop temporary table if exists key_mapping;";
    private static final String CREATE_KEY_MAPPING_FILE = "jdbc/mysql/key_mapping.ddl";

    private final Map<Integer, JdbcErrorCode> dialectErrorCodes;

    private final String createKeyMapping;

    MySqlDialect() {

        dialectErrorCodes = new HashMap<>();
        dialectErrorCodes.put(1062, JdbcErrorCode.INSERT_DUPLICATE);
        dialectErrorCodes.put(1452, JdbcErrorCode.INSERT_MISSING_FK);

        createKeyMapping = loadKeyMappingDdl(CREATE_KEY_MAPPING_FILE);
    }

    @Override
    public JdbcDialect dialectCode() {
        return JdbcDialect.MYSQL;
    }

    @Override
    public JdbcErrorCode mapDialectErrorCode(SQLException error) {
        return dialectErrorCodes.getOrDefault(error.getErrorCode(), JdbcErrorCode.UNKNOWN_ERROR_CODE);
    }

    @Override
    public void prepareMappingTable(Connection conn) throws SQLException {

        try (var stmt = conn.createStatement()) {
            stmt.execute(DROP_KEY_MAPPING_DDL);
            stmt.execute(createKeyMapping);
        }
    }
}
