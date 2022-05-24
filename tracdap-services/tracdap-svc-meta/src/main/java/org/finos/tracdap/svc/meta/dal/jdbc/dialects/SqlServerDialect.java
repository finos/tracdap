/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.meta.dal.jdbc.dialects;

import org.finos.tracdap.common.db.JdbcDialect;
import org.finos.tracdap.svc.meta.dal.jdbc.JdbcErrorCode;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;


public class SqlServerDialect extends Dialect {

    private static final Map<Integer, JdbcErrorCode> dialectErrorCodes = Map.ofEntries(
            Map.entry(2601, JdbcErrorCode.INSERT_DUPLICATE),
            Map.entry(2627, JdbcErrorCode.INSERT_DUPLICATE),
            Map.entry(547, JdbcErrorCode.INSERT_MISSING_FK));

    private static final String DROP_KEY_MAPPING_DDL = "drop table if exists #key_mapping;";
    private static final String CREATE_KEY_MAPPING_FILE = "jdbc/sqlserver/key_mapping.ddl";
    private static final String MAPPING_TABLE_NAME = "#key_mapping";

    private final String createKeyMapping;

    SqlServerDialect() {

        createKeyMapping = loadKeyMappingDdl(CREATE_KEY_MAPPING_FILE);
    }

    @Override
    public JdbcDialect dialectCode() {
        return JdbcDialect.SQLSERVER;
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

    @Override
    public String mappingTableName() {
        return MAPPING_TABLE_NAME;
    }

    @Override
    public boolean supportsGeneratedKeys() {
        return false;
    }

    @Override
    public int booleanType() {
        return Types.BOOLEAN;
    }
}
