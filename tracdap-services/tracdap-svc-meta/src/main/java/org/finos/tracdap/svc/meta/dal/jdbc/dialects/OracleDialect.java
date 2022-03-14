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

public class OracleDialect extends Dialect {

    private static final Map<Integer, JdbcErrorCode> dialectErrorCodes = Map.ofEntries(
            Map.entry(1, JdbcErrorCode.INSERT_DUPLICATE),  // ORA-00001: unique constraint violated
            Map.entry(2291, JdbcErrorCode.INSERT_MISSING_FK));  // ORA-02291: integrity constraint violated - parent key not found

    private static final String MAPPING_TABLE_NAME = "key_mapping";

    @Override
    public JdbcDialect dialectCode() {
        return JdbcDialect.ORACLE;
    }

    @Override
    public JdbcErrorCode mapDialectErrorCode(SQLException error) {
        return dialectErrorCodes.getOrDefault(error.getErrorCode(), JdbcErrorCode.UNKNOWN_ERROR_CODE);
    }

    @Override
    public void prepareMappingTable(Connection conn) {
        // NO-OP, global temporary table deployed as part of Oracle schema
    }

    @Override
    public String mappingTableName() {
        return MAPPING_TABLE_NAME;
    }

    @Override
    public boolean supportsGeneratedKeys() {
        return true;
    }

    @Override
    public int booleanType() {
        // Oracle does not have a BOOLEAN type, we use NUMBER(1) with true = 1, false = 0
        return Types.NUMERIC;
    }
}
