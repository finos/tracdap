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

import org.finos.tracdap.common.db.JdbcDialect;
import org.finos.tracdap.common.db.dialects.IDialect;
import org.finos.tracdap.common.exception.ETracInternal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class JdbcDialects {

    private static final Map<JdbcDialect, String> MAPPING_TABLE_NAME = Map.ofEntries(
            Map.entry(JdbcDialect.H2, "key_mapping"),
            Map.entry(JdbcDialect.MYSQL, "key_mapping"),
            Map.entry(JdbcDialect.MARIADB, "key_mapping"),
            Map.entry(JdbcDialect.POSTGRESQL, "key_mapping"),
            Map.entry(JdbcDialect.SQLSERVER, "#key_mapping"),
            Map.entry(JdbcDialect.ORACLE, "key_mapping")
    );

    private static final Map<JdbcDialect, String> MAPPING_TABLE_DDL_RESET = Map.ofEntries(
            Map.entry(JdbcDialect.H2, "drop table if exists key_mapping;"),
            Map.entry(JdbcDialect.MYSQL, "drop temporary table if exists key_mapping;"),
            Map.entry(JdbcDialect.MARIADB, "drop temporary table if exists key_mapping;"),
            Map.entry(JdbcDialect.SQLSERVER, "drop table if exists #key_mapping;")
    );

    private static final Map<JdbcDialect, String> MAPPING_TABLE_DDL_FILE = Map.ofEntries(
            Map.entry(JdbcDialect.H2, "jdbc/h2/key_mapping.ddl"),
            Map.entry(JdbcDialect.MYSQL, "jdbc/mysql/key_mapping.ddl"),
            Map.entry(JdbcDialect.MARIADB, "jdbc/mariadb/key_mapping.ddl"),
            Map.entry(JdbcDialect.POSTGRESQL, "jdbc/postgresql/key_mapping.ddl"),
            Map.entry(JdbcDialect.SQLSERVER, "jdbc/sqlserver/key_mapping.ddl")
    );

    private static final Map<JdbcDialect, String> MAPPING_TABLE_DDL_SCRIPT = new HashMap<>();

    public static String mappingTableName(IDialect dialect) {
        return mappingTableName(dialect.dialectCode());
    }

    public static String mappingTableName(JdbcDialect dialectCode) {

        var tableName = MAPPING_TABLE_NAME.get(dialectCode);

        if (tableName == null)
            throw new ETracInternal("Mapping table not available for SQL dialect: " + dialectCode.name());

        return tableName;
    }

    public static void prepareMappingTable(Connection conn, IDialect dialect) throws SQLException {
        prepareMappingTable(conn, dialect.dialectCode());
    }

    public static void prepareMappingTable(Connection conn, JdbcDialect dialectCode) throws SQLException {

        // For Oracle this is a no-op, global temporary table is deployed as part of Oracle schema
        if (dialectCode == JdbcDialect.ORACLE)
            return;

        var resetScript = MAPPING_TABLE_DDL_RESET.get(dialectCode);
        var ddlScript = lookupDdlScript(dialectCode);

        try (var stmt = conn.createStatement()) {

            if (resetScript != null)
                stmt.execute(resetScript);

            stmt.execute(ddlScript);
        }
    }

    private static String lookupDdlScript(JdbcDialect dialectCode) {

        var ddlScript = MAPPING_TABLE_DDL_SCRIPT.get(dialectCode);

        if (ddlScript != null && !ddlScript.isEmpty())
            return ddlScript;

        var ddlFile = MAPPING_TABLE_DDL_FILE.get(dialectCode);

        if (ddlFile == null)
            throw new ETracInternal("Mapping table DDL not available for SQL dialect: " + dialectCode.name());

        var loadedDdlScript = loadDdlFile(ddlFile, dialectCode);

        MAPPING_TABLE_DDL_SCRIPT.put(dialectCode, loadedDdlScript);

        return loadedDdlScript;
    }

    private static String loadDdlFile(String ddlFile, JdbcDialect dialectCode) {

        var classLoader = JdbcDialects.class.getClassLoader();

        try (var stream = classLoader.getResourceAsStream(ddlFile)) {

            if (stream == null) {
                var message = String.format("Internal startup error (DAL, JDBC, %s - missing DDL resource for key mapping", dialectCode);
                throw new ETracInternal(message);
            }

            try (var rawReader = new InputStreamReader(stream); var reader = new BufferedReader(rawReader)) {
                return reader.lines().collect(Collectors.joining(System.lineSeparator()));
            }
        }
        catch (IOException e) {
            var message = String.format("Internal startup error (DAL, JDBC, %s - error preparing DDL resource for key mapping", dialectCode);
            throw new ETracInternal(message);
        }
    }
}
