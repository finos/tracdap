package com.accenture.trac.svc.meta.dal.jdbc.dialects;

import com.accenture.trac.svc.meta.dal.jdbc.JdbcDialect;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcErrorCode;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class H2SqlDialect extends Dialect {

    private static final Map<Integer, JdbcErrorCode> dialectErrorCodes = Map.ofEntries(
            Map.entry(23505, JdbcErrorCode.INSERT_DUPLICATE),
            Map.entry(1452, JdbcErrorCode.INSERT_MISSING_FK));

    private static final String DROP_KEY_MAPPING_DDL = "drop table if exists key_mapping;";
    private static final String CREATE_KEY_MAPPING_FILE = "jdbc/mysql/key_mapping.ddl";

    private final String createKeyMapping;

    H2SqlDialect() {

        createKeyMapping = loadKeyMappingDdl(CREATE_KEY_MAPPING_FILE);
    }

    @Override
    public JdbcDialect dialectCode() {
        return JdbcDialect.H2;
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
