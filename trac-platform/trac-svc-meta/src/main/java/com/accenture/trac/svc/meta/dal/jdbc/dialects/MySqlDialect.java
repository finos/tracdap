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
