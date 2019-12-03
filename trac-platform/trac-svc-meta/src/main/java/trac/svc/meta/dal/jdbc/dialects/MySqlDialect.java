package trac.svc.meta.dal.jdbc.dialects;

import trac.svc.meta.dal.jdbc.JdbcDialect;
import trac.svc.meta.dal.jdbc.JdbcErrorCode;
import trac.svc.meta.dal.jdbc.JdbcException;
import trac.svc.meta.exception.TracInternalError;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class MySqlDialect implements IDialect {

    private static final String DROP_KEY_MAPPING_DDL = "drop temporary table if exists key_mapping;";
    private static final String CREATE_KEY_MAPPING_FILE = "jdbc/key_mapping_mysql.ddl";

    private Map<Integer, JdbcErrorCode> errorCodes;
    private Map<Integer, JdbcErrorCode> syntheticErrorCodes;

    private final String createKeyMapping;

    MySqlDialect() {

        errorCodes = new HashMap<>();
        errorCodes.put(1062, JdbcErrorCode.INSERT_DUPLICATE);

        syntheticErrorCodes = new HashMap<>();

        for (var error : JdbcErrorCode.values())
            syntheticErrorCodes.put(error.ordinal(), error);

        createKeyMapping = loadKeyMappingDdl();
    }

    private String loadKeyMappingDdl() {

        var classLoader = getClass().getClassLoader();

        try (var stream = classLoader.getResourceAsStream(CREATE_KEY_MAPPING_FILE)) {

            if (stream == null) {
                var message = "Internal startup error (DAL, JDBC, MySQL - missing DDL resource for key mapping";
                throw new TracInternalError(message);
            }

            try (var rawReader = new InputStreamReader(stream); var reader = new BufferedReader(rawReader)) {
                return reader.lines().collect(Collectors.joining(System.lineSeparator()));
            }
        }
        catch (IOException e) {
            var message = "Internal startup error (DAL, JDBC, MySQL - error preparing DDL resource for key mapping";
            throw new TracInternalError(message);
        }
    }

    @Override
    public JdbcDialect dialectCode() {
        return JdbcDialect.MYSQL;
    }

    @Override
    public JdbcErrorCode mapErrorCode(SQLException error) {

        if (JdbcException.SYNTHETIC_ERROR.equals(error.getSQLState()))
            return syntheticErrorCodes.getOrDefault(error.getErrorCode(), JdbcErrorCode.UNKNOWN_ERROR_CODE);
        else
            return errorCodes.getOrDefault(error.getErrorCode(), JdbcErrorCode.UNKNOWN_ERROR_CODE);
    }

    @Override
    public void prepareMappingTable(Connection conn) throws SQLException {

        try (var stmt = conn.createStatement()) {
            stmt.execute(DROP_KEY_MAPPING_DDL);
            stmt.execute(createKeyMapping);
        }
    }
}
