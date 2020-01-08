package trac.svc.meta.dal.jdbc.dialects;

import trac.svc.meta.dal.jdbc.JdbcDialect;
import trac.svc.meta.dal.jdbc.JdbcErrorCode;
import trac.svc.meta.dal.jdbc.JdbcException;
import trac.svc.meta.exception.TracInternalError;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class Dialect implements IDialect {

    public static IDialect dialectFor(JdbcDialect dialect) {

        switch (dialect) {

            case MYSQL: return new MySqlDialect();
            case H2: return new H2SqlDialect();

            default: throw new RuntimeException("Unsupported JDBC dialect: " + dialect.name());
        }
    }


    private Map<Integer, JdbcErrorCode> syntheticErrorCodes;

    protected Dialect() {

        syntheticErrorCodes = new HashMap<>();

        for (var error : JdbcErrorCode.values())
            syntheticErrorCodes.put(error.ordinal(), error);
    }

    @Override
    public final JdbcErrorCode mapErrorCode(SQLException error) {

        if (JdbcException.SYNTHETIC_ERROR.equals(error.getSQLState()))
            return syntheticErrorCodes.getOrDefault(error.getErrorCode(), JdbcErrorCode.UNKNOWN_ERROR_CODE);
        else
            return mapDialectErrorCode(error);
    }

    protected abstract JdbcErrorCode mapDialectErrorCode(SQLException error);

    protected String loadKeyMappingDdl(String keyMappingDdl) {

        var classLoader = getClass().getClassLoader();

        try (var stream = classLoader.getResourceAsStream(keyMappingDdl)) {

            if (stream == null) {
                var message = String.format("Internal startup error (DAL, JDBC, %s - missing DDL resource for key mapping", dialectCode());
                throw new TracInternalError(message);
            }

            try (var rawReader = new InputStreamReader(stream); var reader = new BufferedReader(rawReader)) {
                return reader.lines().collect(Collectors.joining(System.lineSeparator()));
            }
        }
        catch (IOException e) {
            var message = String.format("Internal startup error (DAL, JDBC, %s - error preparing DDL resource for key mapping", dialectCode());
            throw new TracInternalError(message);
        }
    }
}
