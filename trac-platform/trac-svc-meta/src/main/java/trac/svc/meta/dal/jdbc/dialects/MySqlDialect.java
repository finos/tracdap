package trac.svc.meta.dal.jdbc.dialects;

import trac.svc.meta.dal.jdbc.JdbcDialect;
import trac.svc.meta.dal.jdbc.JdbcErrorCode;
import trac.svc.meta.dal.jdbc.JdbcException;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class MySqlDialect implements IDialect {

    private Map<Integer, JdbcErrorCode> errorCodes;
    private Map<Integer, JdbcErrorCode> syntheticErrorCodes;

    MySqlDialect() {

        errorCodes = new HashMap<>();
        errorCodes.put(1062, JdbcErrorCode.INSERT_DUPLICATE);

        syntheticErrorCodes = new HashMap<>();

        for (var error : JdbcErrorCode.values())
            syntheticErrorCodes.put(error.ordinal(), error);
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
}
