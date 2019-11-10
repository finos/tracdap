package trac.svc.meta.dal.jdbc.dialects;

import trac.svc.meta.dal.jdbc.JdbcDialect;
import trac.svc.meta.dal.jdbc.JdbcErrorCode;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class MySqlDialect implements IDialect {

    private Map<Integer, JdbcErrorCode> errorCodes;


    MySqlDialect() {

        errorCodes = new HashMap<>();
        errorCodes.put(1062, JdbcErrorCode.INSERT_DUPLICATE);
    }

    @Override
    public JdbcDialect dialectCode() {
        return JdbcDialect.MYSQL;
    }

    @Override
    public JdbcErrorCode mapErrorCode(SQLException error) {

        return errorCodes.getOrDefault(error.getErrorCode(), JdbcErrorCode.UNKNOWN_ERROR_CODE);
    }
}
