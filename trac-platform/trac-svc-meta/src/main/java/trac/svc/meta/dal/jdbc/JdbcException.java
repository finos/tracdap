package trac.svc.meta.dal.jdbc;

import java.sql.SQLException;


public class JdbcException extends SQLException {

    public static final String SYNTHETIC_ERROR = "SYNTHETIC_ERROR";

    JdbcException(JdbcErrorCode errorCode) {
        super(errorCode.name(), SYNTHETIC_ERROR, errorCode.ordinal());
    }
}
