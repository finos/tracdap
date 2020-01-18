package trac.svc.meta.dal.jdbc;

public enum JdbcErrorCode {
    UNKNOWN_ERROR_CODE,
    INSERT_DUPLICATE,
    INSERT_MISSING_FK,
    NO_DATA,
    TOO_MANY_ROWS,
    WRONG_OBJECT_TYPE
}
