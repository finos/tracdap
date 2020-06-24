package com.accenture.trac.svc.meta.dal.jdbc;

public enum JdbcErrorCode {
    UNKNOWN_ERROR_CODE,
    INSERT_DUPLICATE,
    INSERT_MISSING_FK,
    NO_DATA,
    TOO_MANY_ROWS,

    // Object type of a metadata item does not match what is stored / expected
    WRONG_OBJECT_TYPE,

    // The definition of a metadata item could not be understood
    INVALID_OBJECT_DEFINITION
}
