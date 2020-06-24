package com.accenture.trac.svc.meta.dal.jdbc.dialects;

import com.accenture.trac.svc.meta.dal.jdbc.JdbcDialect;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcErrorCode;

import java.sql.Connection;
import java.sql.SQLException;


public interface IDialect {

    JdbcDialect dialectCode();

    JdbcErrorCode mapErrorCode(SQLException e);

    void prepareMappingTable(Connection conn) throws SQLException;
}
