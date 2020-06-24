package trac.svc.meta.dal.jdbc.dialects;

import trac.svc.meta.dal.jdbc.JdbcDialect;
import trac.svc.meta.dal.jdbc.JdbcErrorCode;

import java.sql.Connection;
import java.sql.SQLException;


public interface IDialect {

    JdbcDialect dialectCode();

    JdbcErrorCode mapErrorCode(SQLException e);

    void prepareMappingTable(Connection conn) throws SQLException;
}
