package trac.svc.meta.dal.jdbc.dialects;

import trac.svc.meta.dal.jdbc.JdbcDialect;

public class Dialect {

    public static IDialect dialectFor(JdbcDialect dialect) {

        switch (dialect) {
            case MYSQL: return new MySqlDialect();

            default: throw new RuntimeException("Unsupported JDBC dialect: " + dialect.name());
        }

    }
}
