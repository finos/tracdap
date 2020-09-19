/*
 * Copyright 2020 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.accenture.trac.common.db;

import com.accenture.trac.common.exception.EStartup;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;


public class JdbcSetup {

    private static final Map<JdbcDialect, String> JDBC_CLASSES = Map.ofEntries(

            Map.entry(JdbcDialect.H2, "org.h2.jdbcx.JdbcDataSource"),
            //Map.entry(JdbcDialect.MYSQL, "com.mysql.cj.jdbc.MysqlDataSource"),
            Map.entry(JdbcDialect.MYSQL, "org.mariadb.jdbc.MariaDbDataSource"),
            Map.entry(JdbcDialect.MARIADB, "org.mariadb.jdbc.MariaDbDataSource"));
            //Map.entry(JdbcDialect.POSTGRESQL, "org.postgresql.ds.PGSimpleDataSource"),
            //Map.entry(JdbcDialect.SQLSERVER, "com.microsoft.sqlserver.jdbc.SQLServerDataSource"),
            //Map.entry(JdbcDialect.ORACLE, "oracle.jdbc.pool.OracleDataSource"));

    public static JdbcDialect selectDialect(Properties props, String configBase) {

        return getDialect(props, configBase);
    }

    public static DataSource createDatasource(Properties props, String configBase) {

        // TODO: This is a rough implementation that needs cleaning up

        var dialect = getDialect(props, configBase);
        var url = props.getProperty(configBase + ".url");
        var jdbcUrl = String.format("jdbc:%s:%s", dialect.name().toLowerCase(), url);

        var hikariProps = new Properties();
        hikariProps.setProperty("jdbcUrl", jdbcUrl);
        hikariProps.setProperty("poolName", "dal_worker_pool");

        var poolSize = props.getProperty(configBase + ".pool.size");

        if (poolSize != null && !poolSize.isBlank())
            hikariProps.setProperty("maximumPoolSize", poolSize);

        var prefix = configBase + "." + dialect.name().toLowerCase() + ".";

        for (var propKey : props.stringPropertyNames()) {

            if (propKey.startsWith(prefix)) {

                var stem = propKey.substring(prefix.length());
                var hikariPropKey = "dataSource." + stem;

                hikariProps.setProperty(hikariPropKey, props.getProperty(propKey));
            }
        }

        var config = new HikariConfig(hikariProps);
        var source = new HikariDataSource(config);

        var log = LoggerFactory.getLogger(JdbcSetup.class);
        log.info("Database connection pool has " + source.getMaximumPoolSize() + " connections");

        return source;
    }

    private static JdbcDialect getDialect(Properties props, String configBase) {

        var dialectPropKey = configBase + ".dialect";
        var dialect = props.getProperty(dialectPropKey, null);

        if (dialect == null || dialect.isBlank())
            throw new EStartup("Missing required config property: " + dialectPropKey);

        try {
            return Enum.valueOf(JdbcDialect.class, dialect);
        }
        catch (IllegalArgumentException e) {
            throw new EStartup("Unsupported SQL dialect: " + dialect);
        }
    }
}
