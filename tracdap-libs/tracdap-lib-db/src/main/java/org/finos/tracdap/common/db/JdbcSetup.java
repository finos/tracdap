/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.db;

import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.ETracInternal;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;


public class JdbcSetup {

    private static final String DIALECT_PROPERTY = "dialect";
    private static final String JDBC_URL_PROPERTY = "jdbcUrl";

    public static JdbcDialect getSqlDialect(Properties props, String configBase) {

        var dialectPropKey = configBase + DIALECT_PROPERTY;
        var dialect = props.getProperty(dialectPropKey, null);

        if (dialect == null || dialect.isBlank())
            throw new EStartup("Missing required config property: " + dialectPropKey);

        try {
            return Enum.valueOf(JdbcDialect.class, dialect);
        }
        catch (IllegalArgumentException e) {
            throw new EStartup(String.format("Unsupported SQL dialect: [%s]", dialect));
        }
    }

    public static DataSource createDatasource(Properties props, String configBase) {

        try {
            var hikariProps = createHikariProperties(props, configBase);

            var config = new HikariConfig(hikariProps);
            var source = new HikariDataSource(config);

            var log = LoggerFactory.getLogger(JdbcSetup.class);
            log.info("Database connection pool has " + source.getMaximumPoolSize() + " connections");

            return source;
        }
        catch (RuntimeException e) {

            // For some error conditions, the original cause contains useful extra info
            // Particularly in the case of missing JDBC drivers!
            if (e.getCause() instanceof SQLException)
                if (!e.getMessage().contains(e.getCause().getMessage())) {

                var messageTemplate = "Could not connect to database: %s (%s)";
                var message = String.format(messageTemplate, e.getMessage(), e.getCause().getMessage());

                throw new EStartup(message, e);
            }

            var messageTemplate = "Could not connect to database: %s";
            var message = String.format(messageTemplate, e.getMessage());

            throw new EStartup(message, e);
        }
    }

    public static void destroyDatasource(DataSource source) {

        if (!(source instanceof HikariDataSource))
            throw new ETracInternal("Datasource being destroyed was not created by JdbcSetup");

        var hikariSource = (HikariDataSource) source;
        hikariSource.close();
    }

    private static Properties createHikariProperties(Properties props, String configBase) {

        // Properties may be either qualified or unqualified (i.e. configBase is empty)
        // Properties for JDBC are specified without a leading dot, to allow for unqualified props
        // So, the config base is not blank it must end with a dot
        if (!configBase.isEmpty() && !configBase.endsWith("."))
            configBase = configBase + ".";

        var dialect = getSqlDialect(props, configBase);
        var jdbcUrl = buildJdbcUrl(props, configBase, dialect);

        var hikariProps = new Properties();
        hikariProps.setProperty("jdbcUrl", jdbcUrl);

        copyDialectProperties(props, hikariProps, configBase, dialect);

        hikariProps.setProperty("poolName", "dal_worker_pool");

        var poolSize = props.getProperty(configBase + "pool.size");

        if (poolSize != null && !poolSize.isBlank())
            hikariProps.setProperty("maximumPoolSize", poolSize);

        return hikariProps;
    }

    private static String buildJdbcUrl(Properties rootProps, String configBase, JdbcDialect dialect) {

        var jdbcUrlProperty = configBase + JDBC_URL_PROPERTY;
        var jdbcUrlFromConfig = rootProps.getProperty(jdbcUrlProperty);

        return String.format("jdbc:%s:%s", dialect.name().toLowerCase(), jdbcUrlFromConfig);
    }

    private static void copyDialectProperties(
            Properties rootProps, Properties hikariProps,
            String configBase, JdbcDialect dialect) {

        var dialectPrefixFormat = "%s%s.";  // Trailing dot is required!
        var dialectPrefix = String.format(
                dialectPrefixFormat,
                configBase, dialect.name().toLowerCase());

        for (var propKey : rootProps.stringPropertyNames()) {

            if (propKey.startsWith(dialectPrefix)) {

                var dialectProperty = propKey.substring(dialectPrefix.length());
                var hikariPropKey = "dataSource." + dialectProperty;
                var propValue = rootProps.getProperty(propKey);

                hikariProps.setProperty(hikariPropKey, propValue);
            }
        }
    }
}
