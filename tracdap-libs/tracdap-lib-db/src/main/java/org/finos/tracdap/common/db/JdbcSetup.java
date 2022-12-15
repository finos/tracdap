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

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.ETracInternal;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.finos.tracdap.config.PluginConfig;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;


public class JdbcSetup {

    private static final String DIALECT_PROPERTY = "dialect";
    private static final String JDBC_URL_PROPERTY = "jdbcUrl";

    public static JdbcDialect getSqlDialect(PluginConfig config) {

        var dialect = config.getPropertiesOrDefault(DIALECT_PROPERTY, null);
        return checkSqlDialect(dialect);
    }

    public static JdbcDialect getSqlDialect(Properties properties) {

        var dialect = properties.getProperty(DIALECT_PROPERTY, null);
        return checkSqlDialect(dialect);
    }

    private static JdbcDialect checkSqlDialect(String dialect) {

        if (dialect == null || dialect.isBlank())
            throw new EStartup("Missing required config property: " + DIALECT_PROPERTY);

        try {
            return Enum.valueOf(JdbcDialect.class, dialect);
        }
        catch (IllegalArgumentException e) {
            throw new EStartup(String.format("Unsupported SQL dialect: [%s]", dialect));
        }
    }

    public static DataSource createDatasource(ConfigManager configManager, PluginConfig config) {

        // This is the same behavior as the plugin manager when it loads standard plugins
        // Needed when data sources are created in isolation, e.g. tests and the DB deploy tool

        var properties = new Properties();
        properties.putAll(config.getPropertiesMap());

        for (var secret : config.getSecretsMap().entrySet()) {
            var secretKey = secret.getKey();
            var secretValue = configManager.loadPassword(secret.getValue());
            properties.put(secretKey, secretValue);
        }

        return createDatasource(properties);
    }

    public static DataSource createDatasource(Properties properties) {

        try {
            var hikariProps = createHikariProperties(properties);

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

    private static Properties createHikariProperties(Properties properties) {

        var dialect = getSqlDialect(properties);
        var jdbcUrl = buildJdbcUrl(properties, dialect);

        var hikariProps = new Properties();
        hikariProps.setProperty("jdbcUrl", jdbcUrl);

        copyDialectProperties(properties, hikariProps, dialect);

        hikariProps.setProperty("poolName", "dal_worker_pool");

        var poolSize = properties.getProperty("pool.size");

        if (poolSize != null && !poolSize.isBlank())
            hikariProps.setProperty("maximumPoolSize", poolSize);

        return hikariProps;
    }

    private static String buildJdbcUrl(Properties rootProps, JdbcDialect dialect) {

        var jdbcUrlFromConfig = rootProps.getProperty(JDBC_URL_PROPERTY);

        return String.format("jdbc:%s:%s", dialect.name().toLowerCase(), jdbcUrlFromConfig);
    }

    private static void copyDialectProperties(Properties rootProps, Properties hikariProps, JdbcDialect dialect) {

        var dialectPrefix = dialect.name().toLowerCase() + ".";  // Trailing dot is required!

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
