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

package com.accenture.trac.deploy.metadb;

import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.config.StandardArgsProcessor;
import com.accenture.trac.common.db.JdbcSetup;
import com.accenture.trac.common.exception.EStartup;

import org.flywaydb.core.Flyway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Properties;

public class DeployMetaDB {

    private final static String DB_CONFIG_ROOT = "trac.svc.meta.db.sql";
    private final static String SCHEMA_LOCATION = "classpath:%s";

    private final Logger log;
    private final Properties props;

    public DeployMetaDB(Properties props) {

        this.log = LoggerFactory.getLogger(getClass());
        this.props = props;
    }

    public void runDeployment() {

        var dialect = JdbcSetup.selectDialect(props, DB_CONFIG_ROOT);
        var scriptsLocation = String.format(SCHEMA_LOCATION, dialect.name().toLowerCase());

        log.info("SQL Dialect: " + dialect);
        log.info("Scripts location: " + scriptsLocation);

        var dataSource = JdbcSetup.createDatasource(props, DB_CONFIG_ROOT);

        var flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations(scriptsLocation)
                .sqlMigrationPrefix("")
                .sqlMigrationSuffixes(".sql", ".ddl", ".dml")
                .load();

        flyway.migrate();
    }

    public static void main(String[] args) {

        try {

            System.out.println(">>> TRAC Deploy Tool: Meta DB " + "[DEVELOPMENT VERSION]");

            var standardArgs = StandardArgsProcessor.processArgs(args);

            System.out.println(">>> Working directory: " + standardArgs.getWorkingDir());
            System.out.println(">>> Config file: " + standardArgs.getConfigFile());
            System.out.println();

            var configManager = new ConfigManager(standardArgs);
            configManager.initConfigPlugins();
            configManager.initLogging();

            var properties = configManager.loadRootProperties();
            var deploy = new DeployMetaDB(properties);
            deploy.runDeployment();

            System.exit(0);
        }
        catch (EStartup e) {

            if (e.isQuiet())
                System.exit(e.getExitCode());

            System.err.println("The service failed to start: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(e.getExitCode());
        }
        catch (Exception e) {

            System.err.println("There was an unexpected error on the main thread: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(-1);
        }
    }
}
