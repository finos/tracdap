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
import com.accenture.trac.common.config.StandardArgs;
import com.accenture.trac.common.config.StandardArgsProcessor;
import com.accenture.trac.common.db.JdbcSetup;
import com.accenture.trac.common.exception.EStartup;

import com.accenture.trac.common.exception.ETrac;
import com.accenture.trac.common.util.VersionInfo;
import org.flywaydb.core.Flyway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;


public class DeployMetaDB {

    public final static String DEPLOY_SCHEMA_TASK_NAME = "deploy_schema";
    public final static String ADD_TENANT_TASK_NAME = "add_tenant";

    private final static String DB_CONFIG_ROOT = "trac.svc.meta.db.sql";
    private final static String SCHEMA_LOCATION = "classpath:%s";

    private final static List<StandardArgs.Task> METADB_TASKS = List.of(
            StandardArgs.task(DEPLOY_SCHEMA_TASK_NAME, null, "Deploy/update metadata database with the latest physical schema"),
            StandardArgs.task(ADD_TENANT_TASK_NAME, "TENANT_CODE", "Add a new tenant to the metadata database"));

    private final Logger log;
    private final ConfigManager configManager;

    public DeployMetaDB(ConfigManager configManager) {

        this.log = LoggerFactory.getLogger(getClass());
        this.configManager = configManager;
    }

    public void runDeployment(List<StandardArgs.Task> tasks) {

        var componentName = VersionInfo.getComponentName(DeployMetaDB.class);
        var componentVersion = VersionInfo.getComponentVersion(DeployMetaDB.class);
        log.info("{} {}", componentName, componentVersion);

        var properties = configManager.loadRootProperties();
        var dialect = JdbcSetup.getSqlDialect(properties, DB_CONFIG_ROOT);

        // Pick up DB deploy scripts depending on the SQL dialect
        var scriptsLocation = String.format(SCHEMA_LOCATION, dialect.name().toLowerCase());

        log.info("SQL Dialect: " + dialect);
        log.info("Scripts location: " + scriptsLocation);

        var dataSource = JdbcSetup.createDatasource(properties, DB_CONFIG_ROOT);

        try {

            for (var task : tasks) {

                if (DEPLOY_SCHEMA_TASK_NAME.equals(task.getTaskName()))
                    deploySchema(dataSource, scriptsLocation);

                else if (ADD_TENANT_TASK_NAME.equals(task.getTaskName()))
                    addTenant(dataSource, task.getTaskArg());

                else
                    throw new EStartup(String.format("Unknown task: [%s]", task.getTaskName()));
            }

            log.info("All tasks complete");
        }
        finally {

            JdbcSetup.destroyDatasource(dataSource);
        }
    }

    private void deploySchema(DataSource dataSource, String scriptsLocation) {

        log.info("Running task: Deploy schema...");

        var flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations(scriptsLocation)
                .sqlMigrationPrefix("")
                .sqlMigrationSuffixes(".sql", ".ddl", ".dml")
                .load();

        flyway.migrate();
    }

    private void addTenant(DataSource dataSource, String tenantCode) {

        log.info("Running task: Add tenant...");
        log.info("New tenant code: [{}]", tenantCode);

        var maxSelect = "select max(tenant_id) from tenant";
        var insertTenant = "insert into tenant (tenant_id, tenant_code) values (?, ?)";

        short nextId;

        try (var conn = dataSource.getConnection()) {

            try (var stmt = conn.prepareStatement(maxSelect); var rs = stmt.executeQuery()) {

                if (rs.next()) {

                    nextId = rs.getShort(1);

                    if (rs.wasNull())
                        nextId = 1;
                    else
                        nextId++;
                }
                else
                    nextId = 1;
            }

            try (var stmt = conn.prepareStatement(insertTenant)) {

                stmt.setShort(1, nextId);
                stmt.setString(2, tenantCode);
                stmt.execute();
            }
        }
        catch (SQLException e) {

            throw new ETrac("Failed to create tenant: " + e.getMessage(), e);
        }

    }

    public static void main(String[] args) {

        try {

            var componentName = VersionInfo.getComponentName(DeployMetaDB.class);
            var componentVersion = VersionInfo.getComponentVersion(DeployMetaDB.class);
            var startupBanner = String.format(">>> %s %s", componentName, componentVersion);
            System.out.println(startupBanner);

            var standardArgs = StandardArgsProcessor.processArgs(componentName, args, METADB_TASKS);

            System.out.println(">>> Working directory: " + standardArgs.getWorkingDir());
            System.out.println(">>> Config file: " + standardArgs.getConfigFile());
            System.out.println();

            var configManager = new ConfigManager(standardArgs);
            configManager.initConfigPlugins();
            configManager.initLogging();

            var deploy = new DeployMetaDB(configManager);
            deploy.runDeployment(standardArgs.getTasks());

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
