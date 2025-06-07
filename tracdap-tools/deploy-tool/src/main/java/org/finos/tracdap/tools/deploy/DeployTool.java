/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.tools.deploy;

import org.finos.tracdap.common.config.ConfigHelpers;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.ETracPublic;
import org.finos.tracdap.common.startup.StandardArgs;
import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.startup.Startup;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.PluginConfig;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;


/**
 * Deployment tool to manage the TRAC metadata database
 */
public class DeployTool {

    /** Task name for deploying the schema **/
    public final static String DEPLOY_SCHEMA_TASK = "deploy_schema";

    /** Task name for deploying the job cache schema **/
    public final static String DEPLOY_CACHE_SCHEMA_TASK = "deploy_cache_schema";

    /** Task name for adding a tenant **/
    public final static String ADD_TENANT_TASK = "add_tenant";

    /** Task name for activating tenants from the tenants config file **/
    public final static String ACTIVATE_TENANT_CONFIG_TASK = "activate_tenant_config";

    /** Task name for running native SQL against the database **/
    public final static String NATIVE_SQL_TASK = "native_sql";

    private final static String SCHEMA_LOCATION = "classpath:%s/rollout";

    private final static String CACHE_SCHEMA_LOCATION = "classpath:cache/%s/rollout";

    private final static List<StandardArgs.Task> METADB_TASKS = List.of(
            StandardArgs.task(DEPLOY_SCHEMA_TASK, "Deploy/update metadata database with the latest physical schema"),
            StandardArgs.task(DEPLOY_CACHE_SCHEMA_TASK, "Deploy/update job cache database with the latest physical schema"),
            StandardArgs.task(ADD_TENANT_TASK, List.of("CODE", "DISPLAY_NAME"), "Add a new tenant code in the metadata database"),
            StandardArgs.task(ACTIVATE_TENANT_CONFIG_TASK, List.of("CODE"), "Activate any tenant codes found in the tenants config file"),
            StandardArgs.task(NATIVE_SQL_TASK, List.of("SQL_FILE"), "Load a native SQL file and run it against the database"));

    private final Logger log;
    private final ConfigManager configManager;

    /**
     * Entry point for the DeployMetaDB tool.
     *
     * @param args Command line args
     */
    public static void main(String[] args) {

        try {

            var startup = Startup.useCommandLine(DeployTool.class, args, METADB_TASKS);
            startup.runStartupSequence();

            var config = startup.getConfig();
            var tasks = startup.getArgs().getTasks();

            var deploy = new DeployTool(config);
            deploy.runDeployment(tasks);

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

    /**
     * Construct a new instance of the deployment tool
     *
     * @param configManager A prepared instance of ConfigManager
     */
    public DeployTool(ConfigManager configManager) {

        this.log = LoggerFactory.getLogger(getClass());
        this.configManager = configManager;
    }

    /**
     * Run deployment tasks, as specified by standard args tasks on the commane line
     *
     * @param tasks The list of deployment tasks to execute
     */
    public void runDeployment(List<StandardArgs.Task> tasks) {

        var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
        var metadataStoreConfig = platformConfig.getMetadataStore();
        var jobCacheConfig = platformConfig.getJobCache();

        for (var task : tasks) {

            // Config for the database component used at runtime
            // Deploy cache schema uses jobCache config by default, everything else uses metadataStore
            var runtimeConfig = DEPLOY_CACHE_SCHEMA_TASK.equals(task.getTaskName())
                    ? jobCacheConfig : metadataStoreConfig;

            // Allow config to be overridden for individual setup tasks
            var databaseConfig = platformConfig.containsSetupTasks(task.getTaskName())
                    ? platformConfig.getSetupTasksOrThrow(task.getTaskName())
                    : runtimeConfig;

            DataSource dataSource = null;

            try {

                if (DEPLOY_SCHEMA_TASK.equals(task.getTaskName())) {
                    dataSource = createSource(databaseConfig);
                    deploySchema(dataSource, databaseConfig);
                }

                else if (DEPLOY_CACHE_SCHEMA_TASK.equals(task.getTaskName())) {
                    if (!List.of("JDBC", "SQL").contains(jobCacheConfig.getProtocol().toUpperCase()))
                        throw new EStartup("Cache schema cannot be deployed because the job cache is not configured to use SQL");
                    dataSource = createSource(databaseConfig);
                    deployCacheSchema(dataSource, databaseConfig);
                }

                else if (ADD_TENANT_TASK.equals(task.getTaskName())) {
                    dataSource = createSource(databaseConfig);
                    addTenant(dataSource, task.getTaskArg(0), task.getTaskArg(1));
                }

                else if (ACTIVATE_TENANT_CONFIG_TASK.equals(task.getTaskName())) {
                    dataSource = createSource(databaseConfig);
                    activateTenantConfig(dataSource);
                }

                else if (NATIVE_SQL_TASK.equals(task.getTaskName())) {
                    dataSource = createSource(databaseConfig);
                    nativeSql(dataSource, task.getTaskArg(0));
                }

                else
                    throw new EStartup(String.format("Unknown task: [%s]", task.getTaskName()));
            }
            finally {

                if (dataSource != null)
                    JdbcSetup.destroyDatasource(dataSource);
            }
        }

        log.info("All tasks complete");
    }

    private DataSource createSource(PluginConfig pluginConfig) {

        return JdbcSetup.createDatasource(configManager, pluginConfig);
    }

    private void deploySchema(DataSource dataSource, PluginConfig databaseConfig) {

        log.info("Running task: Deploy schema...");

        var dialect = JdbcSetup.getSqlDialect(databaseConfig);
        var scriptsLocation = String.format(SCHEMA_LOCATION, dialect.name().toLowerCase());

        log.info("Metadata store scripts location: {}", scriptsLocation);

        var flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations(scriptsLocation)
                .sqlMigrationPrefix("")
                .sqlMigrationSuffixes(".sql", ".ddl", ".dml")
                .load();

        flyway.migrate();
    }

    private void deployCacheSchema(DataSource dataSource, PluginConfig databaseConfig) {

        log.info("Running task: Deploy cache schema...");

        var dialect = JdbcSetup.getSqlDialect(databaseConfig);
        var scriptsLocation = String.format(CACHE_SCHEMA_LOCATION, dialect.name().toLowerCase());

        log.info("Job cache scripts location: {}", scriptsLocation);

        var flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations(scriptsLocation)
                .sqlMigrationPrefix("")
                .sqlMigrationSuffixes(".sql", ".ddl", ".dml")
                .load();

        flyway.migrate();
    }

    private void addTenant(DataSource dataSource, String tenantCode, String displayName) {

        log.info("Running task: Add tenant...");

        try (var conn = dataSource.getConnection()) {

            addTenant(conn, tenantCode, displayName);
        }
        catch (SQLException e) {

            throw new ETracPublic("Failed to add tenant: " + e.getMessage(), e);
        }
    }

    private void activateTenantConfig(DataSource dataSource) {

        log.info("Running task: Activate tenant config...");

        var tenantConfigMap = ConfigHelpers.loadTenantConfigMap(configManager);

        try (var conn = dataSource.getConnection()) {

            for (var tenantCode : tenantConfigMap.getTenantsMap().keySet()) {

                var tenantConfig = tenantConfigMap.getTenantsMap().get(tenantCode);
                var displayName = tenantConfig.getPropertiesOrDefault("tenant.displayName", "");

                if (!checkTenantCodeExists(conn, tenantCode))
                    addTenant(conn, tenantCode, displayName);
            }
        }
        catch (SQLException e) {

            throw new ETracPublic("Failed to activate tenant config: " + e.getMessage(), e);
        }
    }

    private void addTenant(Connection conn, String tenantCode, String displayName) throws SQLException {

        log.info("New tenant code: [{}]", tenantCode);

        var findMaxId = "select max(tenant_id) from tenant";
        var insertTenant = "insert into tenant (tenant_id, tenant_code, description) values (?, ?, ?)";

        short nextId;

        try (var stmt = conn.prepareStatement(findMaxId); var rs = stmt.executeQuery()) {

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

            if (displayName != null && !displayName.isBlank())
                stmt.setString(3, displayName);
            else
                stmt.setNull(3, java.sql.Types.VARCHAR);

            stmt.execute();
        }
    }

    private boolean checkTenantCodeExists(Connection conn, String tenantCode) throws SQLException {

        var findTenant = "select tenant_id from tenant where tenant_code = ?";

        try (var stmt = conn.prepareStatement(findTenant)) {

            stmt.setString(1, tenantCode);

            try (var rs = stmt.executeQuery()) {

                // True if a record exists
                return rs.next();
            }
        }
        catch (SQLException e) {

            throw new ETracPublic("Failed to alter tenant: " + e.getMessage(), e);
        }
    }

    private void nativeSql(DataSource dataSource, String sqlFile) {

        log.info("Running task: Native SQL...");

        try (var conn = dataSource.getConnection(); var stmt = conn.createStatement()) {

            var sqlScript = Files.readString(Paths.get(sqlFile), StandardCharsets.UTF_8);

            stmt.execute(sqlScript);
        }
        catch (SQLException e) {

            throw new ETracPublic("Failed to execute native SQL: " + e.getMessage(), e);
        }
        catch (IOException e) {

            throw new ETracPublic("Failed to load native SQL: " + e.getMessage(), e);
        }
    }
}
