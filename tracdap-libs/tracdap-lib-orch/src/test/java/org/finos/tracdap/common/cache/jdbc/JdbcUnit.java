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

package org.finos.tracdap.common.cache.jdbc;

import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.exception.EUnexpected;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

public class JdbcUnit implements BeforeAllCallback, AfterAllCallback {

    private static final String SCRIPT_LOCATION = "tracdap-libs/tracdap-lib-orch/src/schema/h2/rollout";

    private static final String JDBC_URL_TEMPLATE = "mem:%s;DB_CLOSE_DELAY=-1";

    private DataSource source;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        var dbId = UUID.randomUUID();
        var jdbcUrl = String.format(JDBC_URL_TEMPLATE, dbId);

        var properties = new Properties();
        properties.setProperty("jdbcUrl", jdbcUrl);
        properties.setProperty("dialect", "H2");
        properties.setProperty("h2.user", "trac");
        properties.setProperty("h2.pass", "trac");
        properties.setProperty("pool.size", "2");

        try {

            source = JdbcSetup.createDatasource(properties);

            // Find project root dir
            var tracRepoDir = Paths.get(".").toAbsolutePath();
            while (!Files.exists(tracRepoDir.resolve("tracdap-api")))
                tracRepoDir = tracRepoDir.getParent();

            var scriptLocation = "filesystem:" + tracRepoDir.resolve(SCRIPT_LOCATION);

            // Use Flyway to deploy the schema
            var flyway = Flyway.configure()
                    .dataSource(source)
                    .locations(scriptLocation)
                    .sqlMigrationPrefix("")
                    .sqlMigrationSuffixes(".sql", ".ddl", ".dml")
                    .load();

            flyway.migrate();

            source = JdbcSetup.createDatasource(properties);

            var dialect = JdbcSetup.getSqlDialect(properties);
            var manager = new JdbcJobCacheManager(source, dialect);

            var testClassMaybe = context.getTestClass();

            if (testClassMaybe.isEmpty())
                throw new EUnexpected();

            var testClass = testClassMaybe.get();
            var managerField = testClass.getDeclaredField("cacheManager");
            managerField.setAccessible(true);
            managerField.set(null, manager);
        }
        catch (Exception e) {

            if (source != null) {
                JdbcSetup.destroyDatasource(source);
                source = null;
            }

            throw e;
        }
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {

        if (source != null) {
            JdbcSetup.destroyDatasource(source);
            source = null;
        }
    }
}
