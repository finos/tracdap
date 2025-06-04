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

package org.finos.tracdap.common.metadata.test;

import org.finos.tracdap.common.db.JdbcDialect;
import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.util.InterfaceLogging;
import org.finos.tracdap.common.metadata.store.IMetadataStore;
import org.finos.tracdap.common.metadata.store.jdbc.JdbcMetadataStore;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

import static org.finos.tracdap.test.meta.SampleMetadata.ALT_TEST_TENANT;
import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;


public class JdbcUnit implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback {

    private static final String SCRIPT_LOCATION = "tracdap-libs/tracdap-lib-meta/src/schema/h2/rollout";

    private static final String JDBC_URL_TEMPLATE = "mem:%s;DB_CLOSE_DELAY=-1";

    private Properties properties;
    private DataSource source;
    private JdbcMetadataStore dal;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        var dbId = UUID.randomUUID();
        var jdbcUrl = String.format(JDBC_URL_TEMPLATE, dbId);

        properties = new Properties();
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

            // Create the test tenant
            var tenantStmt = "insert into tenant (tenant_id, tenant_code, description) values (?, ?, ?)";

            try (var conn = source.getConnection(); var stmt = conn.prepareStatement(tenantStmt)) {

                stmt.setShort(1, (short) 1);
                stmt.setString(2, TEST_TENANT);
                stmt.setString(3, "Test tenant");

                stmt.execute();

                stmt.setShort(1, (short) 2);
                stmt.setString(2, ALT_TEST_TENANT);
                stmt.setString(3, "Alt test tenant");

                stmt.execute();
            }
        }
        finally {

            if (source != null) {
                JdbcSetup.destroyDatasource(source);
                source = null;
            }
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {

        var testClass = context.getTestClass();

        if (testClass.isEmpty() || !IMetadataStoreTest.class.isAssignableFrom(testClass.get()))
            Assertions.fail("JUnit extension for DAL testing requires the test class to implement IDalTestable");

        source = JdbcSetup.createDatasource(properties);
        dal = new JdbcMetadataStore(JdbcDialect.H2, source);
        dal.start();

        var dalWithLogging = InterfaceLogging.wrap(dal, IMetadataStore.class);
        var testInstance = context.getTestInstance();

        if (testInstance.isPresent()) {
            var testCase = (IMetadataStoreTest) testInstance.get();
            testCase.setStore(dalWithLogging);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {

        if (dal != null) {
            dal.stop();
            dal = null;
            source = null;
        }
    }
}
