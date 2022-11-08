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

package org.finos.tracdap.test.meta;

import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.util.InterfaceLogging;
import org.finos.tracdap.common.db.JdbcDialect;
import org.finos.tracdap.svc.meta.dal.IMetadataDal;
import org.finos.tracdap.svc.meta.dal.jdbc.JdbcMetadataDal;

import org.flywaydb.core.Flyway;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.*;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

import static org.finos.tracdap.test.meta.TestData.TEST_TENANT;


public class JdbcUnit implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

    private static final String SCRIPT_LOCATION = "tracdap-services/tracdap-svc-meta/src/schema/h2/rollout";

    private static final String JDBC_URL_TEMPLATE = "mem:%s;DB_CLOSE_DELAY=-1";

    private DataSource source;
    private JdbcMetadataDal dal;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        var dbId = UUID.randomUUID();
        var jdbcUrl = String.format(JDBC_URL_TEMPLATE, dbId);

        var props = new Properties();
        props.setProperty("unit.jdbcUrl", jdbcUrl);
        props.setProperty("unit.dialect", "H2");
        props.setProperty("unit.h2.user", "trac");
        props.setProperty("unit.h2.pass", "trac");
        props.setProperty("unit.pool.size", "2");

        source = JdbcSetup.createDatasource(props, "unit");

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
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {

        var testClass = context.getTestClass();

        if (testClass.isEmpty() || !IDalTestable.class.isAssignableFrom(testClass.get()))
            Assertions.fail("JUnit extension for DAL testing requires the test class to implement IDalTestable");

        var dal = new JdbcMetadataDal(JdbcDialect.H2, source, Runnable::run);
        dal.startup();

        this.dal = dal;

        var dalWithLogging = InterfaceLogging.wrap(dal, IMetadataDal.class);
        var testInstance = context.getTestInstance();

        if (testInstance.isPresent()) {
            var testCase = (IDalTestable) testInstance.get();
            testCase.setDal(dalWithLogging);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {

        if (dal != null)
            dal.shutdown();
    }

    @Override
    public void afterAll(ExtensionContext context) {

        JdbcSetup.destroyDatasource(source);
        source = null;
    }
}
