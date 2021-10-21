/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.test.meta;

import com.accenture.trac.common.db.JdbcSetup;
import com.accenture.trac.common.util.InterfaceLogging;
import com.accenture.trac.common.db.JdbcDialect;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcMetadataDal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.*;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

import static com.accenture.trac.test.meta.TestData.TEST_TENANT;


public class JdbcUnit implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

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
        props.setProperty("unit.pool.size", "1");

        source = JdbcSetup.createDatasource(props, "unit");

        var inputStream = JdbcUnit.class.getResourceAsStream("/h2/001__trac_metadata.ddl");
        var scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name()).useDelimiter("\\A");
        var deployScript = scanner.next();

        try (var conn = source.getConnection(); var stmt = conn.createStatement()) {

            System.out.println("SQL >>> Deploying database schema");

            for (var deployCommand : deployScript.split(";"))
                if (!deployCommand.isBlank()) {
                    System.out.println("SQL >>>\n\n" + deployCommand.strip() + "\n");
                    stmt.execute(deployCommand);
                }

            stmt.execute(String.format("insert into tenant (tenant_id, tenant_code) values (1, '%s')", TEST_TENANT));
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
