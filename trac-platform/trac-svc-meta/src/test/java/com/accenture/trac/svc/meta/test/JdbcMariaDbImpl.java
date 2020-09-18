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

package com.accenture.trac.svc.meta.test;

import com.accenture.trac.common.util.InterfaceLogging;
import com.accenture.trac.common.db.JdbcDialect;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcMetadataDal;

import org.mariadb.jdbc.MariaDbDataSource;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfiguration;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;

import static com.accenture.trac.svc.meta.test.TestData.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Scanner;


public class JdbcMariaDbImpl implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

    private DBConfiguration dbConfig;
    private DB db = null;
    private JdbcMetadataDal dal;

    private Path tempDir;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        tempDir = Files.createTempDirectory("");

        var inputStream = JdbcMariaDbImpl.class.getResourceAsStream("/mysql/trac_metadata.ddl");
        var scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name()).useDelimiter("\\A");
        var deployScript = scanner.next();

        dbConfig = DBConfigurationBuilder.newBuilder()
                .setPort(0)
                .setDataDir(tempDir.toString())
                .build();

        db = DB.newEmbeddedDB(dbConfig);
        db.start();

        var source = new MariaDbDataSource();
        source.setServerName("localhost");
        source.setPort(dbConfig.getPort());

        try (var conn = source.getConnection(); var stmt = conn.createStatement()) {

            System.out.println("SQL >>> Deploying database schema");

            stmt.execute("create database trac_test");
            stmt.execute("use trac_test");

            for (var deployCommand : deployScript.split(";"))
                if (!deployCommand.isBlank()) {
                    System.out.println("SQL >>>\n\n" + deployCommand.strip() + "\n");
                    stmt.execute(deployCommand);
                }

            stmt.execute(String.format("insert into tenant (tenant_id, tenant_code) values (1, '%s')", TEST_TENANT));
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws SQLException {

        var testClass = context.getTestClass();

        if (testClass.isEmpty() || !IDalTestable.class.isAssignableFrom(testClass.get())) {
            Assertions.fail("JUnit extension for DAL testing requires the test class to implement IDalTestable");
        }

        var source = new MariaDbDataSource();
        source.setServerName("localhost");
        source.setPort(dbConfig.getPort());
        source.setDatabaseName("trac_test");

        var dal = new JdbcMetadataDal(JdbcDialect.MYSQL, source, Runnable::run);
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
    public void afterAll(ExtensionContext context) throws Exception {

        if (db != null)
            db.stop();

        Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach(p -> {

            try {
                Files.deleteIfExists(p);
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to clean up MySql data directory");
            }
        });
    }
}
