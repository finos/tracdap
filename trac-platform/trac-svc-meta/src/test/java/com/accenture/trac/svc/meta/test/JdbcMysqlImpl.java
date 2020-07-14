package com.accenture.trac.svc.meta.test;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfiguration;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.accenture.trac.common.util.InterfaceLogging;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcDialect;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcMetadataDal;
import com.mysql.cj.jdbc.MysqlDataSource;

import static com.accenture.trac.svc.meta.test.TestData.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Scanner;


public class JdbcMysqlImpl implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

    private DBConfiguration dbConfig;
    private DB db = null;
    private JdbcMetadataDal dal;

    private Path tempDir;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        tempDir = Files.createTempDirectory("");

        var inputStream = JdbcMysqlImpl.class.getResourceAsStream("/mysql/trac_metadata.ddl");
        var scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name()).useDelimiter("\\A");
        var deployScript = scanner.next();

        dbConfig = DBConfigurationBuilder.newBuilder()
                .setPort(0)
                .setDataDir(tempDir.toString())
                .build();

        db = DB.newEmbeddedDB(dbConfig);
        db.start();

        var source = new MysqlDataSource();
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
    public void beforeEach(ExtensionContext context) {

        var testClass = context.getTestClass();

        if (testClass.isEmpty() || !IDalTestable.class.isAssignableFrom(testClass.get())) {
            Assertions.fail("JUnit extension for DAL testing requires the test class to implement IDalTestable");
        }

        var source = new MysqlDataSource();
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
