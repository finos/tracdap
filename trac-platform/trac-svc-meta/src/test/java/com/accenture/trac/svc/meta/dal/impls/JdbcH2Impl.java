package com.accenture.trac.svc.meta.dal.impls;

import com.accenture.trac.svc.meta.dal.jdbc.JdbcDialect;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcMetadataDal;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.extension.*;
import com.accenture.trac.svc.meta.dal.MetadataDalTestBase;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.UUID;

import static com.accenture.trac.svc.meta.dal.MetadataDalTestData.TEST_TENANT;


public class JdbcH2Impl implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

    private JdbcDataSource source;
    private JdbcMetadataDal dal;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        var dbid = UUID.randomUUID();

        source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + dbid + ";DB_CLOSE_DELAY=-1");
        source.setUser("sa");
        source.setPassword("sa");

        var inputStream = JdbcH2Impl.class.getResourceAsStream("/h2/trac_metadata.ddl");
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

        var dal = new JdbcMetadataDal(JdbcDialect.H2, source, Runnable::run);
        dal.startup();

        this.dal = dal;

        var instance = context.getTestInstance();

        if (instance.isPresent()) {
            var testCase = (MetadataDalTestBase) instance.get();
            testCase.setDal(dal);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {

        if (dal != null)
            dal.shutdown();
    }

    @Override
    public void afterAll(ExtensionContext context) {

    }
}
