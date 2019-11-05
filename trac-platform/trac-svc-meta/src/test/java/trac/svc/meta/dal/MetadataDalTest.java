package trac.svc.meta.dal;

import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import trac.common.metadata.*;
import trac.svc.meta.dal.jdbc.JdbcMetadataDal;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mysql.cj.jdbc.MysqlDataSource;
import ch.vorburger.mariadb4j.junit.MariaDB4jRule;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import ch.vorburger.mariadb4j.DBConfiguration;


class MetadataDalTest {

    private IMetadataDal dal;

    private MariaDB4jRule databaseRule = new MariaDB4jRule(
            DBConfigurationBuilder.newBuilder().build(),
            "trac_test",
            "sql/mysql/trac_metadata.ddl");

    private DalSetupRule dalRule = new DalSetupRule(
            databaseRule,
            x -> this.dal = x);

    @Rule
    TestRule ordering = RuleChain
            .outerRule(databaseRule)
            .around(dalRule);

    static class DalSetupRule extends ExternalResource {

        Supplier<DBConfiguration> dbConfig;
        Consumer<IMetadataDal> setter;

        DalSetupRule(MariaDB4jRule databaseRule, Consumer<IMetadataDal> setter) {
            this.dbConfig = databaseRule::getDBConfiguration;
            this.setter = setter;
        }

        @Override
        protected void before() throws Throwable {

            MysqlDataSource source = new MysqlDataSource();
            source.setServerName("localhost");
            source.setPort(dbConfig.get().getPort());
            source.setDatabaseName("trac_test");

            Executor executor = Runnable::run;

            JdbcMetadataDal dal = new JdbcMetadataDal(source, executor);
            setter.accept(dal);
        }
    }

    @Test
    void testSaveNewObject_ok() throws Exception {

        DataDefinition origDef = DataDefinition.newBuilder()
                .setHeader(ObjectHeader.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setId("")
                        .setVersion(1))
                .addStorage("test-storage")
                .setPath("path/to/test/dataset")
                .setFormat(DataFormat.CSV)
                .setSchema(TableDefinition.newBuilder()
                        .addFields(FieldDefinition.newBuilder()
                                .setFieldName("transaction_id")
                                .setFieldType(PrimitiveType.STRING)
                                .setFieldOrder(1)
                                .setBusinessKey(true))
                        .addFields(FieldDefinition.newBuilder()
                                .setFieldName("customer_id")
                                .setFieldType(PrimitiveType.STRING)
                                .setFieldOrder(2)
                                .setBusinessKey(true))
                        .addFields(FieldDefinition.newBuilder()
                                .setFieldName("order_date")
                                .setFieldType(PrimitiveType.DATE)
                                .setFieldOrder(3)
                                .setBusinessKey(true))
                        .addFields(FieldDefinition.newBuilder()
                                .setFieldName("widgets_ordered")
                                .setFieldType(PrimitiveType.INTEGER)
                                .setFieldOrder(4)
                                .setBusinessKey(true)))
                .build();

        Tag origTag = Tag.newBuilder()
                .setDataDefinition(origDef)
                .setTagVersion(1)
                .putAttrs("dataset_key", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("widget_orders")
                        .build())
                .putAttrs("widget_class", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("non_standard_widget")
                        .build())
                .build();

        CompletableFuture<Tag> result = CompletableFuture.completedFuture(null)
                .thenCompose(x -> dal.saveNewObject("test_tenant", origTag))
                .thenCompose(x -> dal.loadTag("test_tenant", UUID.randomUUID(), 1, 1));

        assertEquals(origTag, result.get());
    }

    @Test
    void testSaveNewObject_duplicate() {

    }

    @Test
    void testSaveNewVersion_ok() {

    }

    @Test
    void testSaveNewVersion_duplicate() {

    }

    @Test
    void testSaveNewVersion_missingObject() {

    }

    @Test
    void testSaveNewVersion_wrongObjectType() {

    }

    @Test
    void testSaveNewTag_ok() {

    }

    @Test
    void testSaveNewTag_duplicate() {

    }

    @Test
    void testSaveNewTag_missingObject() {

    }

    @Test
    void testSaveNewTag_missingVersion() {

    }

    @Test
    void testPreallocate_ok() {

    }

    @Test
    void testPreallocate_duplicate() {

    }

    @Test
    void testPreallocate_missingObjectId() {

    }

    @Test
    void testPreallocate_wrongObjectType() {

    }

    @Test
    void testLoadOneExplicit_ok() {

    }

    @Test
    void testLoadOneLatestVersion_ok() {

    }

    @Test
    void testLoadOneLatestTag_ok() {

    }

    @Test
    void testLoadOne_missingItems() {

    }

    @Test
    void testLoadOne_wrongObjectType() {

    }

    @Test
    void testLoadBatchExplicit_ok() {

    }

    @Test
    void testLoadBatchLatestVersion_ok() {

    }

    @Test
    void testLoadBatchLatestTag_ok() {

    }

    @Test
    void testLoadBatch_missingItems() {

    }

    @Test
    void testLoadBatch_wrongObjectType() {

    }
}
