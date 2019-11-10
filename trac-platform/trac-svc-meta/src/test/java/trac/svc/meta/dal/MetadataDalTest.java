package trac.svc.meta.dal;

import trac.common.metadata.*;
import trac.svc.meta.exception.DuplicateItemError;
import trac.svc.meta.dal.jdbc.JdbcMetadataDal;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import ch.vorburger.mariadb4j.DBConfiguration;
import com.mysql.cj.jdbc.MysqlDataSource;


class MetadataDalTest {

    private static final String TEST_TENANT = "ACME_CORP";

    private static DBConfiguration dbConfig;
    private static DB db = null;

    private IMetadataDal dal;

    @BeforeAll
    static void setupDb(@TempDir Path tempDir) throws Exception {

        var inputStream = MetadataDalTest.class.getResourceAsStream("/mysql/trac_metadata.ddl");
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

    @AfterAll
    static void teardownDb() throws Exception {

        if (db != null)
            db.stop();
    }

    @BeforeEach
    void setupDal() {

        var source = new MysqlDataSource();
        source.setServerName("localhost");
        source.setPort(dbConfig.getPort());
        source.setDatabaseName("trac_test");

        var dal = new JdbcMetadataDal(source, Runnable::run);
        dal.startup();

        this.dal = dal;
    }

    private DataDefinition dummyDataDef() {

        return DataDefinition.newBuilder()
                .setHeader(ObjectHeader.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setId(MetadataCodec.encode(UUID.randomUUID()))
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
    }

    private ModelDefinition dummyModelDef() {

        return ModelDefinition.newBuilder()
                .setHeader(ObjectHeader.newBuilder()
                        .setObjectType(ObjectType.MODEL)
                        .setId(MetadataCodec.encode(UUID.randomUUID()))
                        .setVersion(1))
                .setLanguage("python")
                .setRepository("trac-test-repo")
                .setRepositoryVersion("trac-test-repo-1.2.3-RC4")
                .setPath("src/main/python")
                .setEntryPoint("trac_test.test1.SampleModel1")
                .putParams("param1", Parameter.newBuilder().setParamType(PrimitiveType.STRING).build())
                .putParams("param2", Parameter.newBuilder().setParamType(PrimitiveType.INTEGER).build())
                .putInputs("input1", TableDefinition.newBuilder()
                        .addFields(FieldDefinition.newBuilder()
                                .setFieldName("field1")
                                .setFieldType(PrimitiveType.DATE)
                                .build())
                        .addFields(FieldDefinition.newBuilder()
                                .setFieldName("field2")
                                .setBusinessKey(true)
                                .setFieldType(PrimitiveType.DECIMAL)
                                .setFieldLabel("A display name")
                                .setCategorical(true)
                                .setFormatCode("GBP")
                                .build())
                        .build())
                .putOutputs("output1", TableDefinition.newBuilder()
                        .addFields(FieldDefinition.newBuilder()
                                .setFieldName("checksum_field")
                                .setFieldType(PrimitiveType.DECIMAL)
                                .build())
                        .build())
                .build();
    }

    Tag dummyTag(DataDefinition dataDef) {

        return Tag.newBuilder()
                .setHeader(dataDef.getHeader())
                .setTagVersion(1)
                .setDataDefinition(dataDef)
                .putAttrs("dataset_key", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("widget_orders")
                        .build())
                .putAttrs("widget_type", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("non_standard_widget")
                        .build())
                .build();
    }

    Tag dummyTag(ModelDefinition modelDef) {

        return Tag.newBuilder()
                .setHeader(modelDef.getHeader())
                .setTagVersion(1)
                .setModelDefinition(modelDef)
                .putAttrs("dataset_key", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("widget_orders")
                        .build())
                .putAttrs("widget_type", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("non_standard_widget")
                        .build())
                .build();
    }

    Tag dummyTagForObjectType(ObjectType objectType) {

        if (objectType == ObjectType.DATA)
            return dummyTag(dummyDataDef());

        if (objectType == ObjectType.MODEL)
            return dummyTag(dummyModelDef());

        throw new RuntimeException("Object type not supported for test data: " + objectType.name());
    }

    @Test
    void roundTrip_oneObjectTypeOk() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, origId, 1, 1));

        assertEquals(origTag, future.get());
    }

    @Test
    void roundTrip_allObjectTypesOk() throws Exception {

        var typesToTest = new ObjectType[] {ObjectType.DATA, ObjectType.MODEL};

        for (var objectType: typesToTest) {

            var origTag = dummyTagForObjectType(objectType);
            var origId = MetadataCodec.decode(origTag.getHeader().getId());

            var future = CompletableFuture.completedFuture(0)
                    .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                    .thenCompose(x -> dal.loadTag(TEST_TENANT, origId, 1, 1));

            assertEquals(origTag, future.get());
        }
    }

    @Test
    void testSaveNewObject_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, origId, 1, 1));

        assertEquals(origTag, future.get());
    }

    @Test
    void testSaveNewObject_duplicate() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag));

        assertThrows(DuplicateItemError.class, future::get);
    }

    @Test
    void testSaveNewVersion_ok() {
        fail("Not implemented");
    }

    @Test
    void testSaveNewVersion_duplicate() {
        fail("Not implemented");
    }

    @Test
    void testSaveNewVersion_missingObject() {
        fail("Not implemented");
    }

    @Test
    void testSaveNewVersion_wrongObjectType() {
        fail("Not implemented");
    }

    @Test
    void testSaveNewTag_ok() {
        fail("Not implemented");
    }

    @Test
    void testSaveNewTag_duplicate() {
        fail("Not implemented");
    }

    @Test
    void testSaveNewTag_missingObject() {
        fail("Not implemented");
    }

    @Test
    void testSaveNewTag_missingVersion() {
        fail("Not implemented");
    }

    @Test
    void testPreallocate_ok() {
        fail("Not implemented");
    }

    @Test
    void testPreallocate_duplicate() {
        fail("Not implemented");
    }

    @Test
    void testPreallocate_missingObjectId() {
        fail("Not implemented");
    }

    @Test
    void testPreallocate_wrongObjectType() {
        fail("Not implemented");
    }

    @Test
    void testLoadOneExplicit_ok() {
        fail("Not implemented");
    }

    @Test
    void testLoadOneLatestVersion_ok() {
        fail("Not implemented");
    }

    @Test
    void testLoadOneLatestTag_ok() {
        fail("Not implemented");
    }

    @Test
    void testLoadOne_missingItems() {
        fail("Not implemented");
    }

    @Test
    void testLoadOne_wrongObjectType() {
        fail("Not implemented");
    }

    @Test
    void testLoadBatchExplicit_ok() {
        fail("Not implemented");
    }

    @Test
    void testLoadBatchLatestVersion_ok() {
        fail("Not implemented");
    }

    @Test
    void testLoadBatchLatestTag_ok() {
        fail("Not implemented");
    }

    @Test
    void testLoadBatch_missingItems() {
        fail("Not implemented");
    }

    @Test
    void testLoadBatch_wrongObjectType() {
        fail("Not implemented");
    }
}
