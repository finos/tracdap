package trac.svc.meta.dal;

import org.junit.jupiter.api.function.ThrowingSupplier;
import trac.common.metadata.*;
import trac.svc.meta.dal.jdbc.JdbcDialect;
import trac.svc.meta.exception.DuplicateItemError;
import trac.svc.meta.dal.jdbc.JdbcMetadataDal;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
import trac.svc.meta.exception.MissingItemError;
import trac.svc.meta.exception.WrongItemTypeError;


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

        var dal = new JdbcMetadataDal(JdbcDialect.MYSQL, source, Runnable::run);
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
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("transaction_id")
                                .setFieldType(PrimitiveType.STRING)
                                .setFieldOrder(1)
                                .setBusinessKey(true))
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("customer_id")
                                .setFieldType(PrimitiveType.STRING)
                                .setFieldOrder(2)
                                .setBusinessKey(true))
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("order_date")
                                .setFieldType(PrimitiveType.DATE)
                                .setFieldOrder(3)
                                .setBusinessKey(true))
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("widgets_ordered")
                                .setFieldType(PrimitiveType.INTEGER)
                                .setFieldOrder(4)
                                .setBusinessKey(true)))
                .build();
    }

    private DataDefinition nextDataDef(DataDefinition origDef) {

        return origDef.toBuilder()
                .mergeHeader(origDef.getHeader()
                        .toBuilder()
                        .setVersion(origDef.getHeader().getVersion() + 1)
                        .build())
                .mergeSchema(origDef.getSchema().toBuilder()
                        .addField(FieldDefinition.newBuilder()
                        .setFieldName("extra_field")
                        .setFieldOrder(origDef.getSchema().getFieldCount())
                        .setFieldType(PrimitiveType.FLOAT)
                        .setFieldLabel("We got an extra field!")
                        .setFormatCode("PERCENT")
                        .build()).build())
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
                .putParam("param1", Parameter.newBuilder().setParamType(PrimitiveType.STRING).build())
                .putParam("param2", Parameter.newBuilder().setParamType(PrimitiveType.INTEGER).build())
                .putInput("input1", TableDefinition.newBuilder()
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("field1")
                                .setFieldType(PrimitiveType.DATE)
                                .build())
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("field2")
                                .setBusinessKey(true)
                                .setFieldType(PrimitiveType.DECIMAL)
                                .setFieldLabel("A display name")
                                .setCategorical(true)
                                .setFormatCode("GBP")
                                .build())
                        .build())
                .putOutput("output1", TableDefinition.newBuilder()
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("checksum_field")
                                .setFieldType(PrimitiveType.DECIMAL)
                                .build())
                        .build())
                .build();
    }

    private ModelDefinition nextModelDef(ModelDefinition origDef) {

        return origDef.toBuilder()
                .mergeHeader(origDef.getHeader()
                        .toBuilder()
                        .setVersion(origDef.getHeader().getVersion() + 1)
                        .build())
                .putParam("param3", Parameter.newBuilder().setParamType(PrimitiveType.DATE).build())
                .build();
    }

    Tag dummyTag(DataDefinition dataDef) {

        return Tag.newBuilder()
                .setHeader(dataDef.getHeader())
                .setTagVersion(1)
                .setDataDefinition(dataDef)
                .putAttr("dataset_key", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("widget_orders")
                        .build())
                .putAttr("widget_type", PrimitiveValue.newBuilder()
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
                .putAttr("dataset_key", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("widget_orders")
                        .build())
                .putAttr("widget_type", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("non_standard_widget")
                        .build())
                .build();
    }

    Tag nextTag(Tag previous) {

        return previous.toBuilder()
                .setTagVersion(previous.getTagVersion() + 1)
                .putAttr("extra_attr", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("A new descriptive value")
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

    private <T> T unwrap(CompletableFuture<T> future) throws Exception {

        try {
            return future.get();
        }
        catch (ExecutionException e) {
            var cause = e.getCause();
            if (cause instanceof Exception)
                throw (Exception) cause;
            throw e;
        }
    }

    @Test
    void roundTrip_oneObjectTypeOk() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        assertEquals(origTag, unwrap(future));
    }

    @Test
    void roundTrip_allObjectTypesOk() throws Exception {

        var typesToTest = new ObjectType[] {ObjectType.DATA, ObjectType.MODEL};

        for (var objectType: typesToTest) {

            var origTag = dummyTagForObjectType(objectType);
            var origId = MetadataCodec.decode(origTag.getHeader().getId());

            var future = CompletableFuture.completedFuture(0)
                    .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                    .thenCompose(x -> dal.loadTag(TEST_TENANT, objectType, origId, 1, 1));

            assertEquals(origTag, unwrap(future));
        }
    }

    @Test
    void testSaveNewObject_ok() throws Exception {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        assertEquals(origTag, unwrap(future));

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var future2 = dal.saveNewObjects(TEST_TENANT, Arrays.asList(multi1, multi2));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getHeader().getId()), 1, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getHeader().getId()), 1, 1);

        assertEquals(multi1, unwrap(result1));
        assertEquals(multi2, unwrap(result2));
    }

    @Test
    void testSaveNewObject_duplicate() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        var saveDup =  dal.saveNewObjects(TEST_TENANT, Arrays.asList(origTag, origTag));
        var loadDup = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1);

        assertThrows(DuplicateItemError.class, () -> unwrap(saveDup));
        assertThrows(MissingItemError.class, () -> unwrap(loadDup));

        var saveDup2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag));

        var loadDup2 = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1);

        // First insert should succeed if they are run one by one
        assertThrows(DuplicateItemError.class, () -> unwrap(saveDup2));
        assertDoesNotThrow(() -> unwrap(loadDup2));
    }

    @Test
    void testSaveNewVersion_ok() throws Exception {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        var nextDef = nextDataDef(origDef);
        var nextTag = dummyTag(nextDef);

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1));

        assertEquals(nextTag, unwrap(future));

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = dummyTag(nextDataDef(multi1.getDataDefinition()));
        var multi2v2 = dummyTag(nextModelDef(multi2.getModelDefinition()));

        var future2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObjects(TEST_TENANT, Arrays.asList(multi1, multi2)))
                .thenCompose(x -> dal.saveNewVersions(TEST_TENANT, Arrays.asList(multi1v2, multi2v2)));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getHeader().getId()), 2, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getHeader().getId()), 2, 1);

        assertEquals(multi1v2, unwrap(result1));
        assertEquals(multi2v2, unwrap(result2));
    }

    @Test
    void testSaveNewVersion_duplicate() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        var nextDef = nextDataDef(origDef);
        var nextTag = dummyTag(nextDef);

        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveDup =  dal.saveNewVersions(TEST_TENANT, Arrays.asList(nextTag, nextTag));
        var loadDup = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1);

        unwrap(saveOrig);
        assertThrows(DuplicateItemError.class, () -> unwrap(saveDup));
        assertThrows(MissingItemError.class, () -> unwrap(loadDup));

        var saveDup2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextTag));

        var loadDup2 = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1);

        // First insert should succeed if they are run one by one
        assertThrows(DuplicateItemError.class, () -> unwrap(saveDup2));
        assertEquals(nextTag, unwrap(loadDup2));
    }

    @Test
    void testSaveNewVersion_missingObject() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var nextDef = nextDataDef(origDef);
        var nextTag = dummyTag(nextDef);

        // Save next version, single, without saving original
        var saveNext =  dal.saveNewVersion(TEST_TENANT, nextTag);
        assertThrows(MissingItemError.class, () -> unwrap(saveNext));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = dummyTag(nextModelDef(modelTag.getModelDefinition()));

        // Save next, multiple, one item does not have original
        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveNextMulti = dal.saveNewVersions(TEST_TENANT, Arrays.asList(nextTag, nextModelTag));

        unwrap(saveOrig);
        assertThrows(MissingItemError.class, () -> unwrap(saveNextMulti));
    }

    @Test
    void testSaveNewVersion_wrongObjectType() throws Exception {

        var dataTag = dummyTagForObjectType(ObjectType.DATA);
        var nextDataTag = dummyTag(nextDataDef(dataTag.getDataDefinition()));

        // Create a model def with the same ID as the data def
        var modelDef = dummyModelDef().toBuilder()
                .setHeader(dataTag.getHeader().toBuilder()
                .setObjectType(ObjectType.MODEL).build())
                .build();

        var modelTag = dummyTag(modelDef);
        var nextModelTag = dummyTag(nextModelDef(modelTag.getModelDefinition()));

        // Save next version, single, without saving original
        var saveOrig = dal.saveNewObject(TEST_TENANT, dataTag);
        var saveNext =  dal.saveNewVersion(TEST_TENANT, nextModelTag);
        var saveNextMulti =  dal.saveNewVersions(TEST_TENANT, Arrays.asList(nextDataTag, nextModelTag));

        unwrap(saveOrig);
        assertThrows(WrongItemTypeError.class, () -> unwrap(saveNext));
        assertThrows(WrongItemTypeError.class, () -> unwrap(saveNextMulti));
    }

    @Test
    void testSaveNewTag_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var nextDefTag1 = dummyTag(nextDataDef(origDef));
        var nextDefTag2 = nextTag(nextDefTag1);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        // Test saving tag v2 against object v2
        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextDefTag1))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextDefTag2))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 2));

        assertEquals(nextDefTag2, unwrap(future));

        // Save multiple - this test saves tag v2 against object v1
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = nextTag(multi1);
        var multi2v2 = nextTag(multi2);

        var future2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObjects(TEST_TENANT, Arrays.asList(multi1, multi2)))
                .thenCompose(x -> dal.saveNewTags(TEST_TENANT, Arrays.asList(multi1v2, multi2v2)));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getHeader().getId()), 1, 2);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getHeader().getId()), 1, 2);

        assertEquals(multi1v2, unwrap(result1));
        assertEquals(multi2v2, unwrap(result2));
    }

    @Test
    void testSaveNewTag_duplicate() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var nextTag = nextTag(origTag);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveDup =  dal.saveNewTags(TEST_TENANT, Arrays.asList(nextTag, nextTag));
        var loadDup = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2);

        unwrap(saveOrig);
        assertThrows(DuplicateItemError.class, () -> unwrap(saveDup));
        assertThrows(MissingItemError.class, () -> unwrap(loadDup));

        var saveDup2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextTag))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextTag));

        var loadDup2 = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2);

        // First insert should succeed if they are run one by one
        assertThrows(DuplicateItemError.class, () -> unwrap(saveDup2));
        assertEquals(nextTag, unwrap(loadDup2));
    }

    @Test
    void testSaveNewTag_missingObject() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var nextTag = nextTag(origTag);

        // Save next tag, single, without saving object
        var saveNext =  dal.saveNewTag(TEST_TENANT, nextTag);
        assertThrows(MissingItemError.class, () -> unwrap(saveNext));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = dummyTag(nextModelDef(modelTag.getModelDefinition()));

        // Save next tag, multiple, one item does not have an object
        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveNextMulti = dal.saveNewTags(TEST_TENANT, Arrays.asList(nextTag, nextModelTag));

        unwrap(saveOrig);
        assertThrows(MissingItemError.class, () -> unwrap(saveNextMulti));
    }

    @Test
    void testSaveNewTag_missingVersion() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var nextDef = nextDataDef(origDef);
        var nextDefTag1 = dummyTag(nextDef);
        var nextDefTag2 = nextTag(nextDefTag1);

        // Save next tag (single) on an unknown object version
        // This is an error, even for tag v1
        // saveNewVersion must be called first
        var saveObj = dal.saveNewObject(TEST_TENANT, origTag);
        var saveNext1 =  dal.saveNewTag(TEST_TENANT, nextDefTag1);
        var saveNext2 =  dal.saveNewTag(TEST_TENANT, nextDefTag2);

        unwrap(saveObj);
        assertThrows(MissingItemError.class, () -> unwrap(saveNext1));
        assertThrows(MissingItemError.class, () -> unwrap(saveNext2));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = dummyTag(nextModelDef(modelTag.getModelDefinition()));
        var nextModelTag2 = nextTag(nextModelTag);

        // Save object 1 version 2, and object 2 version 1
        unwrap(dal.saveNewVersion(TEST_TENANT, nextDefTag1));
        unwrap(dal.saveNewObject(TEST_TENANT, modelTag));

        // Save next tag (multiple), second item is missing the required object version
        var saveNextMulti = dal.saveNewTags(TEST_TENANT, Arrays.asList(nextDefTag2, nextModelTag2));
        assertThrows(MissingItemError.class, () -> unwrap(saveNextMulti));

        // Saving the valid tag by itself should not throw
        assertDoesNotThrow(() -> dal.saveNewTag(TEST_TENANT, nextDefTag2));
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
    void testLoadOneExplicit_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var nextDefTag1 = dummyTag(nextDataDef(origDef));
        var nextDefTag2 = nextTag(nextDefTag1);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        // Save v1 t1, v2 t1, v2 t2
        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextDefTag1))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextDefTag2));

        unwrap(future);

        // Load all three items by explicit version / tag number
        var v1t1 = unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));
        var v2t1 = unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1));
        var v2t2 = unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 2));

        assertEquals(origTag, v1t1);
        assertEquals(nextDefTag1, v2t1);
        assertEquals(nextDefTag2, v2t2);
    }

    @Test
    void testLoadOneLatestVersion_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var nextDefTag1 = dummyTag(nextDataDef(origDef));
        var nextDefTag2 = nextTag(nextDefTag1);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        // After save v1t1, latest version = v1t1
        var v1t1 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadLatestVersion(TEST_TENANT, ObjectType.DATA, origId));

        assertEquals(origTag, unwrap(v1t1));

        // After save v2t1, latest version = v2t1
        var v2t1 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextDefTag1))
                .thenCompose(x -> dal.loadLatestVersion(TEST_TENANT, ObjectType.DATA, origId));

        assertEquals(nextDefTag1, unwrap(v2t1));

        // After save v2t2, latest version = v2t2
        var v2t2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextDefTag2))
                .thenCompose(x -> dal.loadLatestVersion(TEST_TENANT, ObjectType.DATA, origId));

        assertEquals(nextDefTag2, unwrap(v2t2));
    }

    @Test
    void testLoadOneLatestTag_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        var nextDefTag1 = dummyTag(nextDataDef(origDef));

        // Save v1 t1, v2 t1
        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextDefTag1));

        unwrap(future);

        // Load latest tag for object versions 1 & 2
        var v1 = unwrap(dal.loadLatestTag(TEST_TENANT, ObjectType.DATA, origId, 1));
        var v2 = unwrap(dal.loadLatestTag(TEST_TENANT, ObjectType.DATA, origId, 2));

        // Should get v1 = v1t1, v2 = v2t1
        assertEquals(origTag, v1);
        assertEquals(nextDefTag1, v2);

        // Save a new tag for object version 1
        var origDefTag2 = nextTag(origTag);

        var v1t2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, origDefTag2))
                .thenCompose(x -> dal.loadLatestTag(TEST_TENANT, ObjectType.DATA, origId, 1));

        assertEquals(origDefTag2, unwrap(v1t2));
    }

    @Test
    void testLoadOne_missingItems() throws Exception {

        assertThrows(MissingItemError.class, () -> unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, UUID.randomUUID(), 1, 1)));
        assertThrows(MissingItemError.class, () -> unwrap(dal.loadLatestTag(TEST_TENANT, ObjectType.DATA, UUID.randomUUID(), 1)));
        assertThrows(MissingItemError.class, () -> unwrap(dal.loadLatestVersion(TEST_TENANT, ObjectType.DATA, UUID.randomUUID())));

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        // Save an item
        var future = dal.saveNewObject(TEST_TENANT, origTag);
        unwrap(future);

        assertThrows(MissingItemError.class, () -> unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2)));  // Missing tag
        assertThrows(MissingItemError.class, () -> unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1)));  // Missing ver
        assertThrows(MissingItemError.class, () -> unwrap(dal.loadLatestTag(TEST_TENANT, ObjectType.DATA, origId, 2)));  // Missing ver
    }

    @Test
    void testLoadOne_wrongObjectType() {
        fail("Not implemented");
    }

    @Test
    void testLoadBatchExplicit_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var nextDefTag1 = dummyTag(nextDataDef(origDef));
        var nextDefTag2 = nextTag(nextDefTag1);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        var modelDef = dummyModelDef();
        var modelTag = dummyTag(modelDef);
        var modelId = MetadataCodec.decode(modelDef.getHeader().getId());

        // Save everything first
        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextDefTag1))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextDefTag2))
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, modelTag));

        unwrap(future);

        var types = Arrays.asList(ObjectType.DATA, ObjectType.DATA, ObjectType.DATA, ObjectType.MODEL);
        var ids = Arrays.asList(origId, origId, origId, modelId);
        var versions = Arrays.asList(1, 2, 2, 1);
        var tagVersions = Arrays.asList(1, 1, 2, 1);

        var result = unwrap(dal.loadTags(TEST_TENANT, types, ids, versions, tagVersions));

        assertEquals(origTag, result.get(0));
        assertEquals(nextDefTag1, result.get(1));
        assertEquals(nextDefTag2, result.get(2));
        assertEquals(modelTag, result.get(3));
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
