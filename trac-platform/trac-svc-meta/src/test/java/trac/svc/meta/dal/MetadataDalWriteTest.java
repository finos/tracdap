package trac.svc.meta.dal;

import org.junit.jupiter.api.extension.ExtendWith;
import trac.common.metadata.MetadataCodec;
import trac.common.metadata.ObjectType;
import trac.svc.meta.dal.impls.JdbcH2Impl;
import trac.svc.meta.dal.impls.JdbcMysqlImpl;
import trac.svc.meta.exception.DuplicateItemError;
import trac.svc.meta.exception.MissingItemError;
import trac.svc.meta.exception.WrongItemTypeError;

import static trac.svc.meta.dal.MetadataDalTestData.*;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.fail;


abstract class MetadataDalWriteTest extends MetadataDalTestBase {

    @ExtendWith(JdbcMysqlImpl.class)
    static class JdbcMysql extends MetadataDalWriteTest {}

    @ExtendWith(JdbcH2Impl.class)
    static class JdbcH2 extends MetadataDalWriteTest {}

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
}
