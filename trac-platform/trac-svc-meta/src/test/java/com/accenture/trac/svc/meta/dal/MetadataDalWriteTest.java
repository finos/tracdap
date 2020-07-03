package com.accenture.trac.svc.meta.dal;

import com.accenture.trac.svc.meta.exception.DuplicateItemError;
import com.accenture.trac.svc.meta.exception.MissingItemError;
import com.accenture.trac.svc.meta.exception.WrongItemTypeError;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.ObjectType;
import static com.accenture.trac.svc.meta.test.TestData.*;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcH2Impl;
import com.accenture.trac.svc.meta.test.JdbcMysqlImpl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.junit.jupiter.api.Assertions.*;


abstract class MetadataDalWriteTest implements IDalTestable {

    private IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }

    @ExtendWith(JdbcH2Impl.class)
    static class JdbcH2 extends MetadataDalWriteTest {}

    @Tag("integration")
    @Tag("int-mysql")
    @ExtendWith(JdbcMysqlImpl.class)
    static class JdbcMysql extends MetadataDalWriteTest {}

    @Test
    void testSaveNewObject_ok() throws Exception {

        // Save one
        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        assertEquals(origTag, unwrap(future));

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var future2 = dal.saveNewObjects(TEST_TENANT, Arrays.asList(multi1, multi2));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getDefinition().getHeader().getObjectId()), 1, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getDefinition().getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, unwrap(result1));
        assertEquals(multi2, unwrap(result2));
    }

    @Test
    void testSaveNewObject_duplicate() {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

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
        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef, UPDATE_HEADER);
        var nextTag = dummyTag(nextDef);

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1));

        assertEquals(nextTag, unwrap(future));

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = dummyTag(nextDataDef(multi1.getDefinition(), UPDATE_HEADER));
        var multi2v2 = dummyTag(nextModelDef(multi2.getDefinition(), UPDATE_HEADER));

        var future2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObjects(TEST_TENANT, Arrays.asList(multi1, multi2)))
                .thenCompose(x -> dal.saveNewVersions(TEST_TENANT, Arrays.asList(multi1v2, multi2v2)));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getDefinition().getHeader().getObjectId()), 2, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getDefinition().getHeader().getObjectId()), 2, 1);

        assertEquals(multi1v2, unwrap(result1));
        assertEquals(multi2v2, unwrap(result2));
    }

    @Test
    void testSaveNewVersion_duplicate() throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef, UPDATE_HEADER);
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

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var nextDef = nextDataDef(origDef, UPDATE_HEADER);
        var nextTag = dummyTag(nextDef);

        // Save next version, single, without saving original
        var saveNext =  dal.saveNewVersion(TEST_TENANT, nextTag);
        assertThrows(MissingItemError.class, () -> unwrap(saveNext));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = dummyTag(nextModelDef(modelTag.getDefinition(), UPDATE_HEADER));

        // Save next, multiple, one item does not have original
        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveNextMulti = dal.saveNewVersions(TEST_TENANT, Arrays.asList(nextTag, nextModelTag));

        unwrap(saveOrig);
        assertThrows(MissingItemError.class, () -> unwrap(saveNextMulti));
    }

    @Test
    void testSaveNewVersion_wrongObjectType() throws Exception {

        var dataTag = dummyTagForObjectType(ObjectType.DATA);
        var nextDataTag = dummyTag(nextDataDef(dataTag.getDefinition(), UPDATE_HEADER));

        // Create a model def with the same ID as the data def
        var modelDef = dummyModelDef(INCLUDE_HEADER).toBuilder()
                .setHeader(dataTag.getDefinition().getHeader().toBuilder()
                        .setObjectType(ObjectType.MODEL).build())
                .build();

        var modelTag = dummyTag(modelDef);
        var nextModelTag = dummyTag(nextModelDef(modelTag.getDefinition(), UPDATE_HEADER));

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

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var nextDefTag1 = dummyTag(nextDataDef(origDef, UPDATE_HEADER));
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

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

        var multi1v2 = nextTag(multi1, UPDATE_TAG_VERSION);
        var multi2v2 = nextTag(multi2, UPDATE_TAG_VERSION);

        var future2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObjects(TEST_TENANT, Arrays.asList(multi1, multi2)))
                .thenCompose(x -> dal.saveNewTags(TEST_TENANT, Arrays.asList(multi1v2, multi2v2)));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getDefinition().getHeader().getObjectId()), 1, 2);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getDefinition().getHeader().getObjectId()), 1, 2);

        assertEquals(multi1v2, unwrap(result1));
        assertEquals(multi2v2, unwrap(result2));
    }

    @Test
    void testSaveNewTag_duplicate() throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

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

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION);

        // Save next tag, single, without saving object
        var saveNext =  dal.saveNewTag(TEST_TENANT, nextTag);
        assertThrows(MissingItemError.class, () -> unwrap(saveNext));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = dummyTag(nextModelDef(modelTag.getDefinition(), UPDATE_HEADER));

        // Save next tag, multiple, one item does not have an object
        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveNextMulti = dal.saveNewTags(TEST_TENANT, Arrays.asList(nextTag, nextModelTag));

        unwrap(saveOrig);
        assertThrows(MissingItemError.class, () -> unwrap(saveNextMulti));
    }

    @Test
    void testSaveNewTag_missingVersion() throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var nextDef = nextDataDef(origDef, UPDATE_HEADER);
        var nextDefTag1 = dummyTag(nextDef);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);

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
        var nextModelTag = dummyTag(nextModelDef(modelTag.getDefinition(), UPDATE_HEADER));
        var nextModelTag2 = nextTag(nextModelTag, UPDATE_TAG_VERSION);

        // Save object 1 version 2, and object 2 version 1
        unwrap(dal.saveNewVersion(TEST_TENANT, nextDefTag1));
        unwrap(dal.saveNewObject(TEST_TENANT, modelTag));

        // Save next tag (multiple), second item is missing the required object version
        var saveNextMulti = dal.saveNewTags(TEST_TENANT, Arrays.asList(nextDefTag2, nextModelTag2));
        assertThrows(MissingItemError.class, () -> unwrap(saveNextMulti));

        // Saving the valid tag by itself should not throw
        Assertions.assertDoesNotThrow(() -> dal.saveNewTag(TEST_TENANT, nextDefTag2));
    }

    @Test
    void testSaveNewTag_wrongObjectType() throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var nextHeader = origDef
                .getHeader()
                .toBuilder()
                .setObjectType(ObjectType.MODEL);

        var nextDef = origDef
                .toBuilder()
                .setHeader(nextHeader);

        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION)
                .toBuilder()
                .setDefinition(nextDef)
                .build();

        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveTag =  dal.saveNewTag(TEST_TENANT, nextTag);
        var loadWrongType = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2);

        unwrap(saveOrig);
        assertThrows(WrongItemTypeError.class, () -> unwrap(saveTag));
        assertThrows(MissingItemError.class, () -> unwrap(loadWrongType));

        var origDef2 = dummyModelDef(INCLUDE_HEADER);
        var origTag2 = dummyTag(origDef2);
        var origId2 = MetadataCodec.decode(origDef2.getHeader().getObjectId());

        var nextTag2 = nextTag(origTag2, UPDATE_TAG_VERSION);

        var saveOrig2 = dal.saveNewObject(TEST_TENANT, origTag2);
        var saveTag2 = dal.saveNewTags(TEST_TENANT, Arrays.asList(nextTag, nextTag2));
        var loadWrongType2 = dal.loadTags(TEST_TENANT,
                Arrays.asList(ObjectType.DATA, ObjectType.MODEL),
                Arrays.asList(origId, origId2),
                Arrays.asList(1, 1),
                Arrays.asList(2, 2));

        unwrap(saveOrig2);
        assertThrows(WrongItemTypeError.class, () -> unwrap(saveTag2));
        assertThrows(MissingItemError.class, () -> unwrap(loadWrongType2));
    }

    @Test
    void testPreallocate_ok() throws Exception {

        // Save one
        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, origId))
                .thenCompose(x -> dal.savePreallocatedObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        assertEquals(origTag, unwrap(future));

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.MODEL);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);
        var id1 = MetadataCodec.decode(multi1.getDefinition().getHeader().getObjectId());
        var id2 = MetadataCodec.decode(multi2.getDefinition().getHeader().getObjectId());

        var future2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.preallocateObjectIds(TEST_TENANT,
                        Arrays.asList(ObjectType.MODEL, ObjectType.MODEL),
                        Arrays.asList(id1, id2)))
                .thenCompose(x -> dal.savePreallocatedObjects(TEST_TENANT, Arrays.asList(multi1, multi2)));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi1.getDefinition().getHeader().getObjectId()), 1, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getDefinition().getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, unwrap(result1));
        assertEquals(multi2, unwrap(result2));
    }

    @Test
    void testPreallocate_duplicate() throws Exception {

        var id1 = UUID.randomUUID();

        unwrap(dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id1));
        assertThrows(DuplicateItemError.class,
                () -> unwrap(dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id1)));

        var obj2 = dummyTagForObjectType(ObjectType.MODEL);
        var id2 = MetadataCodec.decode(obj2.getDefinition().getHeader().getObjectId());
        var id3 = UUID.randomUUID();

        unwrap(dal.saveNewObject(TEST_TENANT, obj2));
        assertThrows(DuplicateItemError.class,
                () -> unwrap(dal.preallocateObjectIds(TEST_TENANT,
                        Arrays.asList(ObjectType.MODEL, ObjectType.MODEL),
                        Arrays.asList(id2, id3))));
    }

    @Test
    void testPreallocate_missingObjectId() throws Exception {

        var obj1 = dummyTagForObjectType(ObjectType.MODEL);

        assertThrows(MissingItemError.class,
                () -> unwrap(dal.savePreallocatedObject(TEST_TENANT, obj1)));

        var obj2 = dummyTagForObjectType(ObjectType.DATA);
        var id2 = MetadataCodec.decode(obj2.getDefinition().getHeader().getObjectId());

        unwrap(dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id2));

        assertThrows(MissingItemError.class,
                () -> unwrap(dal.savePreallocatedObjects(TEST_TENANT, Arrays.asList(obj1, obj2))));
    }

    @Test
    void testPreallocate_wrongObjectType() throws Exception {

        var obj1 = dummyTagForObjectType(ObjectType.MODEL);
        var id1 = MetadataCodec.decode(obj1.getDefinition().getHeader().getObjectId());

        unwrap(dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id1));

        assertThrows(WrongItemTypeError.class,
                () -> unwrap(dal.savePreallocatedObject(TEST_TENANT, obj1)));

        var obj2 = dummyTagForObjectType(ObjectType.DATA);
        var id2 = MetadataCodec.decode(obj2.getDefinition().getHeader().getObjectId());

        unwrap(dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id2));

        assertThrows(WrongItemTypeError.class,
                () -> unwrap(dal.savePreallocatedObjects(TEST_TENANT, Arrays.asList(obj1, obj2))));
    }
}
