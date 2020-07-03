package com.accenture.trac.svc.meta.dal;

import com.accenture.trac.svc.meta.exception.MissingItemError;
import com.accenture.trac.svc.meta.exception.WrongItemTypeError;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.ObjectType;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcH2Impl;
import com.accenture.trac.svc.meta.test.JdbcMysqlImpl;
import static com.accenture.trac.svc.meta.test.TestData.*;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.junit.jupiter.api.Assertions.*;


abstract class MetadataDalReadTest implements IDalTestable {

    private IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }

    @ExtendWith(JdbcH2Impl.class)
    static class JdbcH2 extends MetadataDalReadTest {}

    @Tag("integration")
    @Tag("int-mysql")
    @ExtendWith(JdbcMysqlImpl.class)
    static class JdbcMysql extends MetadataDalReadTest {}

    @Test
    void testLoadOneExplicit_ok() throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var nextDefTag1 = dummyTag(nextDataDef(origDef, UPDATE_HEADER));
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

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

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var nextDefTag1 = dummyTag(nextDataDef(origDef, UPDATE_HEADER));
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

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

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var nextDefTag1 = dummyTag(nextDataDef(origDef, UPDATE_HEADER));

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
        var origDefTag2 = nextTag(origTag, UPDATE_TAG_VERSION);

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

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        // Save an item
        var future = dal.saveNewObject(TEST_TENANT, origTag);
        unwrap(future);

        assertThrows(MissingItemError.class, () -> unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2)));  // Missing tag
        assertThrows(MissingItemError.class, () -> unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1)));  // Missing ver
        assertThrows(MissingItemError.class, () -> unwrap(dal.loadLatestTag(TEST_TENANT, ObjectType.DATA, origId, 2)));  // Missing ver
    }

    @Test
    void testLoadOne_wrongObjectType() throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        unwrap(dal.saveNewObject(TEST_TENANT, origTag));

        assertThrows(WrongItemTypeError.class, () -> unwrap(dal.loadTag(TEST_TENANT, ObjectType.MODEL, origId, 1, 1)));
        assertThrows(WrongItemTypeError.class, () -> unwrap(dal.loadLatestTag(TEST_TENANT, ObjectType.MODEL, origId, 1)));
        assertThrows(WrongItemTypeError.class, () -> unwrap(dal.loadLatestVersion(TEST_TENANT, ObjectType.MODEL, origId)));
    }

    @Test
    void testLoadBatchExplicit_ok() throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var nextDefTag1 = dummyTag(nextDataDef(origDef, UPDATE_HEADER));
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var modelDef = dummyModelDef(INCLUDE_HEADER);
        var modelTag = dummyTag(modelDef);
        var modelId = MetadataCodec.decode(modelDef.getHeader().getObjectId());

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
    void testLoadBatchLatestVersion_ok() throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var nextDefTag1 = dummyTag(nextDataDef(origDef, UPDATE_HEADER));
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var modelDef = dummyModelDef(INCLUDE_HEADER);
        var modelTag = dummyTag(modelDef);
        var modelId = MetadataCodec.decode(modelDef.getHeader().getObjectId());

        // Save everything first
        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextDefTag1))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextDefTag2))
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, modelTag));

        unwrap(future);

        var types = Arrays.asList(ObjectType.DATA, ObjectType.MODEL);
        var ids = Arrays.asList(origId, modelId);

        var result = unwrap(dal.loadLatestVersions(TEST_TENANT, types, ids));

        assertEquals(nextDefTag2, result.get(0));
        assertEquals(modelTag, result.get(1));
    }

    @Test
    void testLoadBatchLatestTag_ok() throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var nextDefTag1 = dummyTag(nextDataDef(origDef, UPDATE_HEADER));
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var modelDef = dummyModelDef(INCLUDE_HEADER);
        var modelTag = dummyTag(modelDef);
        var modelId = MetadataCodec.decode(modelDef.getHeader().getObjectId());

        // Save everything first
        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextDefTag1))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextDefTag2))
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, modelTag));

        unwrap(future);

        var types = Arrays.asList(ObjectType.DATA, ObjectType.DATA, ObjectType.MODEL);
        var ids = Arrays.asList(origId, origId, modelId);
        var versions = Arrays.asList(1, 2, 1);

        var result = unwrap(dal.loadLatestTags(TEST_TENANT, types, ids, versions));

        assertEquals(origTag, result.get(0));
        assertEquals(nextDefTag2, result.get(1));
        assertEquals(modelTag, result.get(2));
    }

    @Test
    void testLoadBatch_missingItems() throws Exception {

        var loadTags = dal.loadTags(TEST_TENANT,
                Collections.singletonList(ObjectType.DATA),
                Collections.singletonList(UUID.randomUUID()),
                Collections.singletonList(1),
                Collections.singletonList(1));

        var loadLatestTags = dal.loadLatestTags(TEST_TENANT,
                Collections.singletonList(ObjectType.DATA),
                Collections.singletonList(UUID.randomUUID()),
                Collections.singletonList(1));

        var loadLatestVersions = dal.loadLatestVersions(TEST_TENANT,
                Collections.singletonList(ObjectType.DATA),
                Collections.singletonList(UUID.randomUUID()));

        assertThrows(MissingItemError.class, () -> unwrap(loadTags));
        assertThrows(MissingItemError.class, () -> unwrap(loadLatestTags));
        assertThrows(MissingItemError.class, () -> unwrap(loadLatestVersions));

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        // Save an item
        var future = dal.saveNewObject(TEST_TENANT, origTag);
        unwrap(future);

        var loadTags2 = dal.loadTags(TEST_TENANT,
                Collections.singletonList(ObjectType.DATA),
                Collections.singletonList(origId),
                Collections.singletonList(1),
                Collections.singletonList(2));

        var loadTags3 = dal.loadTags(TEST_TENANT,
                Collections.singletonList(ObjectType.DATA),
                Collections.singletonList(UUID.randomUUID()),
                Collections.singletonList(2),
                Collections.singletonList(1));

        var loadLatestTag2 = dal.loadLatestTags(TEST_TENANT,
                Collections.singletonList(ObjectType.DATA),
                Collections.singletonList(UUID.randomUUID()),
                Collections.singletonList(2));

        assertThrows(MissingItemError.class, () -> unwrap(loadTags2));  // Missing tag
        assertThrows(MissingItemError.class, () -> unwrap(loadTags3));  // Missing ver
        assertThrows(MissingItemError.class, () -> unwrap(loadLatestTag2));  // Missing ver
    }

    @Test
    void testLoadBatch_wrongObjectType() throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var origDef2 = dummyModelDef(INCLUDE_HEADER);
        var origTag2 = dummyTag(origDef2);
        var origId2 = MetadataCodec.decode(origDef2.getHeader().getObjectId());

        unwrap(dal.saveNewObjects(TEST_TENANT, Arrays.asList(origTag, origTag2)));

        var loadTags = dal.loadTags(TEST_TENANT,
                Arrays.asList(ObjectType.DATA, ObjectType.DATA),
                Arrays.asList(origId, origId2),
                Arrays.asList(1, 1),
                Arrays.asList(1, 1));

        var loadLatestTags = dal.loadLatestTags(TEST_TENANT,
                Arrays.asList(ObjectType.DATA, ObjectType.DATA),
                Arrays.asList(origId, origId2),
                Arrays.asList(1, 1));

        var loadLatestVersions = dal.loadLatestVersions(TEST_TENANT,
                Arrays.asList(ObjectType.DATA, ObjectType.DATA),
                Arrays.asList(origId, origId2));

        assertThrows(WrongItemTypeError.class, () -> unwrap(loadTags));
        assertThrows(WrongItemTypeError.class, () -> unwrap(loadLatestTags));
        assertThrows(WrongItemTypeError.class, () -> unwrap(loadLatestVersions));
    }
}
