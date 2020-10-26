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

package com.accenture.trac.svc.meta.dal;

import com.accenture.trac.svc.meta.exception.MissingItemError;
import com.accenture.trac.svc.meta.exception.WrongItemTypeError;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.ObjectType;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcUnit;
import com.accenture.trac.svc.meta.test.JdbcIntegration;
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

    @ExtendWith(JdbcUnit.class)
    static class Unit extends MetadataDalReadTest {}

    @Tag("integration")
    @Tag("int-metadb")
    @ExtendWith(JdbcIntegration.class)
    static class Integration extends MetadataDalReadTest {}

    @Test
    void testLoadOneExplicit_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

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
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

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
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var nextDefTag1 = dummyTag(nextDataDef(origDef), INCLUDE_HEADER);

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
    void testLoadOne_multiValuedAttr() throws Exception {

        var origDef = dummyDataDef();
        var origTag = addMultiValuedAttr(dummyTag(origDef, INCLUDE_HEADER));
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        unwrap(dal.saveNewObject(TEST_TENANT, origTag));

        var explicit = unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));
        var latestTag = unwrap(dal.loadLatestTag(TEST_TENANT, ObjectType.DATA, origId, 1));
        var latestVersion = unwrap(dal.loadLatestVersion(TEST_TENANT, ObjectType.DATA, origId));

        assertEquals(origTag, explicit);
        assertEquals(origTag, latestTag);
        assertEquals(origTag, latestVersion);
    }

    @Test
    void testLoadOne_missingItems() throws Exception {

        assertThrows(MissingItemError.class, () -> unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, UUID.randomUUID(), 1, 1)));
        assertThrows(MissingItemError.class, () -> unwrap(dal.loadLatestTag(TEST_TENANT, ObjectType.DATA, UUID.randomUUID(), 1)));
        assertThrows(MissingItemError.class, () -> unwrap(dal.loadLatestVersion(TEST_TENANT, ObjectType.DATA, UUID.randomUUID())));

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        // Save an item
        var future = dal.saveNewObject(TEST_TENANT, origTag);
        unwrap(future);

        assertThrows(MissingItemError.class, () -> unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2)));  // Missing tag
        assertThrows(MissingItemError.class, () -> unwrap(dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1)));  // Missing ver
        assertThrows(MissingItemError.class, () -> unwrap(dal.loadLatestTag(TEST_TENANT, ObjectType.DATA, origId, 2)));  // Missing ver
    }

    @Test
    void testLoadOne_wrongObjectType() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        unwrap(dal.saveNewObject(TEST_TENANT, origTag));

        assertThrows(WrongItemTypeError.class, () -> unwrap(dal.loadTag(TEST_TENANT, ObjectType.MODEL, origId, 1, 1)));
        assertThrows(WrongItemTypeError.class, () -> unwrap(dal.loadLatestTag(TEST_TENANT, ObjectType.MODEL, origId, 1)));
        assertThrows(WrongItemTypeError.class, () -> unwrap(dal.loadLatestVersion(TEST_TENANT, ObjectType.MODEL, origId)));
    }

    @Test
    void testLoadBatchExplicit_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var modelDef = dummyModelDef();
        var modelTag = dummyTag(modelDef, INCLUDE_HEADER);
        var modelId = MetadataCodec.decode(modelTag.getHeader().getObjectId());

        // Save everything first
        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextDefTag1))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextDefTag2))
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, modelTag));

        unwrap(future);

        var types = List.of(ObjectType.DATA, ObjectType.DATA, ObjectType.DATA, ObjectType.MODEL);
        var ids = List.of(origId, origId, origId, modelId);
        var versions = List.of(1, 2, 2, 1);
        var tagVersions = List.of(1, 1, 2, 1);

        var result = unwrap(dal.loadTags(TEST_TENANT, types, ids, versions, tagVersions));

        assertEquals(origTag, result.get(0));
        assertEquals(nextDefTag1, result.get(1));
        assertEquals(nextDefTag2, result.get(2));
        assertEquals(modelTag, result.get(3));
    }

    @Test
    void testLoadBatchLatestVersion_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var modelDef = dummyModelDef();
        var modelTag = dummyTag(modelDef, INCLUDE_HEADER);
        var modelId = MetadataCodec.decode(modelTag.getHeader().getObjectId());

        // Save everything first
        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextDefTag1))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextDefTag2))
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, modelTag));

        unwrap(future);

        var types = List.of(ObjectType.DATA, ObjectType.MODEL);
        var ids = List.of(origId, modelId);

        var result = unwrap(dal.loadLatestVersions(TEST_TENANT, types, ids));

        assertEquals(nextDefTag2, result.get(0));
        assertEquals(modelTag, result.get(1));
    }

    @Test
    void testLoadBatchLatestTag_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var modelDef = dummyModelDef();
        var modelTag = dummyTag(modelDef, INCLUDE_HEADER);
        var modelId = MetadataCodec.decode(modelTag.getHeader().getObjectId());

        // Save everything first
        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextDefTag1))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextDefTag2))
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, modelTag));

        unwrap(future);

        var types = List.of(ObjectType.DATA, ObjectType.DATA, ObjectType.MODEL);
        var ids = List.of(origId, origId, modelId);
        var versions = List.of(1, 2, 1);

        var result = unwrap(dal.loadLatestTags(TEST_TENANT, types, ids, versions));

        assertEquals(origTag, result.get(0));
        assertEquals(nextDefTag2, result.get(1));
        assertEquals(modelTag, result.get(2));
    }

    @Test
    void testLoadBatch_multiValuedAttr() throws Exception {

        var origDef = dummyDataDef();
        var origTag = addMultiValuedAttr(dummyTag(origDef, INCLUDE_HEADER));
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var modelDef = dummyModelDef();
        var modelTag = addMultiValuedAttr(dummyTag(modelDef, INCLUDE_HEADER));
        var modelId = MetadataCodec.decode(modelTag.getHeader().getObjectId());

        unwrap(dal.saveNewObject(TEST_TENANT, origTag));
        unwrap(dal.saveNewObject(TEST_TENANT, modelTag));

        var types = List.of(ObjectType.DATA, ObjectType.MODEL);
        var ids = List.of(origId, modelId);
        var versions = List.of(1, 1);
        var tagVersions = List.of(1, 1);

        var explicit = unwrap(dal.loadTags(TEST_TENANT, types, ids, versions, tagVersions));
        var latestTag = unwrap(dal.loadLatestTags(TEST_TENANT, types, ids, versions));
        var latestVersion = unwrap(dal.loadLatestVersions(TEST_TENANT, types, ids));

        assertEquals(origTag, explicit.get(0));
        assertEquals(origTag, latestTag.get(0));
        assertEquals(origTag, latestVersion.get(0));

        assertEquals(modelTag, explicit.get(1));
        assertEquals(modelTag, latestTag.get(1));
        assertEquals(modelTag, latestVersion.get(1));
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

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

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

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var origDef2 = dummyModelDef();
        var origTag2 = dummyTag(origDef2, INCLUDE_HEADER);
        var origId2 = MetadataCodec.decode(origTag2.getHeader().getObjectId());

        unwrap(dal.saveNewObjects(TEST_TENANT, List.of(origTag, origTag2)));

        var loadTags = dal.loadTags(TEST_TENANT,
                List.of(ObjectType.DATA, ObjectType.DATA),
                List.of(origId, origId2),
                List.of(1, 1),
                List.of(1, 1));

        var loadLatestTags = dal.loadLatestTags(TEST_TENANT,
                List.of(ObjectType.DATA, ObjectType.DATA),
                List.of(origId, origId2),
                List.of(1, 1));

        var loadLatestVersions = dal.loadLatestVersions(TEST_TENANT,
                List.of(ObjectType.DATA, ObjectType.DATA),
                List.of(origId, origId2));

        assertThrows(WrongItemTypeError.class, () -> unwrap(loadTags));
        assertThrows(WrongItemTypeError.class, () -> unwrap(loadLatestTags));
        assertThrows(WrongItemTypeError.class, () -> unwrap(loadLatestVersions));
    }
}
