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

import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.TagSelector;
import com.accenture.trac.svc.meta.exception.EMissingItem;
import com.accenture.trac.svc.meta.exception.EWrongItemType;
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
    void testLoadOne_selectVersion() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        // Save v1 t1, v2 t1, v2 t2
        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextDefTag1))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextDefTag2));

        unwrap(future);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        // Load all three items by explicit version / tag number
        var selector1t1 = selector.setObjectVersion(1).setTagVersion(1).build();
        var selector2t1 = selector.setObjectVersion(2).setTagVersion(1).build();
        var selector2t2 = selector.setObjectVersion(2).setTagVersion(2).build();

        var v1t1 = unwrap(dal.loadObject(TEST_TENANT, selector1t1));
        var v2t1 = unwrap(dal.loadObject(TEST_TENANT, selector2t1));
        var v2t2 = unwrap(dal.loadObject(TEST_TENANT, selector2t2));

        assertEquals(origTag, v1t1);
        assertEquals(nextDefTag1, v2t1);
        assertEquals(nextDefTag2, v2t2);
    }

    @Test
    void testLoadOne_selectAsOf() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);

        Thread.sleep(1);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);

        Thread.sleep(1);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);

        unwrap(dal.saveNewObject(TEST_TENANT, origTag));
        unwrap(dal.saveNewVersion(TEST_TENANT, nextDefTag1));
        unwrap(dal.saveNewTag(TEST_TENANT, nextDefTag2));

        var origId = UUID.fromString(origTag.getHeader().getObjectId());
        var v1t1AsOf = MetadataCodec.parseDatetime(origTag.getHeader().getTagTimestamp()).plusNanos(500000);
        var v2t1AsOf = MetadataCodec.parseDatetime(nextDefTag1.getHeader().getTagTimestamp()).plusNanos(500000);
        var v2t2AsOf = MetadataCodec.parseDatetime(nextDefTag2.getHeader().getTagTimestamp()).plusNanos(500000);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        // Load all three items by explicit version / tag number
        var selector1t1 = selector
                .setObjectAsOf(MetadataCodec.quoteDatetime(v1t1AsOf))
                .setTagAsOf(MetadataCodec.quoteDatetime(v1t1AsOf))
                .build();

        var selector2t1 = selector
                .setObjectAsOf(MetadataCodec.quoteDatetime(v2t1AsOf))
                .setTagAsOf(MetadataCodec.quoteDatetime(v2t1AsOf))
                .build();

        var selector2t2 = selector
                .setObjectAsOf(MetadataCodec.quoteDatetime(v2t2AsOf))
                .setTagAsOf(MetadataCodec.quoteDatetime(v2t2AsOf))
                .build();

        var v1t1 = unwrap(dal.loadObject(TEST_TENANT, selector1t1));
        var v2t1 = unwrap(dal.loadObject(TEST_TENANT, selector2t1));
        var v2t2 = unwrap(dal.loadObject(TEST_TENANT, selector2t2));

        assertEquals(origTag, v1t1);
        assertEquals(nextDefTag1, v2t1);
        assertEquals(nextDefTag2, v2t2);
    }

    @Test
    void testLoadOne_selectLatest() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        unwrap(dal.saveNewObject(TEST_TENANT, origTag));
        var v1t1 = unwrap(dal.loadObject(TEST_TENANT, selector));

        unwrap(dal.saveNewVersion(TEST_TENANT, nextDefTag1));
        var v2t1 = unwrap(dal.loadObject(TEST_TENANT, selector));

        unwrap(dal.saveNewTag(TEST_TENANT, nextDefTag2));
        var v2t2 = unwrap(dal.loadObject(TEST_TENANT, selector));

        assertEquals(origTag, v1t1);
        assertEquals(nextDefTag1, v2t1);
        assertEquals(nextDefTag2, v2t2);
    }

    @Test
    void testLoadOne_comboSelector() throws Exception {

        var v1Def = dummyDataDef();
        var v1Tag = dummyTag(v1Def, INCLUDE_HEADER);

        Thread.sleep(1);
        var v2t1Tag = tagForNextObject(v1Tag, nextDataDef(v1Def), INCLUDE_HEADER);

        Thread.sleep(1);
        var v2t2Tag = nextTag(v2t1Tag, UPDATE_TAG_VERSION);

        Thread.sleep(1);
        var v3t1Tag = tagForNextObject(v2t1Tag, nextDataDef(v2t1Tag.getDefinition()), INCLUDE_HEADER);

        unwrap(dal.saveNewObject(TEST_TENANT, v1Tag));
        unwrap(dal.saveNewVersion(TEST_TENANT, v2t1Tag));
        unwrap(dal.saveNewTag(TEST_TENANT, v2t2Tag));
        unwrap(dal.saveNewVersion(TEST_TENANT, v3t1Tag));

        var origId = UUID.fromString(v1Tag.getHeader().getObjectId());
        var v2t1AsOf = MetadataCodec.parseDatetime(v2t1Tag.getHeader().getTagTimestamp()).plusNanos(500000);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        // Explicit object version, latest tag

        var selectCombo1 = selector
                .setObjectVersion(2)
                .setLatestTag(true)
                .build();

        var combo1 = unwrap(dal.loadObject(TEST_TENANT, selectCombo1));
        assertEquals(v2t2Tag, combo1);

        // Use object as-of for v2t1, but select latest tag, should give v1t2

        var selectCombo2 = selector
                .setObjectAsOf(MetadataCodec.quoteDatetime(v2t1AsOf))
                .setLatestTag(true)
                .build();

        var combo2 = unwrap(dal.loadObject(TEST_TENANT, selectCombo2));
        assertEquals(v2t2Tag, combo2);

        // Latest object, tag version 1

        var selectCombo3 = selector
                .setLatestObject(true)
                .setTagVersion(1)
                .build();

        var combo3 = unwrap(dal.loadObject(TEST_TENANT, selectCombo3));
        assertEquals(v3t1Tag, combo3);
    }
    
    @Test
    void testLoadOne_multiValuedAttr() throws Exception {

        var origDef = dummyDataDef();
        var origTag = addMultiValuedAttr(dummyTag(origDef, INCLUDE_HEADER));
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        unwrap(dal.saveNewObject(TEST_TENANT, origTag));

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var loadedTag = unwrap(dal.loadObject(TEST_TENANT, selector));

        assertEquals(origTag, loadedTag);
    }

    @Test
    void testLoadOne_missingItems() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        Thread.sleep(1);
        var asOfTime = MetadataCodec.parseDatetime(origTag.getHeader().getTagTimestamp()).minusNanos(500000);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        var missing1 = selector.setObjectVersion(1).setTagVersion(2).build(); // Missing tag
        var missing2 = selector.setObjectVersion(2).setTagVersion(1).build(); // Missing ver
        var missing3 = selector.setObjectVersion(2).setLatestTag(true).build(); // Missing ver
        var missing4 = selector.setObjectAsOf(MetadataCodec.quoteDatetime(asOfTime)).setLatestTag(true).build(); // as-of before object creation

        // Object should definitely be missing before it is saved!
        assertThrows(EMissingItem.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing1)));
        assertThrows(EMissingItem.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing2)));
        assertThrows(EMissingItem.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing3)));
        assertThrows(EMissingItem.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing4)));

        // Save an item
        unwrap(dal.saveNewObject(TEST_TENANT, origTag));

        // No selectors should match
        assertThrows(EMissingItem.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing1)));
        assertThrows(EMissingItem.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing2)));
        assertThrows(EMissingItem.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing3)));
        assertThrows(EMissingItem.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing4)));
    }

    @Test
    void testLoadOne_wrongObjectType() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        unwrap(dal.saveNewObject(TEST_TENANT, origTag));

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(origId.toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        assertThrows(EWrongItemType.class, () -> unwrap(dal.loadObject(TEST_TENANT, selector)));
    }

    @Test
    void testLoadBatchExplicit_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var modelDef = dummyModelDef();
        var modelTag = dummyTag(modelDef, INCLUDE_HEADER);
        var modelId = UUID.fromString(modelTag.getHeader().getObjectId());

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
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var modelDef = dummyModelDef();
        var modelTag = dummyTag(modelDef, INCLUDE_HEADER);
        var modelId = UUID.fromString(modelTag.getHeader().getObjectId());

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
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var modelDef = dummyModelDef();
        var modelTag = dummyTag(modelDef, INCLUDE_HEADER);
        var modelId = UUID.fromString(modelTag.getHeader().getObjectId());

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
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var modelDef = dummyModelDef();
        var modelTag = addMultiValuedAttr(dummyTag(modelDef, INCLUDE_HEADER));
        var modelId = UUID.fromString(modelTag.getHeader().getObjectId());

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

        assertThrows(EMissingItem.class, () -> unwrap(loadTags));
        assertThrows(EMissingItem.class, () -> unwrap(loadLatestTags));
        assertThrows(EMissingItem.class, () -> unwrap(loadLatestVersions));

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

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

        assertThrows(EMissingItem.class, () -> unwrap(loadTags2));  // Missing tag
        assertThrows(EMissingItem.class, () -> unwrap(loadTags3));  // Missing ver
        assertThrows(EMissingItem.class, () -> unwrap(loadLatestTag2));  // Missing ver
    }

    @Test
    void testLoadBatch_wrongObjectType() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var origDef2 = dummyModelDef();
        var origTag2 = dummyTag(origDef2, INCLUDE_HEADER);
        var origId2 = UUID.fromString(origTag2.getHeader().getObjectId());

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

        assertThrows(EWrongItemType.class, () -> unwrap(loadTags));
        assertThrows(EWrongItemType.class, () -> unwrap(loadLatestTags));
        assertThrows(EWrongItemType.class, () -> unwrap(loadLatestVersions));
    }
}
