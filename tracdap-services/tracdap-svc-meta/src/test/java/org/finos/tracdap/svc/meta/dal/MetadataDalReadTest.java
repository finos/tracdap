/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.meta.dal;

import org.finos.tracdap.metadata.TagSelector;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.exception.EMetadataWrongType;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.finos.tracdap.test.meta.IDalTestable;
import org.finos.tracdap.test.meta.JdbcUnit;
import org.finos.tracdap.test.meta.JdbcIntegration;

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
    void loadByVersion_single() throws Exception {

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
    void loadByVersion_batch() throws Exception {

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

        var batch = unwrap(dal.loadObjects(TEST_TENANT, List.of(selector1t1, selector2t1, selector2t2)));
        var v1t1 = batch.get(0);
        var v2t1 = batch.get(1);
        var v2t2 = batch.get(2);

        assertEquals(origTag, v1t1);
        assertEquals(nextDefTag1, v2t1);
        assertEquals(nextDefTag2, v2t2);
    }

    @Test
    void loadAsOfTime_single() throws Exception {

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
        var v1t1AsOf = MetadataCodec.decodeDatetime(origTag.getHeader().getTagTimestamp()).plusNanos(500000);
        var v2t1AsOf = MetadataCodec.decodeDatetime(nextDefTag1.getHeader().getTagTimestamp()).plusNanos(500000);
        var v2t2AsOf = MetadataCodec.decodeDatetime(nextDefTag2.getHeader().getTagTimestamp()).plusNanos(500000);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        // Load all three items by explicit version / tag number
        var selector1t1 = selector
                .setObjectAsOf(MetadataCodec.encodeDatetime(v1t1AsOf))
                .setTagAsOf(MetadataCodec.encodeDatetime(v1t1AsOf))
                .build();

        var selector2t1 = selector
                .setObjectAsOf(MetadataCodec.encodeDatetime(v2t1AsOf))
                .setTagAsOf(MetadataCodec.encodeDatetime(v2t1AsOf))
                .build();

        var selector2t2 = selector
                .setObjectAsOf(MetadataCodec.encodeDatetime(v2t2AsOf))
                .setTagAsOf(MetadataCodec.encodeDatetime(v2t2AsOf))
                .build();

        var v1t1 = unwrap(dal.loadObject(TEST_TENANT, selector1t1));
        var v2t1 = unwrap(dal.loadObject(TEST_TENANT, selector2t1));
        var v2t2 = unwrap(dal.loadObject(TEST_TENANT, selector2t2));

        assertEquals(origTag, v1t1);
        assertEquals(nextDefTag1, v2t1);
        assertEquals(nextDefTag2, v2t2);
    }

    @Test
    void loadAsOfTime_batch() throws Exception {

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
        var v1t1AsOf = MetadataCodec.decodeDatetime(origTag.getHeader().getTagTimestamp()).plusNanos(500000);
        var v2t1AsOf = MetadataCodec.decodeDatetime(nextDefTag1.getHeader().getTagTimestamp()).plusNanos(500000);
        var v2t2AsOf = MetadataCodec.decodeDatetime(nextDefTag2.getHeader().getTagTimestamp()).plusNanos(500000);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        // Load all three items by explicit version / tag number
        var selector1t1 = selector
                .setObjectAsOf(MetadataCodec.encodeDatetime(v1t1AsOf))
                .setTagAsOf(MetadataCodec.encodeDatetime(v1t1AsOf))
                .build();

        var selector2t1 = selector
                .setObjectAsOf(MetadataCodec.encodeDatetime(v2t1AsOf))
                .setTagAsOf(MetadataCodec.encodeDatetime(v2t1AsOf))
                .build();

        var selector2t2 = selector
                .setObjectAsOf(MetadataCodec.encodeDatetime(v2t2AsOf))
                .setTagAsOf(MetadataCodec.encodeDatetime(v2t2AsOf))
                .build();

        // Same test with batch load
        var batch = unwrap(dal.loadObjects(TEST_TENANT, List.of(selector1t1, selector2t1, selector2t2)));
        var v1t1 = batch.get(0);
        var v2t1 = batch.get(1);
        var v2t2 = batch.get(2);

        assertEquals(origTag, v1t1);
        assertEquals(nextDefTag1, v2t1);
        assertEquals(nextDefTag2, v2t2);
    }

    @Test
    void loadLatest_single() throws Exception {

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
    void loadLatest_batch() throws Exception {

        // A second object to include in the batch so the batch size is bigger than 1
        var extraDef = dummyDataDef();
        var extraTag = dummyTag(extraDef, INCLUDE_HEADER);
        var extraId = UUID.fromString(extraTag.getHeader().getObjectId());

        unwrap(dal.saveNewObject(TEST_TENANT, extraTag));

        var extraSelector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(extraId.toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

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
        var v1t1Batch = unwrap(dal.loadObjects(TEST_TENANT, List.of(selector, extraSelector)));
        var v1t1 = v1t1Batch.get(0);

        unwrap(dal.saveNewVersion(TEST_TENANT, nextDefTag1));
        var v2t1Batch = unwrap(dal.loadObjects(TEST_TENANT, List.of(selector, extraSelector)));
        var v2t1 = v2t1Batch.get(0);

        unwrap(dal.saveNewTag(TEST_TENANT, nextDefTag2));
        var v2t2Batch = unwrap(dal.loadObjects(TEST_TENANT, List.of(selector, extraSelector)));
        var v2t2 = v2t2Batch.get(0);

        assertEquals(origTag, v1t1);
        assertEquals(nextDefTag1, v2t1);
        assertEquals(nextDefTag2, v2t2);
    }

    @Test
    void loadComboSelector_single() throws Exception {

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
        var v2t1AsOf = MetadataCodec.decodeDatetime(v2t1Tag.getHeader().getTagTimestamp()).plusNanos(500000);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        // Explicit object version, latest tag

        var selectCombo1 = selector
                .setObjectVersion(2)
                .setLatestTag(true)
                .build();
        // Use object as-of for v2t1, but select latest tag, should give v1t2

        var selectCombo2 = selector
                .setObjectAsOf(MetadataCodec.encodeDatetime(v2t1AsOf))
                .setLatestTag(true)
                .build();

        // Latest object, tag version 1

        var selectCombo3 = selector
                .setLatestObject(true)
                .setTagVersion(1)
                .build();

        var combo1 = unwrap(dal.loadObject(TEST_TENANT, selectCombo1));
        var combo2 = unwrap(dal.loadObject(TEST_TENANT, selectCombo2));
        var combo3 = unwrap(dal.loadObject(TEST_TENANT, selectCombo3));

        assertEquals(v2t2Tag, combo1);
        assertEquals(v2t2Tag, combo2);
        assertEquals(v3t1Tag, combo3);
    }

    @Test
    void loadComboSelector_batch() throws Exception {

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
        var v2t1AsOf = MetadataCodec.decodeDatetime(v2t1Tag.getHeader().getTagTimestamp()).plusNanos(500000);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        // Explicit object version, latest tag

        var selectCombo1 = selector
                .setObjectVersion(2)
                .setLatestTag(true)
                .build();

        // Use object as-of for v2t1, but select latest tag, should give v1t2

        var selectCombo2 = selector
                .setObjectAsOf(MetadataCodec.encodeDatetime(v2t1AsOf))
                .setLatestTag(true)
                .build();

        // Latest object, tag version 1

        var selectCombo3 = selector
                .setLatestObject(true)
                .setTagVersion(1)
                .build();

        // Testing selectors with different criteria all as part of one batch

        var batch = unwrap(dal.loadObjects(TEST_TENANT, List.of(selectCombo1, selectCombo2, selectCombo3)));
        var combo1 = batch.get(0);
        var combo2 = batch.get(1);
        var combo3 = batch.get(2);

        assertEquals(v2t2Tag, combo1);
        assertEquals(v2t2Tag, combo2);
        assertEquals(v3t1Tag, combo3);
    }
    
    @Test
    void multiValuedAttr_single() throws Exception {

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
    void multiValuedAttr_batch() throws Exception {

        var origDef = dummyDataDef();
        var origTag = addMultiValuedAttr(dummyTag(origDef, INCLUDE_HEADER));
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var origDef2 = dummyDataDef();
        var origTag2 = addMultiValuedAttr(dummyTag(origDef2, INCLUDE_HEADER));
        var origId2 = UUID.fromString(origTag2.getHeader().getObjectId());

        unwrap(dal.saveNewObjects(TEST_TENANT, List.of(origTag, origTag2)));

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId2.toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var batch = unwrap(dal.loadObjects(TEST_TENANT, List.of(selector, selector2)));
        var loadedTag = batch.get(0);
        var loadedTag2 = batch.get(1);

        assertEquals(origTag, loadedTag);
        assertEquals(origTag2, loadedTag2);
    }

    @Test
    void timeWindowBoundary_single() throws Exception {

        var v1Def = dummyDataDef();
        var v1Tag = dummyTag(v1Def, INCLUDE_HEADER);

        Thread.sleep(10);
        var v2Tag = tagForNextObject(v1Tag, nextDataDef(v1Def), INCLUDE_HEADER);

        Thread.sleep(10);
        var t2Tag = nextTag(v2Tag, UPDATE_TAG_VERSION);

        unwrap(dal.saveNewObject(TEST_TENANT, v1Tag));
        unwrap(dal.saveNewVersion(TEST_TENANT, v2Tag));
        unwrap(dal.saveNewTag(TEST_TENANT, t2Tag));

        var origId = UUID.fromString(v1Tag.getHeader().getObjectId());

        // Metadata timestamps use millisecond precision
        var v2BoundaryTime = MetadataCodec.decodeDatetime(v2Tag.getHeader().getTagTimestamp());
        var v2PriorTime = v2BoundaryTime.minusNanos(1000);
        var t2BoundaryTime = MetadataCodec.decodeDatetime(t2Tag.getHeader().getTagTimestamp());
        var t2PriorTime = t2BoundaryTime.minusNanos(1000);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        var v2BoundarySelector = selector.setObjectAsOf(MetadataCodec.encodeDatetime(v2BoundaryTime)).setTagVersion(1).build();
        var v2PriorSelector = selector.setObjectAsOf(MetadataCodec.encodeDatetime(v2PriorTime)).setTagVersion(1).build();
        var t2BoundarySelector = selector.setObjectVersion(2).setTagAsOf(MetadataCodec.encodeDatetime(t2BoundaryTime)).build();
        var t2PriorSelector = selector.setObjectVersion(2).setTagAsOf(MetadataCodec.encodeDatetime(t2PriorTime)).build();

        var v2BoundaryTag = unwrap(dal.loadObject(TEST_TENANT, v2BoundarySelector));
        var v2PriorTag = unwrap(dal.loadObject(TEST_TENANT, v2PriorSelector));
        var t2BoundaryTag = unwrap(dal.loadObject(TEST_TENANT, t2BoundarySelector));
        var t2PriorTag = unwrap(dal.loadObject(TEST_TENANT, t2PriorSelector));

        assertEquals(v2Tag, v2BoundaryTag);
        assertEquals(v1Tag, v2PriorTag);
        assertEquals(t2Tag, t2BoundaryTag);
        assertEquals(v2Tag, t2PriorTag);
    }

    @Test
    void timeWindowBoundary_batch() throws Exception {

        var v1Def = dummyDataDef();
        var v1Tag = dummyTag(v1Def, INCLUDE_HEADER);

        Thread.sleep(10);
        var v2Tag = tagForNextObject(v1Tag, nextDataDef(v1Def), INCLUDE_HEADER);

        Thread.sleep(10);
        var t2Tag = nextTag(v2Tag, UPDATE_TAG_VERSION);

        unwrap(dal.saveNewObject(TEST_TENANT, v1Tag));
        unwrap(dal.saveNewVersion(TEST_TENANT, v2Tag));
        unwrap(dal.saveNewTag(TEST_TENANT, t2Tag));

        var origId = UUID.fromString(v1Tag.getHeader().getObjectId());

        // Metadata timestamps use millisecond precision
        var v2BoundaryTime = MetadataCodec.decodeDatetime(v2Tag.getHeader().getTagTimestamp());
        var v2PriorTime = v2BoundaryTime.minusNanos(1000);
        var t2BoundaryTime = MetadataCodec.decodeDatetime(t2Tag.getHeader().getTagTimestamp());
        var t2PriorTime = t2BoundaryTime.minusNanos(1000);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        var v2BoundarySelector = selector.setObjectAsOf(MetadataCodec.encodeDatetime(v2BoundaryTime)).setTagVersion(1).build();
        var v2PriorSelector = selector.setObjectAsOf(MetadataCodec.encodeDatetime(v2PriorTime)).setTagVersion(1).build();
        var t2BoundarySelector = selector.setObjectVersion(2).setTagAsOf(MetadataCodec.encodeDatetime(t2BoundaryTime)).build();
        var t2PriorSelector = selector.setObjectVersion(2).setTagAsOf(MetadataCodec.encodeDatetime(t2PriorTime)).build();

        var batch = unwrap(dal.loadObjects(TEST_TENANT, List.of(v2BoundarySelector, v2PriorSelector)));
        var v2BoundaryTag = batch.get(0);
        var v2PriorTag = batch.get(1);

        var batch2 = unwrap(dal.loadObjects(TEST_TENANT, List.of(t2BoundarySelector, t2PriorSelector)));
        var t2BoundaryTag = batch2.get(0);
        var t2PriorTag = batch2.get(1);

        assertEquals(v2Tag, v2BoundaryTag);
        assertEquals(v1Tag, v2PriorTag);
        assertEquals(t2Tag, t2BoundaryTag);
        assertEquals(v2Tag, t2PriorTag);
    }

    @Test
    void missingItems_single() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        Thread.sleep(1);
        var asOfTime = MetadataCodec.decodeDatetime(origTag.getHeader().getTagTimestamp()).minusNanos(500000);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        var missing1 = selector.setObjectVersion(1).setTagVersion(2).build(); // Missing tag
        var missing2 = selector.setObjectVersion(2).setTagVersion(1).build(); // Missing ver
        var missing3 = selector.setObjectVersion(2).setLatestTag(true).build(); // Missing ver
        var missing4 = selector.setObjectAsOf(MetadataCodec.encodeDatetime(asOfTime)).setLatestTag(true).build(); // as-of before object creation

        // Object should definitely be missing before it is saved!
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing1)));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing2)));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing3)));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing4)));

        // Save an item
        unwrap(dal.saveNewObject(TEST_TENANT, origTag));

        // No selectors should match
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing1)));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing2)));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing3)));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObject(TEST_TENANT, missing4)));
    }

    @Test
    void missingItems_batch() throws Exception {

        var validDef = dummyDataDef();
        var validTag = dummyTag(validDef, INCLUDE_HEADER);
        var validId = UUID.fromString(validTag.getHeader().getObjectId());

        unwrap(dal.saveNewObject(TEST_TENANT, validTag));

        var validSelector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(validId.toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        Thread.sleep(1);
        var asOfTime = MetadataCodec.decodeDatetime(origTag.getHeader().getTagTimestamp()).minusNanos(500000);

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString());

        var missing1 = selector.setObjectVersion(1).setTagVersion(2).build(); // Missing tag
        var missing2 = selector.setObjectVersion(2).setTagVersion(1).build(); // Missing ver
        var missing3 = selector.setObjectVersion(2).setLatestTag(true).build(); // Missing ver
        var missing4 = selector.setObjectAsOf(MetadataCodec.encodeDatetime(asOfTime)).setLatestTag(true).build(); // as-of before object creation

        // Object should definitely be missing before it is saved!
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObjects(TEST_TENANT, List.of(validSelector, missing1))));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObjects(TEST_TENANT, List.of(validSelector, missing2))));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObjects(TEST_TENANT, List.of(validSelector, missing3))));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObjects(TEST_TENANT, List.of(validSelector, missing4))));

        // Save an item
        unwrap(dal.saveNewObject(TEST_TENANT, origTag));

        // No selectors should match
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObjects(TEST_TENANT, List.of(validSelector, missing1))));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObjects(TEST_TENANT, List.of(validSelector, missing2))));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObjects(TEST_TENANT, List.of(validSelector, missing3))));
        assertThrows(EMetadataNotFound.class, () -> unwrap(dal.loadObjects(TEST_TENANT, List.of(validSelector, missing4))));
    }

    @Test
    void wrongObjectType_single() throws Exception {

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

        assertThrows(EMetadataWrongType.class, () -> unwrap(dal.loadObject(TEST_TENANT, selector)));
    }

    @Test
    void wrongObjectType_batch() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var origDef2 = dummyModelDef();
        var origTag2 = dummyTag(origDef2, INCLUDE_HEADER);
        var origId2 = UUID.fromString(origTag2.getHeader().getObjectId());

        unwrap(dal.saveNewObjects(TEST_TENANT, List.of(origTag, origTag2)));

        // Two selectors in the batch
        // Only one has wrong type

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(origId.toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(origId2.toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        assertThrows(EMetadataWrongType.class, () -> unwrap(dal.loadObjects(TEST_TENANT, List.of(selector, selector2))));
    }
}
