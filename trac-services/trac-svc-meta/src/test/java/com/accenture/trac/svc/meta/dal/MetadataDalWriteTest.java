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

import com.accenture.trac.svc.meta.exception.EDuplicateItem;
import com.accenture.trac.svc.meta.exception.EMissingItem;
import com.accenture.trac.svc.meta.exception.EWrongItemType;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.ObjectType;
import static com.accenture.trac.svc.meta.test.TestData.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcUnit;
import com.accenture.trac.svc.meta.test.JdbcIntegration;

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

    @ExtendWith(JdbcUnit.class)
    static class Unit extends MetadataDalWriteTest {}

    @Tag("integration")
    @Tag("int-metadb")
    @ExtendWith(JdbcIntegration.class)
    static class Integration extends MetadataDalWriteTest {}

    @Test
    void testSaveNewObject_ok() throws Exception {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        assertEquals(origTag, unwrap(future));

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var future2 = dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getHeader().getObjectId()), 1, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, unwrap(result1));
        assertEquals(multi2, unwrap(result2));
    }

    @Test
    void testSaveNewObject_multiValuedAttr() throws Exception {

        // Save one
        var origDef = dummyDataDef();
        var origTag = addMultiValuedAttr(dummyTag(origDef, INCLUDE_HEADER));
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        assertEquals(origTag, unwrap(future));

        // Save multiple
        var multi1 = addMultiValuedAttr(dummyTagForObjectType(ObjectType.DATA));
        var multi2 = addMultiValuedAttr(dummyTagForObjectType(ObjectType.MODEL));

        var future2 = dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getHeader().getObjectId()), 1, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, unwrap(result1));
        assertEquals(multi2, unwrap(result2));
    }

    @Test
    void testSaveNewObject_duplicate() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var saveDup =  dal.saveNewObjects(TEST_TENANT, List.of(origTag, origTag));
        var loadDup = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1);

        assertThrows(EDuplicateItem.class, () -> unwrap(saveDup));
        assertThrows(EMissingItem.class, () -> unwrap(loadDup));

        var saveDup2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag));

        var loadDup2 = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1);

        // First insert should succeed if they are run one by one
        assertThrows(EDuplicateItem.class, () -> unwrap(saveDup2));
        assertDoesNotThrow(() -> unwrap(loadDup2));
    }

    @Test
    void testSaveNewVersion_ok() throws Exception {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef);
        var nextTag = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1));

        assertEquals(nextTag, unwrap(future));

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = tagForNextObject(multi1, nextDataDef(multi1.getDefinition()), INCLUDE_HEADER);
        var multi2v2 = tagForNextObject(multi2, nextModelDef(multi2.getDefinition()), INCLUDE_HEADER);

        var future2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2)))
                .thenCompose(x -> dal.saveNewVersions(TEST_TENANT, List.of(multi1v2, multi2v2)));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getHeader().getObjectId()), 2, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getHeader().getObjectId()), 2, 1);

        assertEquals(multi1v2, unwrap(result1));
        assertEquals(multi2v2, unwrap(result2));
    }

    @Test
    void testSaveNewVersion_multiValuedAttr() throws Exception {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef);
        var nextTag = addMultiValuedAttr(tagForNextObject(origTag, nextDef, INCLUDE_HEADER));

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1));

        assertEquals(nextTag, unwrap(future));

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = addMultiValuedAttr(tagForNextObject(multi1, nextDataDef(multi1.getDefinition()), INCLUDE_HEADER));
        var multi2v2 = addMultiValuedAttr(tagForNextObject(multi2, nextModelDef(multi2.getDefinition()), INCLUDE_HEADER));

        var future2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2)))
                .thenCompose(x -> dal.saveNewVersions(TEST_TENANT, List.of(multi1v2, multi2v2)));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getHeader().getObjectId()), 2, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getHeader().getObjectId()), 2, 1);

        assertEquals(multi1v2, unwrap(result1));
        assertEquals(multi2v2, unwrap(result2));
    }

    @Test
    void testSaveNewVersion_duplicate() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef);
        var nextTag = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);

        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveDup =  dal.saveNewVersions(TEST_TENANT, List.of(nextTag, nextTag));
        var loadDup = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1);

        unwrap(saveOrig);
        assertThrows(EDuplicateItem.class, () -> unwrap(saveDup));
        assertThrows(EMissingItem.class, () -> unwrap(loadDup));

        var saveDup2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextTag))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, nextTag));

        var loadDup2 = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1);

        // First insert should succeed if they are run one by one
        assertThrows(EDuplicateItem.class, () -> unwrap(saveDup2));
        assertEquals(nextTag, unwrap(loadDup2));
    }

    @Test
    void testSaveNewVersion_missingObject() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDef = nextDataDef(origDef);
        var nextTag = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);

        // Save next version, single, without saving original
        var saveNext =  dal.saveNewVersion(TEST_TENANT, nextTag);
        assertThrows(EMissingItem.class, () -> unwrap(saveNext));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);

        // Save next, multiple, one item does not have original
        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveNextMulti = dal.saveNewVersions(TEST_TENANT, List.of(nextTag, nextModelTag));

        unwrap(saveOrig);
        assertThrows(EMissingItem.class, () -> unwrap(saveNextMulti));
    }

    @Test
    void testSaveNewVersion_wrongObjectType() throws Exception {

        var dataTag = dummyTagForObjectType(ObjectType.DATA);
        var nextDataTag = tagForNextObject(dataTag, nextDataDef(dataTag.getDefinition()), INCLUDE_HEADER);

        // Create a model def with the same ID as the data def
        var modelDef = dummyModelDef();
        var rawModelTag = tagForNextObject(dataTag, modelDef, INCLUDE_HEADER);
        var nextModelTag = rawModelTag.toBuilder()
                .setHeader(rawModelTag.getHeader().toBuilder()
                .setObjectType(ObjectType.MODEL))
                .build();

        // Save next version, single, without saving original
        var saveOrig = dal.saveNewObject(TEST_TENANT, dataTag);
        var saveNext =  dal.saveNewVersion(TEST_TENANT, nextModelTag);
        var saveNextMulti =  dal.saveNewVersions(TEST_TENANT, List.of(nextDataTag, nextModelTag));

        unwrap(saveOrig);
        assertThrows(EWrongItemType.class, () -> unwrap(saveNext));
        assertThrows(EWrongItemType.class, () -> unwrap(saveNextMulti));
    }

    @Test
    void testSaveNewTag_ok() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

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
                .thenCompose(x -> dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2)))
                .thenCompose(x -> dal.saveNewTags(TEST_TENANT, List.of(multi1v2, multi2v2)));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getHeader().getObjectId()), 1, 2);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getHeader().getObjectId()), 1, 2);

        assertEquals(multi1v2, unwrap(result1));
        assertEquals(multi2v2, unwrap(result2));
    }

    @Test
    void testSaveNewTag_multiValuedAttr() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = addMultiValuedAttr(nextTag(nextDefTag1, UPDATE_TAG_VERSION));
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

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

        var multi1v2 = addMultiValuedAttr(nextTag(multi1, UPDATE_TAG_VERSION));
        var multi2v2 = addMultiValuedAttr(nextTag(multi2, UPDATE_TAG_VERSION));

        var future2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2)))
                .thenCompose(x -> dal.saveNewTags(TEST_TENANT, List.of(multi1v2, multi2v2)));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, MetadataCodec.decode(multi1.getHeader().getObjectId()), 1, 2);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getHeader().getObjectId()), 1, 2);

        assertEquals(multi1v2, unwrap(result1));
        assertEquals(multi2v2, unwrap(result2));
    }

    @Test
    void testSaveNewTag_duplicate() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveDup =  dal.saveNewTags(TEST_TENANT, List.of(nextTag, nextTag));
        var loadDup = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2);

        unwrap(saveOrig);
        assertThrows(EDuplicateItem.class, () -> unwrap(saveDup));
        assertThrows(EMissingItem.class, () -> unwrap(loadDup));

        var saveDup2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextTag))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, nextTag));

        var loadDup2 = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2);

        // First insert should succeed if they are run one by one
        assertThrows(EDuplicateItem.class, () -> unwrap(saveDup2));
        assertEquals(nextTag, unwrap(loadDup2));
    }

    @Test
    void testSaveNewTag_missingObject() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION);

        // Save next tag, single, without saving object
        var saveNext =  dal.saveNewTag(TEST_TENANT, nextTag);
        assertThrows(EMissingItem.class, () -> unwrap(saveNext));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);

        // Save next tag, multiple, one item does not have an object
        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveNextMulti = dal.saveNewTags(TEST_TENANT, List.of(nextTag, nextModelTag));

        unwrap(saveOrig);
        assertThrows(EMissingItem.class, () -> unwrap(saveNextMulti));
    }

    @Test
    void testSaveNewTag_missingVersion() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDef = nextDataDef(origDef);
        var nextDefTag1 = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);

        // Save next tag (single) on an unknown object version
        // This is an error, even for tag v1
        // saveNewVersion must be called first
        var saveObj = dal.saveNewObject(TEST_TENANT, origTag);
        var saveNext1 =  dal.saveNewTag(TEST_TENANT, nextDefTag1);
        var saveNext2 =  dal.saveNewTag(TEST_TENANT, nextDefTag2);

        unwrap(saveObj);
        assertThrows(EMissingItem.class, () -> unwrap(saveNext1));
        assertThrows(EMissingItem.class, () -> unwrap(saveNext2));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);
        var nextModelTag2 = nextTag(nextModelTag, UPDATE_TAG_VERSION);

        // Save object 1 version 2, and object 2 version 1
        unwrap(dal.saveNewVersion(TEST_TENANT, nextDefTag1));
        unwrap(dal.saveNewObject(TEST_TENANT, modelTag));

        // Save next tag (multiple), second item is missing the required object version
        var saveNextMulti = dal.saveNewTags(TEST_TENANT, List.of(nextDefTag2, nextModelTag2));
        assertThrows(EMissingItem.class, () -> unwrap(saveNextMulti));

        // Saving the valid tag by itself should not throw
        Assertions.assertDoesNotThrow(() -> dal.saveNewTag(TEST_TENANT, nextDefTag2));
    }

    @Test
    void testSaveNewTag_wrongObjectType() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var nextHeader = origTag
                .getHeader()
                .toBuilder()
                .setObjectType(ObjectType.MODEL);

        var nextDef = dummyModelDef();

        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION)
                .toBuilder()
                .setHeader(nextHeader)
                .setDefinition(nextDef)
                .build();

        var saveOrig = dal.saveNewObject(TEST_TENANT, origTag);
        var saveTag =  dal.saveNewTag(TEST_TENANT, nextTag);
        var loadWrongType = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2);

        unwrap(saveOrig);
        assertThrows(EWrongItemType.class, () -> unwrap(saveTag));
        assertThrows(EMissingItem.class, () -> unwrap(loadWrongType));

        var origDef2 = dummyModelDef();
        var origTag2 = dummyTag(origDef2, INCLUDE_HEADER);
        var origId2 = MetadataCodec.decode(origTag2.getHeader().getObjectId());

        var nextTag2 = nextTag(origTag2, UPDATE_TAG_VERSION);

        var saveOrig2 = dal.saveNewObject(TEST_TENANT, origTag2);
        var saveTag2 = dal.saveNewTags(TEST_TENANT, List.of(nextTag, nextTag2));
        var loadWrongType2 = dal.loadTags(TEST_TENANT,
                List.of(ObjectType.DATA, ObjectType.MODEL),
                List.of(origId, origId2),
                List.of(1, 1),
                List.of(2, 2));

        unwrap(saveOrig2);
        assertThrows(EWrongItemType.class, () -> unwrap(saveTag2));
        assertThrows(EMissingItem.class, () -> unwrap(loadWrongType2));
    }

    @Test
    void testPreallocate_ok() throws Exception {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = MetadataCodec.decode(origTag.getHeader().getObjectId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, origId))
                .thenCompose(x -> dal.savePreallocatedObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        assertEquals(origTag, unwrap(future));

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.MODEL);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);
        var id1 = MetadataCodec.decode(multi1.getHeader().getObjectId());
        var id2 = MetadataCodec.decode(multi2.getHeader().getObjectId());

        var future2 = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.preallocateObjectIds(TEST_TENANT,
                        List.of(ObjectType.MODEL, ObjectType.MODEL),
                        List.of(id1, id2)))
                .thenCompose(x -> dal.savePreallocatedObjects(TEST_TENANT, List.of(multi1, multi2)));

        assertDoesNotThrow((ThrowingSupplier<Void>) future2::get);

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi1.getHeader().getObjectId()), 1, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, MetadataCodec.decode(multi2.getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, unwrap(result1));
        assertEquals(multi2, unwrap(result2));
    }

    @Test
    void testPreallocate_duplicate() throws Exception {

        var id1 = UUID.randomUUID();

        unwrap(dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id1));
        assertThrows(EDuplicateItem.class,
                () -> unwrap(dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id1)));

        var obj2 = dummyTagForObjectType(ObjectType.MODEL);
        var id2 = MetadataCodec.decode(obj2.getHeader().getObjectId());
        var id3 = UUID.randomUUID();

        unwrap(dal.saveNewObject(TEST_TENANT, obj2));
        assertThrows(EDuplicateItem.class,
                () -> unwrap(dal.preallocateObjectIds(TEST_TENANT,
                        List.of(ObjectType.MODEL, ObjectType.MODEL),
                        List.of(id2, id3))));
    }

    @Test
    void testPreallocate_missingObjectId() throws Exception {

        var obj1 = dummyTagForObjectType(ObjectType.MODEL);

        assertThrows(EMissingItem.class,
                () -> unwrap(dal.savePreallocatedObject(TEST_TENANT, obj1)));

        var obj2 = dummyTagForObjectType(ObjectType.DATA);
        var id2 = MetadataCodec.decode(obj2.getHeader().getObjectId());

        unwrap(dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id2));

        assertThrows(EMissingItem.class,
                () -> unwrap(dal.savePreallocatedObjects(TEST_TENANT, List.of(obj1, obj2))));
    }

    @Test
    void testPreallocate_wrongObjectType() throws Exception {

        var obj1 = dummyTagForObjectType(ObjectType.MODEL);
        var id1 = MetadataCodec.decode(obj1.getHeader().getObjectId());

        unwrap(dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id1));

        assertThrows(EWrongItemType.class,
                () -> unwrap(dal.savePreallocatedObject(TEST_TENANT, obj1)));

        var obj2 = dummyTagForObjectType(ObjectType.DATA);
        var id2 = MetadataCodec.decode(obj2.getHeader().getObjectId());

        unwrap(dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id2));

        assertThrows(EWrongItemType.class,
                () -> unwrap(dal.savePreallocatedObjects(TEST_TENANT, List.of(obj1, obj2))));
    }
}
