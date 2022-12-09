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

import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.exception.EMetadataWrongType;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.finos.tracdap.test.meta.IDalTestable;
import org.finos.tracdap.test.meta.JdbcUnit;
import org.finos.tracdap.test.meta.JdbcIntegration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.finos.tracdap.test.meta.TestData.*;
import static org.junit.jupiter.api.Assertions.*;


abstract class MetadataDalWriteTest implements IDalTestable {

    private IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }

    @ExtendWith(JdbcUnit.class)
    static class UnitTest extends MetadataDalWriteTest {}

    @Tag("integration")
    @Tag("int-metadb")
    @ExtendWith(JdbcIntegration.class)
    static class IntegrationTest extends MetadataDalWriteTest {}

    @Test
    void testSaveNewObject_ok() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        var result = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1);

        assertEquals(origTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, result1);
        assertEquals(multi2, result2);
    }

    @Test
    void testSaveNewObject_multiValuedAttr() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = addMultiValuedAttr(dummyTag(origDef, INCLUDE_HEADER));
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        var result = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1);

        assertEquals(origTag, result);

        // Save multiple
        var multi1 = addMultiValuedAttr(dummyTagForObjectType(ObjectType.DATA));
        var multi2 = addMultiValuedAttr(dummyTagForObjectType(ObjectType.MODEL));

        dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, result1);
        assertEquals(multi2, result2);
    }

    @Test
    void testSaveNewObject_duplicate() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        assertThrows(EMetadataDuplicate.class, () -> dal.saveNewObjects(TEST_TENANT, List.of(origTag, origTag)));
        assertThrows(EMetadataNotFound.class, () -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        // First insert should succeed if they are run one by one
        assertDoesNotThrow(() -> dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag)));
        assertDoesNotThrow(() -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));
        assertThrows(EMetadataDuplicate.class, () -> dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag)));
    }

    @Test
    void testSaveNewVersion_ok() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef);
        var nextTag = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);

        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        dal.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag));
        var result = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1);

        assertEquals(nextTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = tagForNextObject(multi1, nextDataDef(multi1.getDefinition()), INCLUDE_HEADER);
        var multi2v2 = tagForNextObject(multi2, nextModelDef(multi2.getDefinition()), INCLUDE_HEADER);

        dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        dal.saveNewVersions(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 2, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 2, 1);

        assertEquals(multi1v2, result1);
        assertEquals(multi2v2, result2);
    }

    @Test
    void testSaveNewVersion_multiValuedAttr() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef);
        var nextTag = addMultiValuedAttr(tagForNextObject(origTag, nextDef, INCLUDE_HEADER));

        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        dal.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag));
        var result = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1);

        assertEquals(nextTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = addMultiValuedAttr(tagForNextObject(multi1, nextDataDef(multi1.getDefinition()), INCLUDE_HEADER));
        var multi2v2 = addMultiValuedAttr(tagForNextObject(multi2, nextModelDef(multi2.getDefinition()), INCLUDE_HEADER));

        dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        dal.saveNewVersions(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 2, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 2, 1);

        assertEquals(multi1v2, result1);
        assertEquals(multi2v2, result2);
    }

    @Test
    void testSaveNewVersion_duplicate() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef);
        var nextTag = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);

        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataDuplicate.class, () -> dal.saveNewVersions(TEST_TENANT, List.of(nextTag, nextTag)));
        assertThrows(EMetadataNotFound.class, () -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1));

        // First insert should succeed if they are run one by one
        dal.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag));
        assertThrows(EMetadataDuplicate.class, () -> dal.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag)));

        var loadDup2 = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 1);
        assertEquals(nextTag, loadDup2);
    }

    @Test
    void testSaveNewVersion_missingObject() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDef = nextDataDef(origDef);
        var nextTag = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);

        // Save next version, single, without saving original
        assertThrows(EMetadataNotFound.class, () -> dal.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag)));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);

        // Save next, multiple, one item does not have original
        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataNotFound.class, () -> dal.saveNewVersions(TEST_TENANT, List.of(nextTag, nextModelTag)));
    }

    @Test
    void testSaveNewVersion_wrongObjectType() {

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
        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(dataTag));

        assertThrows(EMetadataWrongType.class, () -> dal.saveNewVersions(TEST_TENANT, Collections.singletonList(nextModelTag)));
        assertThrows(EMetadataWrongType.class, () -> dal.saveNewVersions(TEST_TENANT, List.of(nextDataTag, nextModelTag)));
    }

    @Test
    void testSaveNewTag_ok() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        // Test saving tag v2 against object v2
        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        dal.saveNewVersions(TEST_TENANT, Collections.singletonList(nextDefTag1));
        dal.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2));
        var result = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 2);

        assertEquals(nextDefTag2, result);

        // Save multiple - this test saves tag v2 against object v1
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = nextTag(multi1, UPDATE_TAG_VERSION);
        var multi2v2 = nextTag(multi2, UPDATE_TAG_VERSION);

        dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        dal.saveNewTags(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 2);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 2);

        assertEquals(multi1v2, result1);
        assertEquals(multi2v2, result2);
    }

    @Test
    void testSaveNewTag_multiValuedAttr() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = addMultiValuedAttr(nextTag(nextDefTag1, UPDATE_TAG_VERSION));
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        // Test saving tag v2 against object v2
        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        dal.saveNewVersions(TEST_TENANT, Collections.singletonList(nextDefTag1));
        dal.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2));
        var result = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 2, 2);

        assertEquals(nextDefTag2, result);

        // Save multiple - this test saves tag v2 against object v1
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = addMultiValuedAttr(nextTag(multi1, UPDATE_TAG_VERSION));
        var multi2v2 = addMultiValuedAttr(nextTag(multi2, UPDATE_TAG_VERSION));

        dal.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        dal.saveNewTags(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 2);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 2);

        assertEquals(multi1v2, result1);
        assertEquals(multi2v2, result2);
    }

    @Test
    void testSaveNewTag_duplicate() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataDuplicate.class, () -> dal.saveNewTags(TEST_TENANT, List.of(nextTag, nextTag)));
        assertThrows(EMetadataNotFound.class, () -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2));

        // First insert should succeed if they are run one by one
        dal.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag));
        assertThrows(EMetadataDuplicate.class, () -> dal.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag)));

        var loadDup2 = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2);
        assertEquals(nextTag, loadDup2);
    }

    @Test
    void testSaveNewTag_missingObject() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION);

        // Save next tag, single, without saving object
        assertThrows(EMetadataNotFound.class, () -> dal.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag)));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);

        // Save next tag, multiple, one item does not have an object
        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataNotFound.class, () -> dal.saveNewTags(TEST_TENANT, List.of(nextTag, nextModelTag)));
    }

    @Test
    void testSaveNewTag_missingVersion() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDef = nextDataDef(origDef);
        var nextDefTag1 = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);

        // Save next tag (single) on an unknown object version
        // This is an error, even for tag v1
        // saveNewVersion must be called first
        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        assertThrows(EMetadataNotFound.class, () -> dal.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag1)));
        assertThrows(EMetadataNotFound.class, () -> dal.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2)));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);
        var nextModelTag2 = nextTag(nextModelTag, UPDATE_TAG_VERSION);

        // Save object 1 version 2, and object 2 version 1
        dal.saveNewVersions(TEST_TENANT, Collections.singletonList(nextDefTag1));
        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(modelTag));

        // Save next tag (multiple), second item is missing the required object version
        assertThrows(EMetadataNotFound.class, () -> dal.saveNewTags(TEST_TENANT, List.of(nextDefTag2, nextModelTag2)));

        // Saving the valid tag by itself should not throw
        Assertions.assertDoesNotThrow(() -> dal.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2)));
    }

    @Test
    void testSaveNewTag_wrongObjectType() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

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

        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataWrongType.class, () -> dal.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag)));
        assertThrows(EMetadataNotFound.class, () -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 2));

        var origDef2 = dummyModelDef();
        var origTag2 = dummyTag(origDef2, INCLUDE_HEADER);
        var origId2 = UUID.fromString(origTag2.getHeader().getObjectId());

        var nextTag2 = nextTag(origTag2, UPDATE_TAG_VERSION);

        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag2));

        assertThrows(EMetadataWrongType.class, () -> dal.saveNewTags(TEST_TENANT, List.of(nextTag, nextTag2)));
        assertThrows(EMetadataNotFound.class, () -> dal.loadTags(TEST_TENANT,
                List.of(ObjectType.DATA, ObjectType.MODEL),
                List.of(origId, origId2),
                List.of(1, 1),
                List.of(2, 2)));
    }

    @Test
    void testPreallocate_ok() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, origId);
        dal.savePreallocatedObject(TEST_TENANT, origTag);
        var result = dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1);

        assertEquals(origTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.MODEL);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);
        var id1 = UUID.fromString(multi1.getHeader().getObjectId());
        var id2 = UUID.fromString(multi2.getHeader().getObjectId());

       dal.preallocateObjectIds(TEST_TENANT,
                List.of(ObjectType.MODEL, ObjectType.MODEL),
                List.of(id1, id2));
       dal.savePreallocatedObjects(TEST_TENANT, List.of(multi1, multi2));

        var result1 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi1.getHeader().getObjectId()), 1, 1);
        var result2 = dal.loadTag(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, result1);
        assertEquals(multi2, result2);
    }

    @Test
    void testPreallocate_duplicate() {

        var id1 = UUID.randomUUID();

        dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id1);
        assertThrows(EMetadataDuplicate.class,
                () -> dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id1));

        var obj2 = dummyTagForObjectType(ObjectType.MODEL);
        var id2 = UUID.fromString(obj2.getHeader().getObjectId());
        var id3 = UUID.randomUUID();

        dal.saveNewObjects(TEST_TENANT, Collections.singletonList(obj2));
        assertThrows(EMetadataDuplicate.class,
                () -> dal.preallocateObjectIds(TEST_TENANT,
                        List.of(ObjectType.MODEL, ObjectType.MODEL),
                        List.of(id2, id3)));
    }

    @Test
    void testPreallocate_missingObjectId() {

        var obj1 = dummyTagForObjectType(ObjectType.MODEL);

        assertThrows(EMetadataNotFound.class,
                () -> dal.savePreallocatedObject(TEST_TENANT, obj1));

        var obj2 = dummyTagForObjectType(ObjectType.DATA);
        var id2 = UUID.fromString(obj2.getHeader().getObjectId());

        dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id2);

        assertThrows(EMetadataNotFound.class,
                () -> dal.savePreallocatedObjects(TEST_TENANT, List.of(obj1, obj2)));
    }

    @Test
    void testPreallocate_wrongObjectType() {

        var obj1 = dummyTagForObjectType(ObjectType.MODEL);
        var id1 = UUID.fromString(obj1.getHeader().getObjectId());

        dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id1);

        assertThrows(EMetadataWrongType.class,
                () -> dal.savePreallocatedObject(TEST_TENANT, obj1));

        var obj2 = dummyTagForObjectType(ObjectType.DATA);
        var id2 = UUID.fromString(obj2.getHeader().getObjectId());

        dal.preallocateObjectId(TEST_TENANT, ObjectType.DATA, id2);

        assertThrows(EMetadataWrongType.class,
                () -> dal.savePreallocatedObjects(TEST_TENANT, List.of(obj1, obj2)));
    }
}
