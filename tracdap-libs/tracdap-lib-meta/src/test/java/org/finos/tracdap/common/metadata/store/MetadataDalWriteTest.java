/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.metadata.store;

import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.metadata.TagSelector;
import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.exception.EMetadataWrongType;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.finos.tracdap.common.metadata.test.IMetadataStoreTest;
import org.finos.tracdap.common.metadata.test.JdbcUnit;
import org.finos.tracdap.common.metadata.test.JdbcIntegration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.finos.tracdap.test.meta.SampleMetadata.*;
import static org.junit.jupiter.api.Assertions.*;


abstract class MetadataDalWriteTest implements IMetadataStoreTest {

    private IMetadataStore store;

    public void setStore(IMetadataStore store) {
        this.store = store;
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

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 1);

        assertEquals(origTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 1);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, result1);
        assertEquals(multi2, result2);
    }

    @Test
    void testSaveNewObject_multiValuedAttr() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = addMultiValuedAttr(dummyTag(origDef, INCLUDE_HEADER));
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 1);

        assertEquals(origTag, result);

        // Save multiple
        var multi1 = addMultiValuedAttr(dummyTagForObjectType(ObjectType.DATA));
        var multi2 = addMultiValuedAttr(dummyTagForObjectType(ObjectType.MODEL));

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 1);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, result1);
        assertEquals(multi2, result2);
    }

    @Test
    void testSaveNewObject_duplicate() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        assertThrows(EMetadataDuplicate.class, () -> store.saveNewObjects(TEST_TENANT, List.of(origTag, origTag)));
        assertThrows(EMetadataNotFound.class, () -> store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        // First insert should succeed if they are run one by one
        assertDoesNotThrow(() -> store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag)));
        assertDoesNotThrow(() -> store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 1));
        assertThrows(EMetadataDuplicate.class, () -> store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag)));
    }

    @Test
    void testSaveNewVersion_ok() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef);
        var nextTag = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 1);

        assertEquals(nextTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = tagForNextObject(multi1, nextDataDef(multi1.getDefinition()), INCLUDE_HEADER);
        var multi2v2 = tagForNextObject(multi2, nextModelDef(multi2.getDefinition()), INCLUDE_HEADER);

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        store.saveNewVersions(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 2, 1);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 2, 1);

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

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 1);

        assertEquals(nextTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = addMultiValuedAttr(tagForNextObject(multi1, nextDataDef(multi1.getDefinition()), INCLUDE_HEADER));
        var multi2v2 = addMultiValuedAttr(tagForNextObject(multi2, nextModelDef(multi2.getDefinition()), INCLUDE_HEADER));

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        store.saveNewVersions(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 2, 1);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 2, 1);

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

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataDuplicate.class, () -> store.saveNewVersions(TEST_TENANT, List.of(nextTag, nextTag)));
        assertThrows(EMetadataNotFound.class, () -> store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 1));

        // First insert should succeed if they are run one by one
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag));
        assertThrows(EMetadataDuplicate.class, () -> store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag)));

        var loadDup2 = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 1);
        assertEquals(nextTag, loadDup2);
    }

    @Test
    void testSaveNewVersion_missingObject() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDef = nextDataDef(origDef);
        var nextTag = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);

        // Save next version, single, without saving original
        assertThrows(EMetadataNotFound.class, () -> store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag)));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);

        // Save next, multiple, one item does not have original
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataNotFound.class, () -> store.saveNewVersions(TEST_TENANT, List.of(nextTag, nextModelTag)));
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
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(dataTag));

        assertThrows(EMetadataWrongType.class, () -> store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextModelTag)));
        assertThrows(EMetadataWrongType.class, () -> store.saveNewVersions(TEST_TENANT, List.of(nextDataTag, nextModelTag)));
    }

    @Test
    void testSaveNewTag_ok() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        // Test saving tag v2 against object v2
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextDefTag1));
        store.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 2);

        assertEquals(nextDefTag2, result);

        // Save multiple - this test saves tag v2 against object v1
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = nextTag(multi1, UPDATE_TAG_VERSION);
        var multi2v2 = nextTag(multi2, UPDATE_TAG_VERSION);

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        store.saveNewTags(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 2);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 2);

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
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextDefTag1));
        store.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 2);

        assertEquals(nextDefTag2, result);

        // Save multiple - this test saves tag v2 against object v1
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = addMultiValuedAttr(nextTag(multi1, UPDATE_TAG_VERSION));
        var multi2v2 = addMultiValuedAttr(nextTag(multi2, UPDATE_TAG_VERSION));

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        store.saveNewTags(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 2);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 2);

        assertEquals(multi1v2, result1);
        assertEquals(multi2v2, result2);
    }

    @Test
    void testSaveNewTag_duplicate() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataDuplicate.class, () -> store.saveNewTags(TEST_TENANT, List.of(nextTag, nextTag)));
        assertThrows(EMetadataNotFound.class, () -> store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 2));

        // First insert should succeed if they are run one by one
        store.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag));
        assertThrows(EMetadataDuplicate.class, () -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag)));

        var loadDup2 = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 2);
        assertEquals(nextTag, loadDup2);
    }

    @Test
    void testSaveNewTag_missingObject() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION);

        // Save next tag, single, without saving object
        assertThrows(EMetadataNotFound.class, () -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag)));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);

        // Save next tag, multiple, one item does not have an object
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataNotFound.class, () -> store.saveNewTags(TEST_TENANT, List.of(nextTag, nextModelTag)));
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
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        assertThrows(EMetadataNotFound.class, () -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag1)));
        assertThrows(EMetadataNotFound.class, () -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2)));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);
        var nextModelTag2 = nextTag(nextModelTag, UPDATE_TAG_VERSION);

        // Save object 1 version 2, and object 2 version 1
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextDefTag1));
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(modelTag));

        // Save next tag (multiple), second item is missing the required object version
        assertThrows(EMetadataNotFound.class, () -> store.saveNewTags(TEST_TENANT, List.of(nextDefTag2, nextModelTag2)));

        // Saving the valid tag by itself should not throw
        Assertions.assertDoesNotThrow(() -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2)));
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

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataWrongType.class, () -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag)));
        assertThrows(EMetadataNotFound.class, () -> store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 2));

        var origDef2 = dummyModelDef();
        var origTag2 = dummyTag(origDef2, INCLUDE_HEADER);
        var origId2 = UUID.fromString(origTag2.getHeader().getObjectId());

        var nextTag2 = nextTag(origTag2, UPDATE_TAG_VERSION);

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag2));

        assertThrows(EMetadataWrongType.class, () -> store.saveNewTags(TEST_TENANT, List.of(nextTag, nextTag2)));

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString())
                .setObjectVersion(1)
                .setTagVersion(2)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(origId2.toString())
                .setObjectVersion(1)
                .setTagVersion(2)
                .build();

        assertThrows(EMetadataNotFound.class, () -> store.loadObjects(TEST_TENANT, List.of(selector1, selector2)));
    }

    @Test
    void testPreallocate_ok() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);

        store.savePreallocatedIds(TEST_TENANT, List.of(origTag.getHeader()));
        store.savePreallocatedObjects(TEST_TENANT, Collections.singletonList(origTag));
        var result = store.loadObject(TEST_TENANT, selectorForTag(origTag));

        assertEquals(origTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.MODEL);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

       store.savePreallocatedIds(TEST_TENANT, List.of(multi1.getHeader(), multi2.getHeader()));
       store.savePreallocatedObjects(TEST_TENANT, List.of(multi1, multi2));

        var result1 = store.loadObject(TEST_TENANT, selectorForTag(multi1));
        var result2 = store.loadObject(TEST_TENANT, selectorForTag(multi2));

        assertEquals(multi1, result1);
        assertEquals(multi2, result2);
    }

    @Test
    void testPreallocate_duplicate() {

        var id1 = UUID.randomUUID();
        var header1 = TagHeader.newBuilder().setObjectType(ObjectType.DATA).setObjectId(id1.toString()).build();

        store.savePreallocatedIds(TEST_TENANT, List.of(header1));

        assertThrows(EMetadataDuplicate.class, () ->
                store.savePreallocatedIds(TEST_TENANT, List.of(header1)));

        var obj2 = dummyTagForObjectType(ObjectType.MODEL);
        var id3 = UUID.randomUUID();
        var header3 = TagHeader.newBuilder().setObjectType(ObjectType.DATA).setObjectId(id3.toString()).build();

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(obj2));

        assertThrows(EMetadataDuplicate.class, () ->
                store.savePreallocatedIds(TEST_TENANT, List.of(obj2.getHeader(), header3)));
    }

    @Test
    void testPreallocate_missingObjectId() {

        var obj1 = dummyTagForObjectType(ObjectType.MODEL);

        assertThrows(EMetadataNotFound.class,
                () -> store.savePreallocatedObjects(TEST_TENANT, Collections.singletonList(obj1)));

        var obj2 = dummyTagForObjectType(ObjectType.DATA);

        store.savePreallocatedIds(TEST_TENANT, List.of(obj2.getHeader()));

        assertThrows(EMetadataNotFound.class,
                () -> store.savePreallocatedObjects(TEST_TENANT, List.of(obj1, obj2)));
    }

    @Test
    void testPreallocate_wrongObjectType() {

        var obj1 = dummyTagForObjectType(ObjectType.MODEL);
        var wrongHeader = obj1.getHeader().toBuilder().setObjectType(ObjectType.DATA).build();

        store.savePreallocatedIds(TEST_TENANT, List.of(wrongHeader));

        assertThrows(EMetadataWrongType.class, () ->
                store.savePreallocatedObjects(TEST_TENANT, Collections.singletonList(obj1)));

        var obj2 = dummyTagForObjectType(ObjectType.DATA);

        store.savePreallocatedIds(TEST_TENANT, List.of(obj2.getHeader()));

        assertThrows(EMetadataWrongType.class, () ->
                store.savePreallocatedObjects(TEST_TENANT, List.of(obj1, obj2)));
    }
}
