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

package org.finos.tracdap.svc.meta.services;

import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.exception.EMetadataBadUpdate;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;


class TagUpdateServiceTest {

    @Test
    void createAttr_ok() {

        var baseTag = Tag.newBuilder().build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(42))
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeIntegerValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(42, attrValue);
    }

    @Test
    void createAttr_multiValue() {

        var baseTag = Tag.newBuilder().build();

        var arrayValue = MetadataCodec.encodeArrayValue(List.of(42, 43), TypeSystem.descriptor(BasicType.INTEGER));

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_ATTR)
                .setAttrName("attr_1")
                .setValue(arrayValue)
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeArrayValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(List.of(42L, 43L), attrValue);
    }

    @Test
    void createAttr_alreadyExists() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(42))
                .build();

        assertThrows(EMetadataBadUpdate.class, () ->
                TagUpdateService.applyTagUpdates(baseTag, List.of(update1)));

    }

    @Test
    void replaceAttr_ok() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.REPLACE_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(43))
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeIntegerValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(43, attrValue);
    }

    @Test
    void replaceAttr_doesNotExist() {

        var baseTag = Tag.newBuilder().build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.REPLACE_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(42))
                .build();

        assertThrows(EMetadataBadUpdate.class, () ->
                TagUpdateService.applyTagUpdates(baseTag, List.of(update1)));
    }

    @Test
    void replaceAttr_wrongType() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.REPLACE_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue("droids_you_are_looking_for"))
                .build();

        assertThrows(EMetadataBadUpdate.class, () ->
                TagUpdateService.applyTagUpdates(baseTag, List.of(update1)));
    }

    @Test
    void replaceAttr_singleToMulti() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var arrayValue = MetadataCodec.encodeArrayValue(List.of(42, 43), TypeSystem.descriptor(BasicType.INTEGER));

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.REPLACE_ATTR)
                .setAttrName("attr_1")
                .setValue(arrayValue)
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeArrayValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(List.of(42L, 43L), attrValue);

        var update2 = TagUpdate.newBuilder()
                .setOperation(TagOperation.REPLACE_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(42))
                .build();

        var updatedTag2 = TagUpdateService.applyTagUpdates(baseTag, List.of(update2));

        assertEquals(1, updatedTag2.getAttrsCount());
        assertTrue(updatedTag2.containsAttrs("attr_1"));

        var attrValue2 = MetadataCodec.decodeIntegerValue(updatedTag2.getAttrsOrThrow("attr_1"));
        assertEquals(42, attrValue2);
    }

    @Test
    void appendAttr_ok() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.APPEND_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(43))
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeArrayValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(List.of(42L, 43L), attrValue);
    }

    @Test
    void appendAttr_multiValue() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var arrayValue = MetadataCodec.encodeArrayValue(List.of(43, 44), TypeSystem.descriptor(BasicType.INTEGER));

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.APPEND_ATTR)
                .setAttrName("attr_1")
                .setValue(arrayValue)
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeArrayValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(List.of(42L, 43L, 44L), attrValue);
    }

    @Test
    void appendAttr_doesNotExist() {

        var baseTag = Tag.newBuilder()
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.APPEND_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(43))
                .build();

        assertThrows(EMetadataBadUpdate.class, () ->
                TagUpdateService.applyTagUpdates(baseTag, List.of(update1)));
    }

    @Test
    void appendAttr_wrongType() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.APPEND_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        assertThrows(EMetadataBadUpdate.class, () ->
                TagUpdateService.applyTagUpdates(baseTag, List.of(update1)));
    }

    @Test
    void deleteAttr_ok() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .putAttrs("attr_2", MetadataCodec.encodeValue("droids_you_are_looking_for"))
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.DELETE_ATTR)
                .setAttrName("attr_2")
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeIntegerValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(42, attrValue);
    }

    @Test
    void deleteAttr_doesNotExist() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.DELETE_ATTR)
                .setAttrName("attr_2")
                .build();

        assertThrows(EMetadataBadUpdate.class, () ->
                TagUpdateService.applyTagUpdates(baseTag, List.of(update1)));
    }

    @Test
    void clearAllAttr_ok() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .putAttrs("attr_2", MetadataCodec.encodeValue("droids_you_are_looking_for"))
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CLEAR_ALL_ATTR)
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(0, updatedTag.getAttrsCount());
    }

    @Test
    void clearAllAttr_controlledTags() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .putAttrs("attr_2", MetadataCodec.encodeValue("droids_you_are_looking_for"))
                .putAttrs("trac_controlled", MetadataCodec.encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CLEAR_ALL_ATTR)
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("trac_controlled"));

        var attrValue = MetadataCodec.decodeValue(updatedTag.getAttrsOrThrow("trac_controlled"));
        assertEquals("not_the_droids_you_are_looking_for", attrValue);
    }

    @Test
    void createOrReplace_new() {

        var baseTag = Tag.newBuilder().build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_OR_REPLACE_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(42))
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeIntegerValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(42, attrValue);
    }

    @Test
    void createOrReplace_existing() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_OR_REPLACE_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(43))
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeIntegerValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(43, attrValue);
    }

    @Test
    void createOrAppend_new() {

        var baseTag = Tag.newBuilder().build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_OR_APPEND_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(42))
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeIntegerValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(42, attrValue);
    }

    @Test
    void createOrAppend_existing() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_OR_APPEND_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(43))
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeArrayValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(List.of(42L, 43L), attrValue);
    }

    @Test
    void defaultOperation() {

        var baseTag = Tag.newBuilder().build();

        // Do not set an operation explicitly, should still create
        var update1 = TagUpdate.newBuilder()
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(42))
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeIntegerValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(42, attrValue);

        // Do not set an operation explicitly, should replace if attr already exists
        var update2 = TagUpdate.newBuilder()
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue("the_droids_you_are_looking_for"))
                .build();

        var updatedTag2 = TagUpdateService.applyTagUpdates(baseTag, List.of(update2));

        assertEquals(1, updatedTag2.getAttrsCount());
        assertTrue(updatedTag2.containsAttrs("attr_1"));

        var attrValue2 = MetadataCodec.decodeValue(updatedTag2.getAttrsOrThrow("attr_1"));
        assertEquals("the_droids_you_are_looking_for", attrValue2);
    }

    @Test
    void sequencing_unrelatedOperations() {

        var baseTag = Tag.newBuilder().build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_OR_APPEND_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(42))
                .build();

        var update2 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_OR_APPEND_ATTR)
                .setAttrName("attr_2")
                .setValue(MetadataCodec.encodeValue("the_droids_you_are_looking_for"))
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1, update2));

        assertEquals(2, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));
        assertTrue(updatedTag.containsAttrs("attr_2"));

        var attrValue1 = MetadataCodec.decodeIntegerValue(updatedTag.getAttrsOrThrow("attr_1"));
        var attrValue2 = MetadataCodec.decodeValue(updatedTag.getAttrsOrThrow("attr_2"));
        assertEquals(42, attrValue1);
        assertEquals("the_droids_you_are_looking_for", attrValue2);
    }

    @Test
    void sequencing_deleteRecreate() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var createOp = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(43))
                .build();

        var deleteOp = TagUpdate.newBuilder()
                .setOperation(TagOperation.DELETE_ATTR)
                .setAttrName("attr_1")
                .build();

        // Create should fail when attr already exists
        assertThrows(EMetadataBadUpdate.class, () ->
                TagUpdateService.applyTagUpdates(baseTag, List.of(createOp)));

        // Applying delete then recreate in one operation should succeed
        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(deleteOp, createOp));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeIntegerValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(43, attrValue);
    }

    @Test
    void sequencing_duplicateCreateFails() {

        var baseTag = Tag.newBuilder()
                .build();

        var createOp = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(42))
                .build();

        // Duplicate create op should fail
        assertThrows(EMetadataBadUpdate.class, () ->
                TagUpdateService.applyTagUpdates(baseTag, List.of(createOp, createOp)));

        // Retry a single create op - should succeed
        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(createOp));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeIntegerValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(42, attrValue);
    }

    @Test
    void sequencing_createAppend() {

        var baseTag = Tag.newBuilder()
                .build();

        var createOp = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(42))
                .build();

        var appendOp = TagUpdate.newBuilder()
                .setOperation(TagOperation.APPEND_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(43))
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(createOp, appendOp));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeArrayValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(List.of(42L, 43L), attrValue);
    }

    @Test
    void sequencing_appendMultiple() {

        var baseTag = Tag.newBuilder()
                .putAttrs("attr_1", MetadataCodec.encodeValue(42))
                .build();

        var arrayValue = MetadataCodec.encodeArrayValue(List.of(43, 44), TypeSystem.descriptor(BasicType.INTEGER));

        var appendOp1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.APPEND_ATTR)
                .setAttrName("attr_1")
                .setValue(arrayValue)
                .build();

        var appendOp2 = TagUpdate.newBuilder()
                .setOperation(TagOperation.APPEND_ATTR)
                .setAttrName("attr_1")
                .setValue(MetadataCodec.encodeValue(45))
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(appendOp1, appendOp2));

        assertEquals(1, updatedTag.getAttrsCount());
        assertTrue(updatedTag.containsAttrs("attr_1"));

        var attrValue = MetadataCodec.decodeArrayValue(updatedTag.getAttrsOrThrow("attr_1"));
        assertEquals(List.of(42L, 43L, 44L, 45L), attrValue);
    }

    @Test
    void normalization_ok() {

        var baseTag = Tag.newBuilder().build();

        var nonNormalValue = Value.newBuilder()
                .setIntegerValue(42)
                .build();

        var update1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_ATTR)
                .setAttrName("attr_1")
                .setValue(nonNormalValue)
                .build();

        var updatedTag = TagUpdateService.applyTagUpdates(baseTag, List.of(update1));

        var storedValue = updatedTag.getAttrsOrThrow("attr_1");
        assertTrue(storedValue.hasType());
        assertEquals(BasicType.INTEGER, storedValue.getType().getBasicType());

        var update2 = TagUpdate.newBuilder()
                .setOperation(TagOperation.REPLACE_ATTR)
                .setAttrName("attr_1")
                .setValue(nonNormalValue)
                .build();

        var updatedTag2 = TagUpdateService.applyTagUpdates(updatedTag, List.of(update2));

        var storedValue2 = updatedTag2.getAttrsOrThrow("attr_1");
        assertTrue(storedValue2.hasType());
        assertEquals(BasicType.INTEGER, storedValue2.getType().getBasicType());

        var update3 = TagUpdate.newBuilder()
                .setOperation(TagOperation.APPEND_ATTR)
                .setAttrName("attr_1")
                .setValue(nonNormalValue)
                .build();

        var updatedTag3 = TagUpdateService.applyTagUpdates(updatedTag2, List.of(update3));

        var storedValue3 = updatedTag3.getAttrsOrThrow("attr_1");
        assertTrue(storedValue3.hasType());
        assertEquals(BasicType.ARRAY, storedValue3.getType().getBasicType());
        assertEquals(BasicType.INTEGER, storedValue3.getType().getArrayType().getBasicType());
    }
}
