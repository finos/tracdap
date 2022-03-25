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

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.test.BaseValidatorTest;
import org.finos.tracdap.metadata.*;
import org.junit.jupiter.api.Test;


public class TagUpdateTest extends BaseValidatorTest {

    @Test
    void tagUpdate_basicOk() {

        var attr = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectValid(attr);
    }

    @Test
    void tagUpdate_nameMissing() {

        var attr = TagUpdate.newBuilder()
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectInvalid(attr);
    }

    @Test
    void tagUpdate_nameEmpty() {

        var attr = TagUpdate.newBuilder()
                .setAttrName("")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectInvalid(attr);
    }

    @Test
    void tagUpdate_nameInvalid() {

        var attr1 = TagUpdate.newBuilder()
                .setAttrName("123_starts_with_a_number")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectInvalid(attr1);

        var attr2 = TagUpdate.newBuilder()
                .setAttrName("contains_@$%_special_!%&*_chars")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectInvalid(attr2);

        var attr3 = TagUpdate.newBuilder()
                .setAttrName(" leading_space")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectInvalid(attr3);

        var attr4 = TagUpdate.newBuilder()
                .setAttrName("contains\r\nspace")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectInvalid(attr4);

        var attr5 = TagUpdate.newBuilder()
                .setAttrName("trailing_space\t")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectInvalid(attr5);
    }

    @Test
    void tagUpdate_nonAscii() {

        // For now attr names are identifiers, i.e. ascii words

        var attr = TagUpdate.newBuilder()
                .setAttrName("你好世界")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectInvalid(attr);
    }

    @Test
    void tagUpdate_nameReserved() {

        // An update to a reserved attr is a valid update as a stand-alone object
        // Whether it is allowed depends on context, i.e. who sent it and to what service / method
        // So, validation at the object level should succeed

        var attr1 = TagUpdate.newBuilder()
                .setAttrName("trac_attr_update")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectValid(attr1);

        var attr2 = TagUpdate.newBuilder()
                .setAttrName("_reserved_attr")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectValid(attr2);
    }

    @Test
    void tagUpdate_operationDefault() {

        // It is valid to omit the operation, default is CREATE_OR_REPLACE

        var attr = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectValid(attr);
    }

    @Test
    void tagUpdate_operationClear() {

        // CLEAR operations do not accept name or value

        var attr1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CLEAR_ALL_ATTR)
                .build();

        expectValid(attr1);

        var attr2 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CLEAR_ALL_ATTR)
                .setAttrName("some_attr")
                .build();

        expectInvalid(attr2);

        var attr3 = TagUpdate.newBuilder()
                .setOperation(TagOperation.CLEAR_ALL_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectInvalid(attr3);
    }

    @Test
    void tagUpdate_operationDelete() {

        // DELETE operations must have an attr name, but no value

        var attr1 = TagUpdate.newBuilder()
                .setOperation(TagOperation.DELETE_ATTR)
                .setAttrName("some_attr")
                .build();

        expectValid(attr1);

        var attr2 = TagUpdate.newBuilder()
                .setOperation(TagOperation.DELETE_ATTR)
                .build();

        expectInvalid(attr2);

        var attr3 = TagUpdate.newBuilder()
                .setOperation(TagOperation.DELETE_ATTR)
                .setAttrName("some_attr")
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        expectInvalid(attr3);
    }

    @Test
    void tagUpdate_valueMissing() {

        var attr = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setOperation(TagOperation.CREATE_ATTR)
                .build();

        expectInvalid(attr);
    }

    @Test
    void tagUpdate_valueExplicitNull() {

        // Null values are not allowed as attributes
        // Delete the attribute instead!

        var nullValue = Value.newBuilder()
                .setType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING))
                .build();

        expectValid(nullValue);

        var attr = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(nullValue)
                .build();

        expectInvalid(attr);
    }

    @Test
    void tagUpdate_valueInvalid() {

        var invalidValue = Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.STRING))
                .setIntegerValue(1)
                .build();

        var attr = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(invalidValue)
                .build();

        expectInvalid(attr);
    }

    @Test
    void tagUpdate_valueArray() {

        // arrays are valid for all create/replace/append operations

        var arrayType = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING))
                .build();

        var arrayValue = Value.newBuilder()
                .setType(arrayType)
                .setArrayValue(ArrayValue.newBuilder()
                .addItems(MetadataCodec.encodeValue("item_1"))
                .addItems(MetadataCodec.encodeValue("item_2")))
                .build();

        var attr1 = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(arrayValue)
                .build();

        expectValid(attr1);

        var attr2 = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(arrayValue)
                .build();

        expectValid(attr2);

        var attr3 = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setOperation(TagOperation.APPEND_ATTR)
                .setValue(arrayValue)
                .build();

        expectValid(attr3);

        var attr4 = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setOperation(TagOperation.CREATE_OR_REPLACE_ATTR)
                .setValue(arrayValue)
                .build();

        expectValid(attr4);

        var attr5 = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setOperation(TagOperation.CREATE_OR_APPEND_ATTR)
                .setValue(arrayValue)
                .build();

        expectValid(attr5);
    }

    @Test
    void tagUpdate_valueArrayNested() {

        // Nested arrays are not allowed

        var arrayType = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)))
                .build();

        var arrayValue = Value.newBuilder()
                .setType(arrayType)
                .setArrayValue(ArrayValue.newBuilder()
                .addItems(Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addItems(MetadataCodec.encodeValue("item_1"))
                        .addItems(MetadataCodec.encodeValue("item_2")))))
                .build();

        var attr1 = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(arrayValue)
                .build();

        expectInvalid(attr1);
    }

    @Test
    void tagUpdate_valueMap() {

        // Map values are not allowed

        var arrayType = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING))
                .build();

        var arrayValue = Value.newBuilder()
                .setType(arrayType)
                .setMapValue(MapValue.newBuilder()
                        .putEntries("key_1", MetadataCodec.encodeValue("item_1"))
                        .putEntries("key_2", MetadataCodec.encodeValue("item_2")))
                .build();

        var attr1 = TagUpdate.newBuilder()
                .setAttrName("some_attr")
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(arrayValue)
                .build();

        expectInvalid(attr1);
    }
}
