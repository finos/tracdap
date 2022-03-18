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

package org.finos.tracdap.common.validation.fixed;

import com.google.protobuf.Message;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.BasicType;
import org.finos.tracdap.metadata.TypeDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


class TypeSystemTest {

    static Validator validator;

    @BeforeAll
    static void setupValidator() {

        validator = new Validator();
    }

    static <TMsg extends Message> void expectValid(TMsg msg) {

        Assertions.assertDoesNotThrow(
                () -> validator.validateFixedObject(msg),
                "Validation failure for expected valid message");
    }

    @Test
    void typeDescriptor_okPrimitive() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .build());
    }

    @Test
    void typeDescriptor_okArray() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING))
                .build());
    }

    @Test
    void typeDescriptor_okArrayNested() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.INTEGER)))
                .build());
    }

    @Test
    void typeDescriptor_okMap() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING))
                .build());
    }

    @Test
    void typeDescriptor_okMapNested() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.INTEGER)))
                .build());
    }

    @Test
    void typeDescriptor_okMapOfArray() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)))
                .build());
    }
}
