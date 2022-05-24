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

package org.finos.tracdap.common.validation.test;

import com.google.protobuf.Message;
import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.validation.Validator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

public class BaseValidatorTest {

    private static Validator validator;

    @BeforeAll
    static void setupValidator() {

        validator = new Validator();
    }

    protected static <TMsg extends Message> void expectValid(TMsg msg) {

        Assertions.assertDoesNotThrow(
                () -> validator.validateFixedObject(msg),
                "Validation failed for a valid message");
    }

    protected static <TMsg extends Message> void expectInvalid(TMsg msg) {

        Assertions.assertThrows(EInputValidation.class,
                () -> validator.validateFixedObject(msg),
                "Validation passed for an invalid message");
    }
}
