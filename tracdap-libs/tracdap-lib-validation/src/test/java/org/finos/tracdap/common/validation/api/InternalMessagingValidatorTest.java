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

package org.finos.tracdap.common.validation.api;

import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.api.internal.InternalMessagingProto;
import org.finos.tracdap.api.internal.PlatformConfigUpdate;
import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.PlatformConfigEntry;

import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Regression test for InternalMessagingValidator.platformConfigUpdate(). Uses
 * Validator.validateFixedMethod() - the real entry point the gRPC validation interceptor calls -
 * rather than invoking the static validator method directly, so this exercises the actual
 * registration lookup. Before this validator existed, every call to this RPC failed with
 * "Required validator is not registered", regardless of message content, since the method had no
 * matching @Validator registration at all (unlike its sibling RPC, configUpdate).
 */
class InternalMessagingValidatorTest {

    private static Validator validator;
    private static Descriptors.MethodDescriptor methodDescriptor;

    @BeforeAll
    static void setup() {

        validator = new Validator();

        var serviceDescriptor = InternalMessagingProto.getDescriptor().findServiceByName("InternalMessagingApi");
        methodDescriptor = serviceDescriptor.findMethodByName("platformConfigUpdate");
    }

    private static PlatformConfigEntry validEntry() {

        return PlatformConfigEntry.newBuilder()
                .setConfigClass("license")
                .setConfigKey("license")
                .build();
    }

    @Test
    void validUpdate_passes() {

        var update = PlatformConfigUpdate.newBuilder()
                .setUpdateType(ConfigUpdateType.UPDATE)
                .setConfigEntry(validEntry())
                .build();

        Assertions.assertDoesNotThrow(() -> validator.validateFixedMethod(update, methodDescriptor));
    }

    @Test
    void missingUpdateType_isRejected() {

        var update = PlatformConfigUpdate.newBuilder()
                .setConfigEntry(validEntry())
                .build();

        Assertions.assertThrows(EInputValidation.class, () -> validator.validateFixedMethod(update, methodDescriptor));
    }

    @Test
    void missingConfigEntry_isRejected() {

        var update = PlatformConfigUpdate.newBuilder()
                .setUpdateType(ConfigUpdateType.UPDATE)
                .build();

        Assertions.assertThrows(EInputValidation.class, () -> validator.validateFixedMethod(update, methodDescriptor));
    }

    @Test
    void invalidConfigEntry_isRejected() {

        var update = PlatformConfigUpdate.newBuilder()
                .setUpdateType(ConfigUpdateType.UPDATE)
                .setConfigEntry(PlatformConfigEntry.newBuilder().build())
                .build();

        Assertions.assertThrows(EInputValidation.class, () -> validator.validateFixedMethod(update, methodDescriptor));
    }
}
