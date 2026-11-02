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

import org.finos.tracdap.api.AdminServiceProto;
import org.finos.tracdap.api.CheckTenantStorageRequest;
import org.finos.tracdap.api.CreateTenantRequest;
import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.validation.Validator;

import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


class TenantAdminValidatorTest {

    private static Validator validator;
    private static Descriptors.MethodDescriptor createTenant;
    private static Descriptors.MethodDescriptor checkTenantStorage;

    @BeforeAll
    static void setup() {

        validator = new Validator();

        var serviceDescriptor = AdminServiceProto.getDescriptor().findServiceByName("TracAdminApi");
        createTenant = serviceDescriptor.findMethodByName("createTenant");
        checkTenantStorage = serviceDescriptor.findMethodByName("checkTenantStorage");
    }

    @ParameterizedTest
    @ValueSource(strings = {"TENANT_1", "acme", "_T1", "SIXTEEN_CHARS_16"})
    void createTenant_validCode(String tenantCode) {

        var request = CreateTenantRequest.newBuilder()
                .setTenantCode(tenantCode)
                .setDisplayName("Production 1")
                .build();

        Assertions.assertDoesNotThrow(() -> validator.validateFixedMethod(request, createTenant));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "TENANT-1", "1TENANT", "TENANT 1", "SEVENTEEN_CHARS17"})
    void createTenant_invalidCode(String tenantCode) {

        var request = CreateTenantRequest.newBuilder()
                .setTenantCode(tenantCode)
                .setDisplayName("Production 1")
                .build();

        Assertions.assertThrows(EInputValidation.class, () -> validator.validateFixedMethod(request, createTenant));
    }

    @Test
    void createTenant_displayNameRequired() {

        var request = CreateTenantRequest.newBuilder()
                .setTenantCode("TENANT_1")
                .build();

        Assertions.assertThrows(EInputValidation.class, () -> validator.validateFixedMethod(request, createTenant));
    }

    @Test
    void checkTenantStorage_codeValidated() {

        var valid = CheckTenantStorageRequest.newBuilder().setTenantCode("TENANT_1").build();
        var invalid = CheckTenantStorageRequest.newBuilder().setTenantCode("TENANT-1").build();
        var missing = CheckTenantStorageRequest.newBuilder().build();

        Assertions.assertDoesNotThrow(() -> validator.validateFixedMethod(valid, checkTenantStorage));
        Assertions.assertThrows(EInputValidation.class, () -> validator.validateFixedMethod(invalid, checkTenantStorage));
        Assertions.assertThrows(EInputValidation.class, () -> validator.validateFixedMethod(missing, checkTenantStorage));
    }
}
