/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.data.api;

import com.accenture.trac.common.metadata.MetadataUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static com.accenture.trac.svc.data.api.DataApiTest_File.BASIC_CREATE_FILE_REQUEST;
import static com.accenture.trac.svc.data.api.DataApiTest_File.BASIC_UPDATE_FILE_REQUEST;
import static com.accenture.trac.svc.data.api.Helpers.readRequest;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.resultOf;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DataApiTest_Tenancy extends DataApiTest_Base {


    @Test
    void testCreateFile_tenancy() {

        var noTenant = BASIC_CREATE_FILE_REQUEST.toBuilder().clearTenant().build();
        var noTenantResult = Helpers.clientStreaming(dataClient::createFile, noTenant);
        waitFor(TEST_TIMEOUT, noTenantResult);
        var noTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(noTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, noTenantError.getStatus().getCode());

        var invalidTenant = BASIC_CREATE_FILE_REQUEST.toBuilder().setTenant("£$%^**!\0\n/`¬").build();
        var invalidTenantResult = Helpers.clientStreaming(dataClient::createFile, invalidTenant);
        waitFor(TEST_TIMEOUT, invalidTenantResult);
        var invalidTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalidTenantError.getStatus().getCode());

        var unknownTenant = BASIC_CREATE_FILE_REQUEST.toBuilder().setTenant("UNKNOWN").build();
        var unknownTenantResult = Helpers.clientStreaming(dataClient::createFile, unknownTenant);
        waitFor(TEST_TIMEOUT, unknownTenantResult);
        var unknownTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(unknownTenantResult));
        assertEquals(Status.Code.NOT_FOUND, unknownTenantError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_tenancy() throws Exception {

        // Create a valid v1

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        // Set up an update request using the valid v1 prior version

        var v1Selector = MetadataUtil.selectorFor(v1Id);
        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();

        // No do the tenancy checks on updateFile

        var noTenant = updateRequest.toBuilder().clearTenant().build();
        var noTenantResult = Helpers.clientStreaming(dataClient::updateFile, noTenant);
        waitFor(TEST_TIMEOUT, noTenantResult);
        var noTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(noTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, noTenantError.getStatus().getCode());

        var invalidTenant = updateRequest.toBuilder().setTenant("£$%^**!\0\n/`¬").build();
        var invalidTenantResult = Helpers.clientStreaming(dataClient::updateFile, invalidTenant);
        waitFor(TEST_TIMEOUT, invalidTenantResult);
        var invalidTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalidTenantError.getStatus().getCode());

        var unknownTenant = updateRequest.toBuilder().setTenant("UNKNOWN").build();
        var unknownTenantResult = Helpers.clientStreaming(dataClient::updateFile, unknownTenant);
        waitFor(TEST_TIMEOUT, unknownTenantResult);
        var unknownTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(unknownTenantResult));
        assertEquals(Status.Code.NOT_FOUND, unknownTenantError.getStatus().getCode());
    }

    @Test
    void testReadFile_tenancy() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var readRequest = readRequest(v1Id);

        var noTenant = readRequest.toBuilder().clearTenant().build();
        var noTenantResult= Helpers.serverStreamingDiscard(dataClient::readFile, noTenant, execContext);
        waitFor(TEST_TIMEOUT, noTenantResult);
        var noTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(noTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, noTenantError.getStatus().getCode());

        var invalidTenant = readRequest.toBuilder().setTenant("£$%^**!\0\n/`¬").build();
        var invalidTenantResult= Helpers.serverStreamingDiscard(dataClient::readFile, invalidTenant, execContext);
        waitFor(TEST_TIMEOUT, invalidTenantResult);
        var invalidTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalidTenantError.getStatus().getCode());

        var unknownTenant = readRequest.toBuilder().setTenant("UNKNOWN").build();
        var unknownTenantResult = Helpers.serverStreamingDiscard(dataClient::readFile, unknownTenant, execContext);
        waitFor(TEST_TIMEOUT, unknownTenantResult);
        var unknownTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(unknownTenantResult));
        assertEquals(Status.Code.NOT_FOUND, unknownTenantError.getStatus().getCode());
    }
}
