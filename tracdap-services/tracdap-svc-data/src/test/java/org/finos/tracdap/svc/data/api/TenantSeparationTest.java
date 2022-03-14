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

package org.finos.tracdap.svc.data.api;

import org.finos.tracdap.common.metadata.MetadataUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.finos.tracdap.svc.data.api.DataApiTestHelpers.readRequest;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.resultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TenantSeparationTest extends DataApiTestBase {

    @Test
    void testCreateFile_tenantOmitted() {

        var noTenant = FileOperationsTest.BASIC_CREATE_FILE_REQUEST.toBuilder().clearTenant().build();
        var noTenantResult = DataApiTestHelpers.clientStreaming(dataClient::createFile, noTenant);
        waitFor(TEST_TIMEOUT, noTenantResult);
        var noTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(noTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, noTenantError.getStatus().getCode());
    }

    @Test
    void testCreateFile_tenantInvalid() {

        var invalidTenant = FileOperationsTest.BASIC_CREATE_FILE_REQUEST.toBuilder().setTenant("£$%^**!\0\n/`¬").build();
        var invalidTenantResult = DataApiTestHelpers.clientStreaming(dataClient::createFile, invalidTenant);
        waitFor(TEST_TIMEOUT, invalidTenantResult);
        var invalidTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalidTenantError.getStatus().getCode());
    }

    @Test
    void testCreateFile_tenantNotFound() {

        var unknownTenant = FileOperationsTest.BASIC_CREATE_FILE_REQUEST.toBuilder().setTenant("UNKNOWN").build();
        var unknownTenantResult = DataApiTestHelpers.clientStreaming(dataClient::createFile, unknownTenant);
        waitFor(TEST_TIMEOUT, unknownTenantResult);
        var unknownTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(unknownTenantResult));
        assertEquals(Status.Code.NOT_FOUND, unknownTenantError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_tenantOmitted() throws Exception {

        var createFile = DataApiTestHelpers.clientStreaming(dataClient::createFile, FileOperationsTest.BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var v1Selector = MetadataUtil.selectorFor(v1Id);
        var updateRequest = FileOperationsTest.BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();

        var noTenant = updateRequest.toBuilder().clearTenant().build();
        var noTenantResult = DataApiTestHelpers.clientStreaming(dataClient::updateFile, noTenant);
        waitFor(TEST_TIMEOUT, noTenantResult);
        var noTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(noTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, noTenantError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_tenantInvalid() throws Exception {

        var createFile = DataApiTestHelpers.clientStreaming(dataClient::createFile, FileOperationsTest.BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var v1Selector = MetadataUtil.selectorFor(v1Id);
        var updateRequest = FileOperationsTest.BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();

        var invalidTenant = updateRequest.toBuilder().setTenant("£$%^**!\0\n/`¬").build();
        var invalidTenantResult = DataApiTestHelpers.clientStreaming(dataClient::updateFile, invalidTenant);
        waitFor(TEST_TIMEOUT, invalidTenantResult);
        var invalidTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalidTenantError.getStatus().getCode());

    }

    @Test
    void testUpdateFile_tenantNotFound() throws Exception {

        var createFile = DataApiTestHelpers.clientStreaming(dataClient::createFile, FileOperationsTest.BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var v1Selector = MetadataUtil.selectorFor(v1Id);
        var updateRequest = FileOperationsTest.BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();

        var unknownTenant = updateRequest.toBuilder().setTenant("UNKNOWN").build();
        var unknownTenantResult = DataApiTestHelpers.clientStreaming(dataClient::updateFile, unknownTenant);
        waitFor(TEST_TIMEOUT, unknownTenantResult);
        var unknownTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(unknownTenantResult));
        assertEquals(Status.Code.NOT_FOUND, unknownTenantError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_crossTenant() throws Exception {

        // Try to update a file created in a different tenant
        // Should fail with object not found, because original object does not exist in the tenant

        var createFile = DataApiTestHelpers.clientStreaming(dataClient::createFile, FileOperationsTest.BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var v1Selector = MetadataUtil.selectorFor(v1Id);
        var updateRequest = FileOperationsTest.BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();

        var crossTenant = updateRequest.toBuilder().setTenant(TEST_TENANT_2).build();
        var crossTenantResult = DataApiTestHelpers.clientStreaming(dataClient::updateFile, crossTenant);
        waitFor(TEST_TIMEOUT, crossTenantResult);
        var crossTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(crossTenantResult));
        assertEquals(Status.Code.NOT_FOUND, crossTenantError.getStatus().getCode());
    }

    @Test
    void testReadFile_tenantOmitted() throws Exception {

        var createFile = DataApiTestHelpers.clientStreaming(dataClient::createFile, FileOperationsTest.BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var readRequest = readRequest(v1Id);

        var noTenant = readRequest.toBuilder().clearTenant().build();
        var noTenantResult = DataApiTestHelpers.serverStreamingDiscard(dataClient::readFile, noTenant, execContext);
        waitFor(TEST_TIMEOUT, noTenantResult);
        var noTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(noTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, noTenantError.getStatus().getCode());
    }

    @Test
    void testReadFile_tenantInvalid() throws Exception {

        var createFile = DataApiTestHelpers.clientStreaming(dataClient::createFile, FileOperationsTest.BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var readRequest = readRequest(v1Id);

        var invalidTenant = readRequest.toBuilder().setTenant("£$%^**!\0\n/`¬").build();
        var invalidTenantResult = DataApiTestHelpers.serverStreamingDiscard(dataClient::readFile, invalidTenant, execContext);
        waitFor(TEST_TIMEOUT, invalidTenantResult);
        var invalidTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalidTenantError.getStatus().getCode());
    }

    @Test
    void testReadFile_tenantNotFound() throws Exception {

        var createFile = DataApiTestHelpers.clientStreaming(dataClient::createFile, FileOperationsTest.BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var readRequest = readRequest(v1Id);

        var unknownTenant = readRequest.toBuilder().setTenant("UNKNOWN").build();
        var unknownTenantResult = DataApiTestHelpers.serverStreamingDiscard(dataClient::readFile, unknownTenant, execContext);
        waitFor(TEST_TIMEOUT, unknownTenantResult);
        var unknownTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(unknownTenantResult));
        assertEquals(Status.Code.NOT_FOUND, unknownTenantError.getStatus().getCode());
    }

    @Test
    void testReadFile_crossTenant() throws Exception {

        // Try to read a file created in a different tenant
        // Should fail with object not found, because original object does not exist in the tenant

        var createFile = DataApiTestHelpers.clientStreaming(dataClient::createFile, FileOperationsTest.BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var readRequest = readRequest(v1Id);

        var crossTenant = readRequest.toBuilder().setTenant(TEST_TENANT_2).build();
        var crossTenantResult = DataApiTestHelpers.serverStreamingDiscard(dataClient::readFile, crossTenant, execContext);
        waitFor(TEST_TIMEOUT, crossTenantResult);
        var crossTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(crossTenantResult));
        assertEquals(Status.Code.NOT_FOUND, crossTenantError.getStatus().getCode());
    }
}
