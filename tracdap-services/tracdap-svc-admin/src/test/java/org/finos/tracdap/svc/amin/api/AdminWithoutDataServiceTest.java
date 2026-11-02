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

package org.finos.tracdap.svc.amin.api;

import org.finos.tracdap.api.CheckTenantStorageRequest;
import org.finos.tracdap.api.PlatformConfigListRequest;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.helpers.PlatformTest;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;


class AdminWithoutDataServiceTest {

    private static final String ADMIN_NO_DATA_CONFIG = "config/trac-admin-no-data.yaml";

    @RegisterExtension
    static final PlatformTest platform = PlatformTest.forConfig(ADMIN_NO_DATA_CONFIG)
            .runDbDeploy(true)
            .addTenant(TEST_TENANT)
            .startService(TracMetadataService.class)
            .startService(TracAdminService.class)
            .build();

    @Test
    void adminServiceRuns() {

        var adminClient = platform.adminClientBlocking();
        var request = PlatformConfigListRequest.newBuilder().setConfigClass("no_data_service").build();

        Assertions.assertDoesNotThrow(() -> adminClient.listPlatformConfigEntries(request));
    }

    @Test
    void checkTenantStorage_noDataService() {

        var adminClient = platform.adminClientBlocking();
        var request = CheckTenantStorageRequest.newBuilder().setTenantCode(TEST_TENANT).build();

        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> adminClient.checkTenantStorage(request));

        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
        Assertions.assertEquals("No data service is configured", error.getStatus().getDescription());
    }
}
