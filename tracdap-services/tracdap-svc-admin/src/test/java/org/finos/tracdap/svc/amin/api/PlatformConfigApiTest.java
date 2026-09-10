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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.service.PlatformStateManager;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.PlatformConfigEntry;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.finos.tracdap.test.meta.SampleMetadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.*;


abstract class PlatformConfigApiTest {

    public static final String TRAC_DYNAMIC_CONFIG = "config/trac-dynamic.yaml";
    public static final String TRAC_CONFIG_ENV_VAR = "TRAC_CONFIG_FILE";

    protected TracAdminApiGrpc.TracAdminApiBlockingStub adminApi;
    protected PlatformTest platform;

    // Include this test case as a unit test
    static class UnitTest extends PlatformConfigApiTest {

        @RegisterExtension
        public static final PlatformTest testPlatform = PlatformTest.forConfig(TRAC_DYNAMIC_CONFIG)
                .runDbDeploy(true)
                .startService(TracAdminService.class)
                .startService(TracMetadataService.class)
                .build();

        @BeforeEach
        void setup() {
            platform = testPlatform;
            adminApi = testPlatform.createClient(ConfigKeys.ADMIN_SERVICE_KEY, TracAdminApiGrpc::newBlockingStub);
        }
    }

    // Include this test case for integration against different database backends
    @org.junit.jupiter.api.Tag("integration")
    @org.junit.jupiter.api.Tag("int-metadb")
    static class IntegrationTest extends PlatformConfigApiTest {

        private static final String TRAC_CONFIG_ENV_FILE = System.getenv(TRAC_CONFIG_ENV_VAR);

        @RegisterExtension
        public static final PlatformTest testPlatform = PlatformTest.forConfig(TRAC_CONFIG_ENV_FILE)
                .runDbDeploy(false)
                .startService(TracAdminService.class)
                .startService(TracMetadataService.class)
                .build();

        @BeforeEach
        void setup() {
            platform = testPlatform;
            adminApi = testPlatform.createClient(ConfigKeys.ADMIN_SERVICE_KEY, TracAdminApiGrpc::newBlockingStub);
        }
    }

    @Test
    void createAndRead() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var writeRequest = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("createAndRead")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createPlatformConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();

        assertEquals("createAndRead", configEntry.getConfigClass());
        assertEquals("entry1", configEntry.getConfigKey());
        assertEquals(1, configEntry.getConfigVersion());
        assertFalse(configEntry.getConfigDeleted());

        var readRequest = PlatformConfigReadRequest.newBuilder()
                .setEntry(configEntry)
                .build();

        var rtConfig = adminApi.readPlatformConfigObject(readRequest);

        assertEquals(configObj, rtConfig.getDefinition());
    }

    @Test
    void createDuplicateFails() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var writeRequest = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("createDuplicateFails")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        adminApi.createPlatformConfigObject(writeRequest);

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.createPlatformConfigObject(writeRequest));
        assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
    }

    @Test
    void updateAndRead() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var writeRequest = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("updateAndRead")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createPlatformConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();

        var configObj2 = SampleMetadata.dummyVersionForType(configObj);

        var writeRequest2 = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("updateAndRead")
                .setConfigKey("entry1")
                .setPriorEntry(configEntry)
                .setDefinition(configObj2)
                .build();

        var writeResponse2 = adminApi.updatePlatformConfigObject(writeRequest2);
        var configEntry2 = writeResponse2.getEntry();

        assertEquals(2, configEntry2.getConfigVersion());

        var readRequest = PlatformConfigReadRequest.newBuilder()
                .setEntry(configEntry2)
                .build();

        var rtConfig = adminApi.readPlatformConfigObject(readRequest);

        assertEquals(configObj2, rtConfig.getDefinition());
        assertNotEquals(configObj, rtConfig.getDefinition());
    }

    @Test
    void updatePriorMismatchFails() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var writeRequest = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("updatePriorMismatchFails")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createPlatformConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();

        // Apply one legitimate update, so the entry is now at version 2
        var configObj2 = SampleMetadata.dummyVersionForType(configObj);

        adminApi.updatePlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("updatePriorMismatchFails")
                .setConfigKey("entry1")
                .setPriorEntry(configEntry)
                .setDefinition(configObj2)
                .build());

        // Try to update again using the stale (version 1) prior entry
        var staleUpdate = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("updatePriorMismatchFails")
                .setConfigKey("entry1")
                .setPriorEntry(configEntry)
                .setDefinition(configObj2)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.updatePlatformConfigObject(staleUpdate));
        assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
    }

    @Test
    void deleteAndTryRead() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var writeRequest = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("deleteAndTryRead")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createPlatformConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();

        var deleteRequest = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("deleteAndTryRead")
                .setConfigKey("entry1")
                .setPriorEntry(configEntry)
                .build();

        var deleteResponse = adminApi.deletePlatformConfigObject(deleteRequest);
        assertTrue(deleteResponse.getEntry().getConfigDeleted());

        var readRequest = PlatformConfigReadRequest.newBuilder()
                .setEntry(configEntry)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.readPlatformConfigObject(readRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void deleteAndCreateDifferent() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var writeRequest = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("deleteAndCreateDifferent")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createPlatformConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();

        adminApi.deletePlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("deleteAndCreateDifferent")
                .setConfigKey("entry1")
                .setPriorEntry(configEntry)
                .build());

        var configObj2 = SampleMetadata.dummyVersionForType(configObj);

        var recreateResponse = adminApi.createPlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("deleteAndCreateDifferent")
                .setConfigKey("entry1")
                .setDefinition(configObj2)
                .build());

        // entry1 was created (v1) then deleted (v2), so re-creating continues the version sequence at v3
        assertEquals(3, recreateResponse.getEntry().getConfigVersion());
        assertFalse(recreateResponse.getEntry().getConfigDeleted());

        var rtConfig = adminApi.readPlatformConfigObject(PlatformConfigReadRequest.newBuilder()
                .setEntry(recreateResponse.getEntry())
                .build());

        assertEquals(configObj2, rtConfig.getDefinition());
    }

    @Test
    void readBatch() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);
        var configObj2 = SampleMetadata.dummyVersionForType(configObj);

        var writeResponse = adminApi.createPlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("readBatch")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build());

        var writeResponse2 = adminApi.createPlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("readBatch")
                .setConfigKey("entry2")
                .setDefinition(configObj2)
                .build());

        var readBatchRequest = PlatformConfigReadBatchRequest.newBuilder()
                .addEntries(writeResponse.getEntry())
                .addEntries(writeResponse2.getEntry())
                .build();

        var readBatchResponse = adminApi.readPlatformConfigBatch(readBatchRequest);

        assertEquals(2, readBatchResponse.getEntriesCount());
        assertEquals(configObj, readBatchResponse.getEntries(0).getDefinition());
        assertEquals(configObj2, readBatchResponse.getEntries(1).getDefinition());
    }

    @Test
    void listEntries() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);
        var configObj2 = SampleMetadata.dummyVersionForType(configObj);

        var writeResponse = adminApi.createPlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("listEntries")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build());

        var writeResponse2 = adminApi.createPlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("listEntries")
                .setConfigKey("entry2")
                .setDefinition(configObj2)
                .build());

        var listEntriesRequest = PlatformConfigListRequest.newBuilder()
                .setConfigClass("listEntries")
                .build();

        var listEntriesResponse = adminApi.listPlatformConfigEntries(listEntriesRequest);

        assertEquals(2, listEntriesResponse.getEntriesCount());
        assertEquals(writeResponse.getEntry(), listEntriesResponse.getEntries(0));
        assertEquals(writeResponse2.getEntry(), listEntriesResponse.getEntries(1));
    }

    @Test
    void liveUpdatePropagatesToIndependentStateManager() {

        // End-to-end smoke test: write a platform config item through the admin API, then confirm a
        // second, independently-constructed PlatformStateManager sees it live, with no restart needed

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var stateManager = new PlatformStateManager(
                "liveUpdate", platform.metaClientInternalBlocking(),
                new GrpcConcern() { public String concernName() { return "test"; } });

        stateManager.init();
        assertFalse(stateManager.hasConfig("entry1"));

        var writeResponse = adminApi.createPlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("liveUpdate")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build());

        // The state manager was not re-initialized - simulate the live notification it would
        // have received from NotifierService.platformConfigUpdate() via its own MessageProcessor
        var update = org.finos.tracdap.api.internal.PlatformConfigUpdate.newBuilder()
                .setUpdateType(org.finos.tracdap.api.internal.ConfigUpdateType.CREATE)
                .setConfigEntry(writeResponse.getEntry())
                .build();

        var status = stateManager.applyConfigUpdate(update);

        assertEquals(org.finos.tracdap.api.internal.ReceivedCode.OK, status.getCode());
        assertTrue(stateManager.hasConfig("entry1"));
        assertEquals(configObj, stateManager.getConfig("entry1"));
    }
}
