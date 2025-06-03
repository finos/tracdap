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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.ResourceType;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.finos.tracdap.test.meta.SampleMetadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;


abstract class AdminConfigApiTest {

    public static final String TRAC_CONFIG_SECRETS = "config/trac-secrets.yaml";
    public static final String TRAC_TENANTS_SECRETS = "config/trac-secrets-tenants.yaml";
    public static final String TRAC_CONFIG_ENV_VAR = "TRAC_CONFIG_FILE";

    protected TracAdminApiGrpc.TracAdminApiBlockingStub adminApi;

    // Include this test case as a unit test
    static class UnitTest extends AdminConfigApiTest {

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_SECRETS, List.of(TRAC_TENANTS_SECRETS))
                .runDbDeploy(true)
                .addTenant(TEST_TENANT)
                .startService(TracAdminService.class)
                .startService(TracMetadataService.class)
                .build();

        @BeforeEach
        void setup() {
            adminApi = platform.createClient(ConfigKeys.ADMIN_SERVICE_KEY, TracAdminApiGrpc::newBlockingStub);
        }
    }

    // Include this test case for integration against different database backends
    @org.junit.jupiter.api.Tag("integration")
    @org.junit.jupiter.api.Tag("int-metadb")
    static class IntegrationTest extends AdminConfigApiTest {

        private static final String TRAC_CONFIG_ENV_FILE = System.getenv(TRAC_CONFIG_ENV_VAR);

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_ENV_FILE)
                .runDbDeploy(false)
                .addTenant(TEST_TENANT)
                .startService(TracAdminService.class)
                .startService(TracMetadataService.class)
                .build();

        @BeforeEach
        void setup() {
            adminApi = platform.createClient(ConfigKeys.ADMIN_SERVICE_KEY, TracAdminApiGrpc::newBlockingStub);
        }
    }

    @Test
    void createAndRead() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var writeRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("createAndRead")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();
        var configDetails = configEntry.getDetails();

        assertEquals("createAndRead", configEntry.getConfigClass());
        assertEquals("entry1", configEntry.getConfigKey());
        assertEquals(1, configEntry.getConfigVersion());
        assertTrue(configEntry.getIsLatestConfig());

        assertEquals(ObjectType.CONFIG, configDetails.getObjectType());

        var readRequest = ConfigReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setEntry(configEntry)
                .build();

        var rtConfig = adminApi.readConfigObject(readRequest);

        assertEquals(configObj, rtConfig.getDefinition());
        assertEquals(MetadataCodec.encodeValue("createAndRead"), rtConfig.getAttrsOrDefault("trac_config_class", null));
        assertEquals(MetadataCodec.encodeValue("entry1"), rtConfig.getAttrsOrDefault("trac_config_key", null));
    }

    @Test
    void createAndReadMasked() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.RESOURCE);

        // Expect the config API to mask secrets on round trip
        var maskedResource = configObj.getResource().toBuilder();
        for (var secret : configObj.getResource().getSecretsMap().keySet())
            maskedResource.putSecrets(secret, "");
        var maskedResourceObj = configObj.toBuilder().setResource(maskedResource).build();

        var writeRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("createAndReadMasked")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();
        var configDetails = configEntry.getDetails();

        assertEquals("createAndReadMasked", configEntry.getConfigClass());
        assertEquals("entry1", configEntry.getConfigKey());
        assertEquals(1, configEntry.getConfigVersion());
        assertTrue(configEntry.getIsLatestConfig());

        assertEquals(ObjectType.RESOURCE, configDetails.getObjectType());

        var readRequest = ConfigReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setEntry(configEntry)
                .build();

        var rtConfig = adminApi.readConfigObject(readRequest);

        assertEquals(maskedResourceObj, rtConfig.getDefinition());
        assertEquals(MetadataCodec.encodeValue("createAndReadMasked"), rtConfig.getAttrsOrDefault("trac_config_class", null));
        assertEquals(MetadataCodec.encodeValue("entry1"), rtConfig.getAttrsOrDefault("trac_config_key", null));
    }

    @Test
    void updateAndRead() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var writeRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("updateAndRead")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();

        var configObj2 = SampleMetadata.dummyVersionForType(configObj);

        var writeRequest2 = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("updateAndRead")
                .setConfigKey("entry1")
                .setPriorEntry(configEntry)
                .setDefinition(configObj2)
                .build();

        var writeResponse2 = adminApi.updateConfigObject(writeRequest2);
        var configEntry2 = writeResponse2.getEntry();

        var configDetails = configEntry2.getDetails();

        assertEquals("updateAndRead", configEntry.getConfigClass());
        assertEquals("entry1", configEntry.getConfigKey());
        assertEquals(1, configEntry.getConfigVersion());
        assertTrue(configEntry.getIsLatestConfig());

        assertEquals(ObjectType.CONFIG, configDetails.getObjectType());

        var readRequest = ConfigReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setEntry(configEntry2)
                .build();

        var rtConfig = adminApi.readConfigObject(readRequest);

        assertEquals(configObj2, rtConfig.getDefinition());
        assertNotEquals(configObj, rtConfig.getDefinition());
    }

    @Test
    void updateAndReadMasked() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.RESOURCE);

        // Expect the config API to mask secrets on round trip
        var maskedResource = configObj.getResource().toBuilder();
        for (var secret : configObj.getResource().getSecretsMap().keySet())
            maskedResource.putSecrets(secret, "");
        var maskedResourceObj = configObj.toBuilder().setResource(maskedResource).build();

        var writeRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("updateAndReadMasked")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();

        var configObj2 = SampleMetadata.dummyVersionForType(configObj);

        // Expect the config API to mask secrets on round trip
        var maskedResource2 = configObj2.getResource().toBuilder();
        for (var secret : configObj2.getResource().getSecretsMap().keySet())
            maskedResource2.putSecrets(secret, "");
        var maskedResourceObj2 = configObj2.toBuilder().setResource(maskedResource2).build();

        var writeRequest2 = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("updateAndReadMasked")
                .setConfigKey("entry1")
                .setPriorEntry(configEntry)
                .setDefinition(configObj2)
                .build();

        var writeResponse2 = adminApi.updateConfigObject(writeRequest2);
        var configEntry2 = writeResponse2.getEntry();

        var configDetails = configEntry2.getDetails();

        assertEquals("updateAndReadMasked", configEntry.getConfigClass());
        assertEquals("entry1", configEntry.getConfigKey());
        assertEquals(1, configEntry.getConfigVersion());
        assertTrue(configEntry.getIsLatestConfig());

        assertEquals(ObjectType.RESOURCE, configDetails.getObjectType());

        var readRequest = ConfigReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setEntry(configEntry2)
                .build();

        var rtConfig = adminApi.readConfigObject(readRequest);

        assertEquals(maskedResourceObj2, rtConfig.getDefinition());
        assertNotEquals(maskedResourceObj, rtConfig.getDefinition());
    }

    @Test
    void deleteAndTryRead() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var writeRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("deleteAndTryRead")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();

        var writeRequest2 = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("deleteAndTryRead")
                .setConfigKey("entry1")
                .setPriorEntry(configEntry)
                .clearDefinition()
                .build();

        var writeResponse2 = adminApi.deleteConfigObject(writeRequest2);
        var configEntry2 = writeResponse2.getEntry();

        var readRequest = ConfigReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setEntry(configEntry2)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.readConfigObject(readRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        // Original object should still be available, so long as we don't try to get latest
        var readOrigRequest = ConfigReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setEntry(configEntry.toBuilder().clearIsLatestConfig())
                .build();

        var rtConfig = adminApi.readConfigObject(readOrigRequest);
        assertEquals(configObj, rtConfig.getDefinition());
    }

    @Test
    void deleteAndCreateDifferent() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var writeRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("deleteAndCreateDifferent")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();

        var writeRequest2 = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("deleteAndCreateDifferent")
                .setConfigKey("entry1")
                .setPriorEntry(configEntry)
                .clearDefinition()
                .build();

        var writeResponse2 = adminApi.deleteConfigObject(writeRequest2);
        assertFalse(writeResponse2.getEntry().hasDetails());

        var resourceObj = SampleMetadata.dummyDefinitionForType(ObjectType.RESOURCE);

        // Expect the config API to mask secrets on round trip
        var maskedResource = resourceObj.getResource().toBuilder();
        for (var secret : resourceObj.getResource().getSecretsMap().keySet())
            maskedResource.putSecrets(secret, "");
        var maskedResourceObj = resourceObj.toBuilder().setResource(maskedResource).build();

        var resourceWriteRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("deleteAndCreateDifferent")
                .setConfigKey("entry1")
                .setDefinition(resourceObj)
                .build();

        var resourceWriteResponse = adminApi.createConfigObject(resourceWriteRequest);
        var resourceEntry = resourceWriteResponse.getEntry();

        var readRequest = ConfigReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setEntry(resourceEntry)
                .build();

        var rtResource = adminApi.readConfigObject(readRequest);
        assertEquals(maskedResourceObj, rtResource.getDefinition());
    }

    @Test
    void readSimpleEntry() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);

        var writeRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("readSimpleEntry")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeResponse = adminApi.createConfigObject(writeRequest);
        var configEntry = writeResponse.getEntry();
        var configDetails = configEntry.getDetails();

        assertTrue(configEntry.getIsLatestConfig());
        assertEquals(ObjectType.CONFIG, configDetails.getObjectType());

        var readRequest = ConfigReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setEntry(ConfigEntry.newBuilder()
                .setConfigClass("readSimpleEntry")
                .setConfigKey("entry1")
                .setIsLatestConfig(true))
                .build();

        var rtConfig = adminApi.readConfigObject(readRequest);

        assertEquals(configObj, rtConfig.getDefinition());
        assertEquals(MetadataCodec.encodeValue("readSimpleEntry"), rtConfig.getAttrsOrDefault("trac_config_class", null));
        assertEquals(MetadataCodec.encodeValue("entry1"), rtConfig.getAttrsOrDefault("trac_config_key", null));
    }

    @Test
    void readBatch() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);
        var configObj2 = SampleMetadata.dummyVersionForType(configObj);

        var writeRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("readBatch")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeRequest2 = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("readBatch")
                .setConfigKey("entry2")
                .setDefinition(configObj2)
                .build();

        var writeResponse = adminApi.createConfigObject(writeRequest);
        var writeResponse2 = adminApi.createConfigObject(writeRequest2);

        var readBatchRequest = ConfigReadBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addEntries(writeResponse.getEntry())
                .addEntries(writeResponse2.getEntry())
                .build();

        var readBatchResponse = adminApi.readConfigBatch(readBatchRequest);

        assertEquals(2, readBatchResponse.getEntriesCount());
        assertEquals(configObj, readBatchResponse.getEntries(0).getDefinition());
        assertEquals(configObj2, readBatchResponse.getEntries(1).getDefinition());
    }

    @Test
    void listEntries() {

        var configObj = SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG);
        var configObj2 = SampleMetadata.dummyVersionForType(configObj);

        var writeRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("listEntries")
                .setConfigKey("entry1")
                .setDefinition(configObj)
                .build();

        var writeRequest2 = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("listEntries")
                .setConfigKey("entry2")
                .setDefinition(configObj2)
                .build();

        var writeResponse = adminApi.createConfigObject(writeRequest);
        var writeResponse2 = adminApi.createConfigObject(writeRequest2);

        var listEntriesRequest = ConfigListRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("listEntries")
                .build();

        var listEntriesResponse = adminApi.listConfigEntries(listEntriesRequest);

        assertEquals(2, listEntriesResponse.getEntriesCount());
        assertEquals(writeResponse.getEntry(), listEntriesResponse.getEntries(0));
        assertEquals(writeResponse2.getEntry(), listEntriesResponse.getEntries(1));
    }

    @Test
    void listEntriesWithFilter() {

        var resourceObj = SampleMetadata.dummyDefinitionForType(ObjectType.RESOURCE);
        var resourceObj2 = resourceObj.toBuilder().setResource(resourceObj.getResource().toBuilder().setResourceType(ResourceType.INTERNAL_STORAGE)).build();

        var writeRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("listEntriesWithFilter")
                .setConfigKey("entry1")
                .setDefinition(resourceObj)
                .build();

        var writeRequest2 = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("listEntriesWithFilter")
                .setConfigKey("entry2")
                .setDefinition(resourceObj2)
                .build();

        var writeResponse = adminApi.createConfigObject(writeRequest);
        var writeResponse2 = adminApi.createConfigObject(writeRequest2);

        var listEntriesRequest = ConfigListRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("listEntriesWithFilter")
                .setResourceType(ResourceType.MODEL_REPOSITORY)
                .build();

        var listEntriesResponse = adminApi.listConfigEntries(listEntriesRequest);

        assertEquals(1, listEntriesResponse.getEntriesCount());
        assertEquals(writeResponse.getEntry(), listEntriesResponse.getEntries(0));
        assertNotEquals(writeResponse2.getEntry(), listEntriesResponse.getEntries(0));
    }
}
