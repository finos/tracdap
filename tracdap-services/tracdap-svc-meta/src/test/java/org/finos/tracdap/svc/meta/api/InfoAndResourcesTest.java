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

package org.finos.tracdap.svc.meta.api;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.util.VersionInfo;
import org.finos.tracdap.metadata.ResourceType;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;


abstract class InfoAndResourcesTest {

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TRAC_CONFIG_ENV_VAR = "TRAC_CONFIG_FILE";

    protected TracMetadataApiGrpc.TracMetadataApiBlockingStub readApi;

    // Include this test case as a unit test
    static class UnitTest extends InfoAndResourcesTest {

        @RegisterExtension
        private static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
                .runDbDeploy(true)
                .addTenant(TEST_TENANT)
                .startService(TracMetadataService.class)
                .build();

        @BeforeEach
        void setup() {
            readApi = platform.metaClientBlocking();
        }
    }

    // For now, do not run this test for integration against the metadb backend databases
    // Currently resources come from config, so there is no integration point
    // When resources move to being managed as metadata objects, this will need to be enabled
    @Disabled
    @Tag("integration")
    @Tag("int-metadb")
    static class IntegrationTest extends InfoAndResourcesTest {

        private static final String TRAC_CONFIG_ENV_FILE = System.getenv(TRAC_CONFIG_ENV_VAR);

        @RegisterExtension
        private static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_ENV_FILE)
                .runDbDeploy(false)
                .addTenant(TEST_TENANT)
                .startService(TracMetadataService.class)
                .build();

        @BeforeEach
        void setup() {
            readApi = platform.metaClientBlocking();
        }
    }

    @Test
    void platformInfo() {

        var platformInfo = readApi.platformInfo(PlatformInfoRequest.newBuilder().build());

        System.out.println("Running TRAC D.A.P. version " + platformInfo.getTracVersion());

        var expectedVersion = VersionInfo.getComponentVersion(TracMetadataService.class);
        Assertions.assertEquals(expectedVersion, platformInfo.getTracVersion());

        // Environment settings are set up for this test in test-unit.yaml in the -list-test resources folder
        // For integration tests, config files are in the .github folder under config

        Assertions.assertEquals("TEST_ENVIRONMENT", platformInfo.getEnvironment());
        Assertions.assertFalse(platformInfo.getProduction());
        Assertions.assertTrue(platformInfo.containsDeploymentInfo("region"));
        Assertions.assertEquals("UK", platformInfo.getDeploymentInfoOrThrow("region"));
    }

    @Test
    void listTenants() {

        var tenantsResponse = readApi.listTenants(ListTenantsRequest.newBuilder().build());
        var tenants = tenantsResponse.getTenantsList();

        Assertions.assertEquals(1, tenants.size());
        Assertions.assertEquals(TEST_TENANT, tenants.get(0).getTenantCode());

        // Default description set up in org.finos.tracdap.test.helpers.PlatformTest
        // Also for integration tests, in the integration.yml workflow for GitHub actions
        var expectedDescription = "Test tenant [" + TEST_TENANT + "]";
        Assertions.assertEquals(expectedDescription, tenants.get(0).getDescription());
    }

    @Test
    void listResources_repo() {

        var request = ListResourcesRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setResourceType(ResourceType.MODEL_REPOSITORY)
                .build();

        var response = readApi.listResources(request);
        var storageResources = response.getResourcesList();

        Assertions.assertEquals(2, storageResources.size());

        var unitTestRepo = storageResources.stream()
                .filter(resource -> resource.getResourceKey().equals("UNIT_TEST_REPO"))
                .findFirst();

        Assertions.assertTrue(unitTestRepo.isPresent());
        Assertions.assertEquals(ResourceType.MODEL_REPOSITORY, unitTestRepo.get().getResourceType());
        Assertions.assertEquals("LOCAL", unitTestRepo.get().getProtocol());
        Assertions.assertEquals("repo_value_1", unitTestRepo.get().getPropertiesOrThrow("unit_test_property"));
    }

    @Test
    void listResources_internalStorage() {

        var request = ListResourcesRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setResourceType(ResourceType.INTERNAL_STORAGE)
                .build();

        var response = readApi.listResources(request);
        var storageResources = response.getResourcesList();

        Assertions.assertEquals(2, storageResources.size());

        var unitTestRepo = storageResources.stream()
                .filter(resource -> resource.getResourceKey().equals("UNIT_TEST_STORAGE"))
                .findFirst();

        Assertions.assertTrue(unitTestRepo.isPresent());
        Assertions.assertEquals(ResourceType.INTERNAL_STORAGE, unitTestRepo.get().getResourceType());
        Assertions.assertEquals("LOCAL", unitTestRepo.get().getProtocol());
        Assertions.assertEquals("storage_value_1", unitTestRepo.get().getPropertiesOrThrow("unit_test_property"));
    }

    @Test
    void listResources_unknownTenant() {

        var request = ListResourcesRequest.newBuilder()
                .setTenant("UNKNOWN_TENANT")
                .setResourceType(ResourceType.MODEL_REPOSITORY)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.listResources(request));
        Assertions.assertEquals(Status.Code.NOT_FOUND, e.getStatus().getCode());
    }

    @Test
    void listResources_invalidTenant() {

        var request = ListResourcesRequest.newBuilder()
                .setTenant("$$$INVALID")
                .setResourceType(ResourceType.MODEL_REPOSITORY)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.listResources(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    void listResources_invalidResourceType() {

        var request = ListResourcesRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setResourceType(ResourceType.RESOURCE_TYPE_NOT_SET)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.listResources(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    void getResource_repoOk() {

        var request = ResourceInfoRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setResourceType(ResourceType.MODEL_REPOSITORY)
                .setResourceKey("UNIT_TEST_REPO")
                .build();

        var response = readApi.resourceInfo(request);

        Assertions.assertEquals(ResourceType.MODEL_REPOSITORY, response.getResourceType());
        Assertions.assertEquals("UNIT_TEST_REPO", response.getResourceKey());
        Assertions.assertEquals("LOCAL", response.getProtocol());
        Assertions.assertTrue(response.containsProperties("unit_test_property"));
        Assertions.assertTrue(response.containsProperties("unit-test.property"));
        Assertions.assertEquals("repo_value_1", response.getPropertiesOrThrow("unit_test_property"));
        Assertions.assertEquals("repo-value.1", response.getPropertiesOrThrow("unit-test.property"));
    }

    @Test
    void getResource_repoUnknownKey() {

        var request = ResourceInfoRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setResourceType(ResourceType.MODEL_REPOSITORY)
                .setResourceKey("UNKNOWN_REPO")
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.resourceInfo(request));
        Assertions.assertEquals(Status.Code.NOT_FOUND, e.getStatus().getCode());
    }

    @Test
    void getResource_repoInvalidKey() {

        var request = ResourceInfoRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setResourceType(ResourceType.MODEL_REPOSITORY)
                .setResourceKey("$$$-INVALID")
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.resourceInfo(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    void getResource_internalStorageOk() {

        var request = ResourceInfoRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setResourceType(ResourceType.INTERNAL_STORAGE)
                .setResourceKey("UNIT_TEST_STORAGE")
                .build();

        var response = readApi.resourceInfo(request);

        Assertions.assertEquals(ResourceType.INTERNAL_STORAGE, response.getResourceType());
        Assertions.assertEquals("UNIT_TEST_STORAGE", response.getResourceKey());
        Assertions.assertEquals("LOCAL", response.getProtocol());
        Assertions.assertTrue(response.containsProperties("unit_test_property"));
        Assertions.assertTrue(response.containsProperties("unit-test.property"));
        Assertions.assertEquals("storage_value_1", response.getPropertiesOrThrow("unit_test_property"));
        Assertions.assertEquals("storage-value.1", response.getPropertiesOrThrow("unit-test.property"));
    }

    @Test
    void getResource_internalStorageUnknownKey() {

        var request = ResourceInfoRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setResourceType(ResourceType.INTERNAL_STORAGE)
                .setResourceKey("UNKNOWN_STORAGE")
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.resourceInfo(request));
        Assertions.assertEquals(Status.Code.NOT_FOUND, e.getStatus().getCode());
    }

    @Test
    void getResource_internalStorageInvalidKey() {

        var request = ResourceInfoRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setResourceType(ResourceType.INTERNAL_STORAGE)
                .setResourceKey("$$$-INVALID")
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.resourceInfo(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    void getResource_unknownTenant() {

        var request = ResourceInfoRequest.newBuilder()
                .setTenant("UNKNOWN_TENANT")
                .setResourceType(ResourceType.MODEL_REPOSITORY)
                .setResourceKey("UNIT_TEST_REPO")
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.resourceInfo(request));
        Assertions.assertEquals(Status.Code.NOT_FOUND, e.getStatus().getCode());
    }

    @Test
    void getResource_invalidTenant() {

        var request = ResourceInfoRequest.newBuilder()
                .setTenant("$$$-INVALID")
                .setResourceType(ResourceType.MODEL_REPOSITORY)
                .setResourceKey("UNIT_TEST_REPO")
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.resourceInfo(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    void getResource_invalidResourceType() {

        var request = ResourceInfoRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setResourceType(ResourceType.RESOURCE_TYPE_NOT_SET)
                .setResourceKey("UNIT_TEST_REPO")
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.resourceInfo(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    void getClientConfig_ok() {

        var request = ClientConfigRequest.newBuilder()
                .setApplication("client-app")
                .build();

        var response = readApi.clientConfig(request);

        Assertions.assertEquals(1, response.getPropertiesCount());
        Assertions.assertTrue(response.containsProperties("unit.test.property"));
        Assertions.assertEquals("value1", response.getPropertiesOrThrow("unit.test.property"));
    }

    @Test
    void getClientConfig_appInvalid() {

        var request = ClientConfigRequest.newBuilder()
                .setApplication("%%%client-app")
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.clientConfig(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    void getClientConfig_appNotFound() {

        var request = ClientConfigRequest.newBuilder()
                .setApplication("unknown-app")
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> readApi.clientConfig(request));
        Assertions.assertEquals(Status.Code.NOT_FOUND, e.getStatus().getCode());
    }

}
