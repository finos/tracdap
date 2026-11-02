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

package org.finos.tracdap.svc.data.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.TenantInfo;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;


class LiveTenantTest {

    private static final String LIVE_TENANTS_CONFIG = "config/trac-dynamic-live-tenants.yaml";
    private static final String TENANT_TEMPLATE = "config/trac-tenant-template.yaml";
    private static final String TRAC_DYNAMIC_CONFIG = "config/trac-dynamic.yaml";

    private static final String TENANT_STORAGE_DIR = "tenant_storage";
    private static final String TENANT_RESULTS_DIR = "tenant_results";

    private static final Duration STORAGE_TIMEOUT = Duration.ofSeconds(30);

    static class TemplateConfigured {

        @RegisterExtension
        static final PlatformTest platform = PlatformTest.forConfig(LIVE_TENANTS_CONFIG, List.of(TENANT_TEMPLATE))
                .runDbDeploy(true)
                .preStartAction(LiveTenantTest::createStorageParents)
                .startService(TracMetadataService.class)
                .startService(TracDataService.class)
                .startService(TracAdminService.class)
                .build();

        @Test
        void createTenant_configFromTemplate() {

            var adminClient = platform.adminClientBlocking();

            var response = adminClient.createTenant(createRequest("LIVE_CONFIG", "Live config tenant"));

            Assertions.assertEquals("LIVE_CONFIG", response.getTenant().getTenantCode());
            Assertions.assertEquals("Live config tenant", response.getTenant().getDescription());

            var entryKeys = response.getEntriesList().stream()
                    .map(e -> e.getConfigClass() + "/" + e.getConfigKey())
                    .sorted().collect(Collectors.toList());

            Assertions.assertEquals(List.of(
                    "trac_config/trac_tenant_config",
                    "trac_resources/datasets",
                    "trac_resources/trac_results"), entryKeys);

            var tenantConfig = readConfig(adminClient, "LIVE_CONFIG", ConfigKeys.TRAC_CONFIG, ConfigKeys.TRAC_TENANT_CONFIG)
                    .getDefinition().getConfig();

            Assertions.assertEquals("Live config tenant", tenantConfig.getPropertiesOrThrow(ConfigKeys.TENANT_DISPLAY_NAME));
            Assertions.assertEquals("datasets", tenantConfig.getPropertiesOrThrow(ConfigKeys.STORAGE_DEFAULT_LOCATION));
            Assertions.assertEquals("trac_results", tenantConfig.getPropertiesOrThrow(ConfigKeys.RESULT_STORAGE_LOCATION));

            var datasets = readConfig(adminClient, "LIVE_CONFIG", ConfigKeys.TRAC_RESOURCES, "datasets")
                    .getDefinition().getResource();

            var expectedRoot = platform.workingDir().resolve(TENANT_STORAGE_DIR).resolve("LIVE_CONFIG");
            Assertions.assertEquals(expectedRoot, Path.of(datasets.getPropertiesOrThrow("rootPath")));

            var tenants = platform.metaClientBlocking().listTenants(ListTenantsRequest.getDefaultInstance());
            Assertions.assertTrue(tenants.getTenantsList().contains(response.getTenant()));
        }

        @Test
        void createTenant_duplicate() {

            var adminClient = platform.adminClientBlocking();

            adminClient.createTenant(createRequest("LIVE_DUPLICATE", "Live duplicate tenant"));

            var error = Assertions.assertThrows(StatusRuntimeException.class,
                    () -> adminClient.createTenant(createRequest("LIVE_DUPLICATE", "Live duplicate tenant")));

            Assertions.assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
        }

        @Test
        void createTenant_invalidCode() {

            var adminClient = platform.adminClientBlocking();

            var error = Assertions.assertThrows(StatusRuntimeException.class,
                    () -> adminClient.createTenant(createRequest("LIVE-INVALID", "Live invalid tenant")));

            Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

            var tenants = platform.metaClientBlocking().listTenants(ListTenantsRequest.getDefaultInstance());
            Assertions.assertTrue(tenants.getTenantsList().stream().map(TenantInfo::getTenantCode).noneMatch("LIVE-INVALID"::equals));
        }

        @Test
        void checkTenantStorage_presentAfterCreate() {

            var adminClient = platform.adminClientBlocking();

            adminClient.createTenant(createRequest("LIVE_STORAGE", "Live storage tenant"));

            var storage = waitForStorage(adminClient, "LIVE_STORAGE");

            Assertions.assertEquals(2, storage.getStorageCount());
            Assertions.assertTrue(storage.getStorageOrThrow("datasets"));
            Assertions.assertTrue(storage.getStorageOrThrow("trac_results"));

            Assertions.assertTrue(Files.isDirectory(platform.workingDir().resolve(TENANT_STORAGE_DIR).resolve("LIVE_STORAGE")));
            Assertions.assertTrue(Files.isDirectory(platform.workingDir().resolve(TENANT_RESULTS_DIR).resolve("LIVE_STORAGE")));
        }

        @Test
        void checkTenantStorage_unknownTenant() {

            var adminClient = platform.adminClientBlocking();

            var request = CheckTenantStorageRequest.newBuilder().setTenantCode("LIVE_UNKNOWN").build();
            var error = Assertions.assertThrows(StatusRuntimeException.class, () -> adminClient.checkTenantStorage(request));

            Assertions.assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
        }

        @Test
        void liveTenant_fileRoundTrip() {

            var adminClient = platform.adminClientBlocking();
            var dataClient = platform.dataClientBlocking();

            adminClient.createTenant(createRequest("LIVE_DATA", "Live data tenant"));
            waitForStorage(adminClient, "LIVE_DATA");

            var content = ByteString.copyFrom("Hello live tenant", StandardCharsets.UTF_8);

            var writeRequest = FileWriteRequest.newBuilder()
                    .setTenant("LIVE_DATA")
                    .setName("hello.txt")
                    .setMimeType("text/plain")
                    .setSize(content.size())
                    .setContent(content)
                    .build();

            var fileId = dataClient.createSmallFile(writeRequest);

            var readRequest = FileReadRequest.newBuilder()
                    .setTenant("LIVE_DATA")
                    .setSelector(MetadataUtil.selectorFor(fileId))
                    .build();

            var readResponse = dataClient.readSmallFile(readRequest);

            Assertions.assertEquals(content, readResponse.getContent());
        }
    }

    static class StorageParentsMissing {

        @RegisterExtension
        static final PlatformTest platform = PlatformTest.forConfig(LIVE_TENANTS_CONFIG, List.of(TENANT_TEMPLATE))
                .runDbDeploy(true)
                .startService(TracMetadataService.class)
                .startService(TracDataService.class)
                .startService(TracAdminService.class)
                .build();

        @Test
        void checkTenantStorage_missingUntilFixed() throws Exception {

            var adminClient = platform.adminClientBlocking();

            adminClient.createTenant(createRequest("LIVE_MISSING", "Live missing tenant"));

            // The data service cannot create a root whose parent is missing, the storage is reported missing
            var deadline = Instant.now().plus(Duration.ofSeconds(3));

            while (Instant.now().isBefore(deadline)) {

                var storage = checkStorage(adminClient, "LIVE_MISSING");

                Assertions.assertEquals(2, storage.getStorageCount());
                Assertions.assertFalse(storage.getStorageOrThrow("datasets"));
                Assertions.assertFalse(storage.getStorageOrThrow("trac_results"));

                Thread.sleep(250);
            }

            Assertions.assertFalse(Files.exists(platform.workingDir().resolve(TENANT_STORAGE_DIR)));

            // Once the installation is fixed, the next check finds the storage
            createStorageParents(platform);

            var storage = waitForStorage(adminClient, "LIVE_MISSING");

            Assertions.assertTrue(storage.getStorageOrThrow("datasets"));
            Assertions.assertTrue(storage.getStorageOrThrow("trac_results"));
        }
    }

    static class TemplateNotConfigured {

        @RegisterExtension
        static final PlatformTest platform = PlatformTest.forConfig(TRAC_DYNAMIC_CONFIG)
                .runDbDeploy(true)
                .startService(TracMetadataService.class)
                .startService(TracAdminService.class)
                .build();

        @Test
        void createTenant_noTemplate() {

            var adminClient = platform.adminClientBlocking();

            var error = Assertions.assertThrows(StatusRuntimeException.class,
                    () -> adminClient.createTenant(createRequest("LIVE_NO_TEMPLATE", "Live tenant")));

            Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
            Assertions.assertEquals("No tenant template is configured", error.getStatus().getDescription());

            var tenants = platform.metaClientBlocking().listTenants(ListTenantsRequest.getDefaultInstance());
            Assertions.assertTrue(tenants.getTenantsList().stream().map(TenantInfo::getTenantCode).noneMatch("LIVE_NO_TEMPLATE"::equals));
        }
    }

    private static void createStorageParents(PlatformTest platform) {

        try {
            Files.createDirectories(platform.workingDir().resolve(TENANT_STORAGE_DIR));
            Files.createDirectories(platform.workingDir().resolve(TENANT_RESULTS_DIR));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static CreateTenantRequest createRequest(String tenantCode, String displayName) {

        return CreateTenantRequest.newBuilder()
                .setTenantCode(tenantCode)
                .setDisplayName(displayName)
                .build();
    }

    private static ConfigReadResponse readConfig(
            TracAdminApiGrpc.TracAdminApiBlockingStub adminClient,
            String tenant, String configClass, String configKey) {

        var request = ConfigReadRequest.newBuilder()
                .setTenant(tenant)
                .setEntry(ConfigEntry.newBuilder()
                        .setConfigClass(configClass)
                        .setConfigKey(configKey)
                        .setIsLatestConfig(true))
                .build();

        return adminClient.readConfigObject(request);
    }

    private static CheckTenantStorageResponse checkStorage(TracAdminApiGrpc.TracAdminApiBlockingStub adminClient, String tenant) {

        var request = CheckTenantStorageRequest.newBuilder().setTenantCode(tenant).build();
        return adminClient.checkTenantStorage(request);
    }

    private static CheckTenantStorageResponse waitForStorage(TracAdminApiGrpc.TracAdminApiBlockingStub adminClient, String tenant) {

        var deadline = Instant.now().plus(STORAGE_TIMEOUT);

        while (true) {

            var storage = checkStorage(adminClient, tenant);

            if (storage.getStorageCount() > 0 && storage.getStorageMap().values().stream().allMatch(present -> present))
                return storage;

            if (Instant.now().isAfter(deadline))
                Assertions.fail("Timed out waiting for tenant storage: " + storage);

            try {
                Thread.sleep(250);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assertions.fail("Interrupted waiting for tenant storage");
            }
        }
    }
}
