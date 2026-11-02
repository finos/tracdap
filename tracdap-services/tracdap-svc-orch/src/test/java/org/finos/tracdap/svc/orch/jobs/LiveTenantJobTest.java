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

package org.finos.tracdap.svc.orch.jobs;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;


@Tag("integration")
@Tag("int-e2e")
public class LiveTenantJobTest {

    private static final String E2E_LIVE_TENANTS_CONFIG = "config/trac-e2e-live-tenants.yaml";
    private static final String TENANT_TEMPLATE = "config/trac-tenant-template.yaml";

    private static final String LIVE_TENANT = "LIVE_E2E";
    private static final String LOCAL_REPO = "TRAC_LOCAL_REPO";

    private static final Duration STORAGE_TIMEOUT = Duration.ofSeconds(30);

    private static final Logger log = LoggerFactory.getLogger(LiveTenantJobTest.class);

    // No tenants exist when the services start, the test tenant is created while they are running

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(E2E_LIVE_TENANTS_CONFIG, List.of(TENANT_TEMPLATE))
            .runDbDeploy(true)
            .runCacheDeploy(true)
            .prepareLocalExecutor(true)
            .preStartAction(LiveTenantJobTest::createStorageParents)
            .startService(TracMetadataService.class)
            .startService(TracDataService.class)
            .startService(TracOrchestratorService.class)
            .startService(TracAdminService.class)
            .build();

    @Test
    void createTenantThenRunJob() throws Exception {

        var adminClient = platform.adminClientBlocking();
        var dataClient = platform.dataClientBlocking();

        log.info("Creating tenant [{}] on the running platform...", LIVE_TENANT);

        adminClient.createTenant(CreateTenantRequest.newBuilder()
                .setTenantCode(LIVE_TENANT)
                .setDisplayName("Live E2E tenant")
                .build());

        waitForStorage(adminClient);

        var repoResource = ResourceDefinition.newBuilder()
                .setResourceType(ResourceType.MODEL_REPOSITORY)
                .setProtocol("local")
                .putProperties("repoUrl", platform.tracRepoDir().toString());

        adminClient.createConfigObject(ConfigWriteRequest.newBuilder()
                .setTenant(LIVE_TENANT)
                .setConfigClass(ConfigKeys.TRAC_RESOURCES)
                .setConfigKey(LOCAL_REPO)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.RESOURCE)
                        .setResource(repoResource))
                .build());

        log.info("Uploading data to [{}]...", LIVE_TENANT);

        var content = ByteString.copyFrom("Data in a live tenant", StandardCharsets.UTF_8);

        var fileId = dataClient.createSmallFile(FileWriteRequest.newBuilder()
                .setTenant(LIVE_TENANT)
                .setName("live_tenant.txt")
                .setMimeType("text/plain")
                .setSize(content.size())
                .setContent(content)
                .build());

        var fileContent = dataClient.readSmallFile(FileReadRequest.newBuilder()
                .setTenant(LIVE_TENANT)
                .setSelector(MetadataUtil.selectorFor(fileId))
                .build());

        Assertions.assertEquals(content, fileContent.getContent());

        log.info("Running IMPORT_MODEL job in [{}]...", LIVE_TENANT);

        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository(LOCAL_REPO)
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.schema_files.PnlAggregationSchemas")
                .setVersion(GitHelpers.getCurrentCommit())
                .build();

        var attrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("live_tenant:schema_files"))
                .build());

        var modelTag = Helpers.doModelImport(platform, LIVE_TENANT, modelStub, attrs, attrs);

        Assertions.assertEquals("tutorial.schema_files.PnlAggregationSchemas", modelTag.getDefinition().getModel().getEntryPoint());
    }

    private static void createStorageParents(PlatformTest platform) {

        try {
            Files.createDirectories(platform.workingDir().resolve("tenant_storage"));
            Files.createDirectories(platform.workingDir().resolve("tenant_results"));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void waitForStorage(TracAdminApiGrpc.TracAdminApiBlockingStub adminClient) throws InterruptedException {

        var request = CheckTenantStorageRequest.newBuilder().setTenantCode(LIVE_TENANT).build();
        var deadline = Instant.now().plus(STORAGE_TIMEOUT);

        while (true) {

            var storage = adminClient.checkTenantStorage(request);

            if (storage.getStorageCount() > 0 && storage.getStorageMap().values().stream().allMatch(present -> present))
                return;

            if (Instant.now().isAfter(deadline))
                Assertions.fail("Timed out waiting for tenant storage: " + storage);

            Thread.sleep(250);
        }
    }
}
