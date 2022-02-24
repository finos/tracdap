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

package com.accenture.trac.test.e2e;

import com.accenture.trac.api.TracDataApiGrpc;
import com.accenture.trac.api.TracMetadataApiGrpc;
import com.accenture.trac.api.TracOrchestratorApiGrpc;
import com.accenture.trac.common.startup.StandardArgs;
import com.accenture.trac.deploy.metadb.DeployMetaDB;
import com.accenture.trac.gateway.TracPlatformGateway;
import com.accenture.trac.svc.data.TracDataService;
import com.accenture.trac.svc.meta.TracMetadataService;
import com.accenture.trac.svc.orch.TracOrchestratorService;
import com.accenture.trac.test.config.ConfigHelpers;
import com.accenture.trac.test.helpers.ServiceHelpers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;


@Tag("integration")
public class RunModelTest {

    static final String TRAC_UNIT_CONFIG = "config/trac-unit.yaml";
    static final String TEST_TENANT = "ACME_CORP";

    @TempDir
    static Path tracDir;
    static URL platformConfig;
    static URL gatewayConfig;
    static String keystoreKey;

    static TracMetadataService metaSvc;
    static TracDataService dataSvc;
    static TracOrchestratorService orchSvc;
    static TracPlatformGateway gateway;

    TracMetadataApiGrpc.TracMetadataApiBlockingStub metaClient;
    TracDataApiGrpc.TracDataApiBlockingStub dataClient;
    TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClient;

    @BeforeAll
    static void setupClass() throws Exception {

        var substitutions = Map.of(
                "${TRAC_RUN_DIR}", tracDir.toString().replace("\\", "\\\\"));

        platformConfig = ConfigHelpers.prepareConfig(
                TRAC_UNIT_CONFIG, tracDir,
                substitutions);

        keystoreKey = "";  // not yet used

        var databaseTasks = List.of(
                StandardArgs.task(DeployMetaDB.DEPLOY_SCHEMA_TASK_NAME, "", ""),
                StandardArgs.task(DeployMetaDB.ADD_TENANT_TASK_NAME, TEST_TENANT, ""));

        ServiceHelpers.runDbDeploy(tracDir, platformConfig, keystoreKey, databaseTasks);

        preparePlugins();

        startServices();
    }

    static void preparePlugins() throws Exception {

        // TODO: Replace this with extensions, to run E2E tests over different backend configurations

        Files.createDirectory(tracDir.resolve("unit_test_storage"));
    }

    static void startServices() {

        metaSvc = ServiceHelpers.startService(TracMetadataService.class, tracDir, platformConfig, keystoreKey);
        dataSvc = ServiceHelpers.startService(TracDataService.class, tracDir, platformConfig, keystoreKey);
        orchSvc = ServiceHelpers.startService(TracOrchestratorService.class, tracDir, platformConfig, keystoreKey);
    }

    @AfterAll
    static void stopServices() {

        if (orchSvc != null)
            orchSvc.stop();

        if (dataSvc != null)
            dataSvc.stop();

        if (metaSvc != null)
            metaSvc.stop();
    }

    @Test
    void test1() {

    }
}
