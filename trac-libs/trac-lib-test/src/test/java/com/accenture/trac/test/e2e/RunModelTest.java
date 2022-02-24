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

import com.accenture.trac.api.*;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.startup.StandardArgs;
import com.accenture.trac.deploy.metadb.DeployMetaDB;
import com.accenture.trac.gateway.TracPlatformGateway;
import com.accenture.trac.metadata.*;
import com.accenture.trac.svc.data.TracDataService;
import com.accenture.trac.svc.meta.TracMetadataService;
import com.accenture.trac.svc.orch.TracOrchestratorService;
import com.accenture.trac.test.config.ConfigHelpers;
import com.accenture.trac.test.helpers.ServiceHelpers;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


@Tag("integration")
@Tag("int-e2e")
public class RunModelTest {

    static final String TRAC_UNIT_CONFIG = "config/trac-unit.yaml";
    static final String TEST_TENANT = "ACME_CORP";

    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
    private static final String PYTHON_EXE = IS_WINDOWS ? "python.exe" : "python";
    private static final String VENV_BIN_SUBDIR = IS_WINDOWS ? "Scripts" : "bin";
    private static final String VENV_ENV_VAR = "VIRTUAL_ENV";

    final Logger log = LoggerFactory.getLogger(getClass());

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

        System.out.println("Setting up Python venv for local executor...");

        var venvPath = tracDir.resolve("venv");
        var venvPb = new ProcessBuilder();
        venvPb.command("python", "-m", "venv", venvPath.toString());

        var venvP = venvPb.start();
        venvP.waitFor(60, TimeUnit.SECONDS);

        System.out.println("Installing the TRAC runtime for Python...");

        // This assumes the runtime has already been built externally (requires full the Python build chain)

        var pythonExe = venvPath
                .resolve(VENV_BIN_SUBDIR)
                .resolve(PYTHON_EXE)
                .toString();

        var tracRtDistDir = Paths.get(".")
                .toAbsolutePath()
                .normalize()
                .resolve("../../trac-runtime/python/build/dist");

        var tracRtWhl = Files.find(tracRtDistDir, 1, (file, attrs) -> file.getFileName().toString().endsWith(".whl"))
                .findFirst();

        if (tracRtWhl.isEmpty())
            throw new RuntimeException("Could not find TRAC runtime wheel");

        var pipPB = new ProcessBuilder();
        pipPB.command(pythonExe, "-m", "pip", "install", tracRtWhl.get().toString());
        pipPB.environment().put(VENV_ENV_VAR, venvPath.toString());

        var pipP = pipPB.start();
        pipP.waitFor(2, TimeUnit.MINUTES);
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

    static TagHeader inputDataId;
    static TagHeader modelId;

    @Test @Order(1)
    void loadInputData() {

        log.info("Loading input data...");

        var inputSchema = SchemaDefinition.newBuilder()
                .build();

        var inputBytes = ByteString.copyFrom(new byte[0]);

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(inputSchema)
                .setFormat("text/csv")
                .setContent(inputBytes)
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_dataset")
                        .setValue(MetadataCodec.encodeValue("run_model:customer_loans")))
                .build();

        inputDataId = dataClient.createSmallDataset(writeRequest);

        log.info("Input data loaded, data ID = [{}]", inputDataId.getObjectId());
    }

    @Test @Order(2)
    void importModel() {

        log.info("Running IMPORT_MODEL job...");

        var importModel = ImportModelJob.newBuilder()
                .setLanguage("python")
                .setRepository("trac_git_repo")   // TODO
                .setPath("examples/models/python/using_data")
                .setEntryPoint("using_data.UsingDataModel")
                .setVersion("main")
                .addModelAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_model")
                        .setValue(MetadataCodec.encodeValue("run_model:using_data")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_MODEL)
                .setImportModel(importModel))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_model:import_model")))
                .build();

        var jobStatus = orchClient.submitJob(jobRequest);

        log.info("Job status: {}", jobStatus.toString());

        var jobId = jobStatus.getJobId();
    }
}
