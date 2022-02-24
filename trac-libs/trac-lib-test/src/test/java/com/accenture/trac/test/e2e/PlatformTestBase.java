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
import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.plugin.PluginManager;
import com.accenture.trac.common.startup.StandardArgs;
import com.accenture.trac.config.InstanceConfig;
import com.accenture.trac.config.PlatformConfig;
import com.accenture.trac.deploy.metadb.DeployMetaDB;
import com.accenture.trac.svc.data.TracDataService;
import com.accenture.trac.svc.meta.TracMetadataService;
import com.accenture.trac.svc.orch.TracOrchestratorService;
import com.accenture.trac.test.config.ConfigHelpers;
import com.accenture.trac.test.helpers.ServiceHelpers;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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

public class PlatformTestBase {

    public static final String TRAC_UNIT_CONFIG = "config/trac-unit.yaml";
    public static final String TRAC_EXEC_DIR = "TRAC_EXEC_DIR";
    public static final String TEST_TENANT = "ACME_CORP";

    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
    private static final String PYTHON_EXE = IS_WINDOWS ? "python.exe" : "python";
    private static final String VENV_BIN_SUBDIR = IS_WINDOWS ? "Scripts" : "bin";
    private static final String VENV_ENV_VAR = "VIRTUAL_ENV";
    private static final String TRAC_RUNTIME_DIST_DIR = "trac-runtime/python/build/dist";

    protected static final Logger log = LoggerFactory.getLogger(PlatformTestBase.class);

    static Path tracRepoDir;

    @TempDir
    static Path tracDir;
    static Path tracExecDir;
    static URL platformConfigUrl;
    static String keystoreKey;
    static PlatformConfig platformConfig;

    static TracMetadataService metaSvc;
    static TracDataService dataSvc;
    static TracOrchestratorService orchSvc;

    ManagedChannel metaChannel;
    ManagedChannel dataChannel;
    ManagedChannel orchChannel;

    TracMetadataApiGrpc.TracMetadataApiBlockingStub metaClient;
    TracDataApiGrpc.TracDataApiBlockingStub dataClient;
    TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClient;

    @BeforeAll
    static void setupClass() throws Exception {

        tracRepoDir = Paths.get(".").toAbsolutePath();

        while (!Files.exists(tracRepoDir.resolve("trac-api")))
            tracRepoDir = tracRepoDir.getParent();

        prepareConfig();
        prepareDatabase();
        preparePlugins();

        startServices();
    }

    @AfterAll
    static void tearDownClass() {

        stopServices();
    }

    @BeforeEach
    void setup() {

        startClients();
    }

    @AfterEach
    void tearDown() throws Exception {

        stopClients();
    }

    static void prepareConfig() throws Exception {

        log.info("Prepare config for platform testing...");

        tracExecDir = System.getenv().containsKey(TRAC_EXEC_DIR)
                ? Paths.get(System.getenv(TRAC_EXEC_DIR))
                : tracDir;

        var substitutions = Map.of(
                "${TRAC_DIR}", tracDir.toString().replace("\\", "\\\\"),
                "${TRAC_EXEC_DIR}", tracExecDir.toString().replace("\\", "\\\\"));

        platformConfigUrl = ConfigHelpers.prepareConfig(
                TRAC_UNIT_CONFIG, tracDir,
                substitutions);

        keystoreKey = "";  // not yet used


        var plugins = new PluginManager();
        plugins.initConfigPlugins();

        var config = new ConfigManager(platformConfigUrl.toString(), tracDir, plugins);
        platformConfig = config.loadRootConfigObject(PlatformConfig.class);
    }

    static void prepareDatabase() {

        log.info("Deploy database schema...");

        var databaseTasks = List.of(
                StandardArgs.task(DeployMetaDB.DEPLOY_SCHEMA_TASK_NAME, "", ""),
                StandardArgs.task(DeployMetaDB.ADD_TENANT_TASK_NAME, TEST_TENANT, ""));

        ServiceHelpers.runDbDeploy(tracDir, platformConfigUrl, keystoreKey, databaseTasks);
    }

    static void preparePlugins() throws Exception {

        // TODO: Replace this with extensions, to run E2E tests over different backend configurations

        Files.createDirectory(tracDir.resolve("unit_test_storage"));

        var venvDir = tracExecDir.resolve("venv").normalize();

        if (Files.exists(venvDir)) {

            log.info("Using existing venv: [{}]", venvDir);
        }
        else {
            log.info("Creating a new venv: [{}]", venvDir);

            var venvPath = tracDir.resolve("venv");
            var venvPb = new ProcessBuilder();
            venvPb.command("python", "-m", "venv", venvPath.toString());

            var venvP = venvPb.start();
            venvP.waitFor(60, TimeUnit.SECONDS);

            log.info("Installing TRAC runtime for Python...");

            // This assumes the runtime has already been built externally (requires full the Python build chain)

            var pythonExe = venvPath
                    .resolve(VENV_BIN_SUBDIR)
                    .resolve(PYTHON_EXE)
                    .toString();

            var tracRtDistDir = tracRepoDir.resolve(TRAC_RUNTIME_DIST_DIR);
            var tracRtWhl = Files.find(tracRtDistDir, 1, (file, attrs) -> file.toString().endsWith(".whl"))
                    .findFirst();

            if (tracRtWhl.isEmpty())
                throw new RuntimeException("Could not find TRAC runtime wheel");

            var pipPB = new ProcessBuilder();
            pipPB.command(pythonExe, "-m", "pip", "install", tracRtWhl.get().toString());
            pipPB.environment().put(VENV_ENV_VAR, venvPath.toString());

            var pipP = pipPB.start();
            pipP.waitFor(2, TimeUnit.MINUTES);

        }
    }

    static void startServices() {

        metaSvc = ServiceHelpers.startService(TracMetadataService.class, tracDir, platformConfigUrl, keystoreKey);
        dataSvc = ServiceHelpers.startService(TracDataService.class, tracDir, platformConfigUrl, keystoreKey);
        orchSvc = ServiceHelpers.startService(TracOrchestratorService.class, tracDir, platformConfigUrl, keystoreKey);
    }

    static void stopServices() {

        if (orchSvc != null)
            orchSvc.stop();

        if (dataSvc != null)
            dataSvc.stop();

        if (metaSvc != null)
            metaSvc.stop();
    }

    void startClients() {

        metaChannel = channelForInstance(platformConfig.getInstances().getMeta(0));
        dataChannel = channelForInstance(platformConfig.getInstances().getData(0));
        orchChannel = channelForInstance(platformConfig.getInstances().getOrch(0));

        metaClient = TracMetadataApiGrpc.newBlockingStub(metaChannel);
        dataClient = TracDataApiGrpc.newBlockingStub(dataChannel);
        orchClient = TracOrchestratorApiGrpc.newBlockingStub(orchChannel);
    }

    void stopClients() throws Exception {

        orchChannel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
        dataChannel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
        metaChannel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
    }

    static ManagedChannel channelForInstance(InstanceConfig instance) {

        var builder = NettyChannelBuilder.forAddress(instance.getHost(), instance.getPort());

        if (instance.getScheme().equalsIgnoreCase("HTTP"))
            builder.usePlaintext();

        builder.directExecutor();

        return builder.build();
    }
}
