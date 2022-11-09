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

package org.finos.tracdap.test.helpers;

import io.grpc.*;
import org.finos.tracdap.api.TracDataApiGrpc;
import org.finos.tracdap.api.TracMetadataApiGrpc;
import org.finos.tracdap.api.TracOrchestratorApiGrpc;
import org.finos.tracdap.api.TrustedMetadataApiGrpc;
import org.finos.tracdap.common.auth.GrpcClientAuth;
import org.finos.tracdap.common.auth.JwtProcessor;
import org.finos.tracdap.common.auth.UserInfo;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.startup.StandardArgs;
import org.finos.tracdap.config.InstanceConfig;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.tools.auth.AuthTool;
import org.finos.tracdap.tools.deploy.metadb.DeployMetaDB;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.config.ConfigHelpers;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class PlatformTest implements BeforeAllCallback, AfterAllCallback {

    public static final String TRAC_EXEC_DIR = "TRAC_EXEC_DIR";
    public static final String STORAGE_ROOT_DIR = "storage_root";
    public static final String DEFAULT_STORAGE_FORMAT = "ARROW_FILE";

    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
    private static final String PYTHON_EXE = IS_WINDOWS ? "python.exe" : "python";
    private static final String VENV_BIN_SUBDIR = IS_WINDOWS ? "Scripts" : "bin";
    private static final String VENV_ENV_VAR = "VIRTUAL_ENV";
    private static final String TRAC_RUNTIME_DIST_DIR = "tracdap-runtime/python/build/dist";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String testConfig;
    private final List<String> tenants;
    private final String storageFormat;

    private final boolean runDbDeploy;
    private final boolean startMeta;
    private final boolean startData;
    private final boolean startOrch;

    private String authToken;

    private PlatformTest(
            String testConfig, List<String> tenants, String storageFormat,
            boolean runDbDeploy, boolean startMeta, boolean startData, boolean startOrch) {

        this.testConfig = testConfig;
        this.tenants = tenants;
        this.storageFormat = storageFormat;
        this.runDbDeploy = runDbDeploy;
        this.startMeta = startMeta;
        this.startData = startData;
        this.startOrch = startOrch;
    }

    public static Builder forConfig(String testConfig) {
        var builder = new Builder();
        builder.testConfig = testConfig;
        return builder;
    }

    public static class Builder {

        private String testConfig;
        private final List<String> tenants = new ArrayList<>();
        private String storageFormat = DEFAULT_STORAGE_FORMAT;
        private boolean runDbDeploy = true;  // Run DB deploy by default
        private boolean startMeta;
        private boolean startData;
        private boolean startOrch;

        public Builder addTenant(String testTenant) { this.tenants.add(testTenant); return this; }
        public Builder storageFormat(String storageFormat) { this.storageFormat = storageFormat; return this; }
        public Builder runDbDeploy(boolean runDbDeploy) { this.runDbDeploy = runDbDeploy; return this; }
        public Builder startMeta() { startMeta = true; return this; }
        public Builder startData() { startData = true; return this; }
        public Builder startOrch() { startOrch = true; return this; }
        public Builder startAll() { return startMeta().startData().startOrch(); }

        public PlatformTest build() {

            return new PlatformTest(
                    testConfig, tenants, storageFormat,
                    runDbDeploy, startMeta, startData, startOrch);
        }
    }

    private Path tracDir;
    private Path tracStorageDir;
    private Path tracExecDir;
    private Path tracRepoDir;
    private URL platformConfigUrl;
    private String keystoreKey;
    private PlatformConfig platformConfig;

    private TracMetadataService metaSvc;
    private TracDataService dataSvc;
    private TracOrchestratorService orchSvc;

    private ManagedChannel metaChannel;
    private ManagedChannel dataChannel;
    private ManagedChannel orchChannel;

    public TracMetadataApiGrpc.TracMetadataApiFutureStub metaClientFuture() {
        var userCreds = new GrpcClientAuth(authToken);
        return TracMetadataApiGrpc.newFutureStub(metaChannel).withCallCredentials(userCreds);
    }

    public TracMetadataApiGrpc.TracMetadataApiBlockingStub metaClientBlocking() {
        var userCreds = new GrpcClientAuth(authToken);
        return TracMetadataApiGrpc.newBlockingStub(metaChannel).withCallCredentials(userCreds);
    }

    public TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub metaClientTrustedBlocking() {
        var userCreds = new GrpcClientAuth(authToken);
        return TrustedMetadataApiGrpc.newBlockingStub(metaChannel).withCallCredentials(userCreds);
    }

    public TracDataApiGrpc.TracDataApiStub dataClient() {
        var userCreds = new GrpcClientAuth(authToken);
        return TracDataApiGrpc.newStub(dataChannel).withCallCredentials(userCreds);
    }

    public TracDataApiGrpc.TracDataApiBlockingStub dataClientBlocking() {
        var userCreds = new GrpcClientAuth(authToken);
        return TracDataApiGrpc.newBlockingStub(dataChannel).withCallCredentials(userCreds);
    }

    public TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClientBlocking() {
        var userCreds = new GrpcClientAuth(authToken);
        return TracOrchestratorApiGrpc.newBlockingStub(orchChannel).withCallCredentials(userCreds);
    }

    public Path storageRootDir() {
        return tracStorageDir;
    }

    public Path tracRepoDir() {
        return tracRepoDir;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        findDirectories();
        prepareConfig();
        preparePlugins();

        prepareAuth();

        if (runDbDeploy)
            prepareDatabase();

        startServices();
        startClients();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {

        stopClients();
        stopServices();

        cleanupDirectories();
    }

    void findDirectories() throws Exception {

        tracDir = Files.createTempDirectory("trac_platform_test_");

        tracStorageDir = tracDir.resolve(STORAGE_ROOT_DIR);
        Files.createDirectory(tracStorageDir);

        tracExecDir = System.getenv().containsKey(TRAC_EXEC_DIR)
                ? Paths.get(System.getenv(TRAC_EXEC_DIR))
                : tracDir;

        tracRepoDir = Paths.get(".").toAbsolutePath();

        while (!Files.exists(tracRepoDir.resolve("tracdap-api")))
            tracRepoDir = tracRepoDir.getParent();
    }

    void cleanupDirectories() {

        try (var walk = Files.walk(tracDir)) {

            walk.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        catch (IOException e) {

            log.warn("Failed to clean up test files: " + e.getMessage(), e);
        }
    }

    void prepareConfig() throws Exception {

        log.info("Prepare config for platform testing...");

        // Git is not available in CI for tests run inside containers
        // So, only look up the current repo if it is needed by the orchestrator
        // To run orchestrator tests in a container, we'd need to pass the repo URL in, e.g. with an env var from CI
        String currentGitOrigin = startOrch
                ? getCurrentGitOrigin()
                : "git_repo_not_configured";

        // Substitutions are used by template config files in test resources
        // But it is harmless to apply them to fully defined config files as well
        var substitutions = Map.of(
                "${TRAC_DIR}", tracDir.toString().replace("\\", "\\\\"),
                "${TRAC_STORAGE_DIR}", tracStorageDir.toString().replace("\\", "\\\\"),
                "${STORAGE_FORMAT}", storageFormat,
                "${TRAC_EXEC_DIR}", tracExecDir.toString().replace("\\", "\\\\"),
                "${TRAC_LOCAL_REPO}", tracRepoDir.toString(),
                "${TRAC_GIT_REPO}", currentGitOrigin);

        platformConfigUrl = ConfigHelpers.prepareConfig(
                testConfig, tracDir,
                substitutions);

        // Used for storing and accessing secrets
        // Core auth setup is mandatory and needs this to be set
        keystoreKey = "d7xbeK-julOi8-bBwd9k";


        var plugins = new PluginManager();
        plugins.initConfigPlugins();

        var config = new ConfigManager(platformConfigUrl.toString(), tracDir, plugins);
        platformConfig = config.loadRootConfigObject(PlatformConfig.class);
    }

    private String getCurrentGitOrigin() throws Exception {

        var pb = new ProcessBuilder();
        pb.command("git", "config", "--get", "remote.origin.url");

        var proc = pb.start();

        try {
            proc.waitFor(10, TimeUnit.SECONDS);

            var procResult = proc.getInputStream().readAllBytes();
            var origin = new String(procResult, StandardCharsets.UTF_8).strip();

            log.info("Using Git origin: {}", origin);

            return origin;
        }
        finally {
            proc.destroy();
        }
    }

    void prepareAuth() {

        log.info("Running auth tool to set up root authentication keys...");

        // Running the auth tool will create the secrets file and add the public / private keys for auth

        var authTasks = new ArrayList<StandardArgs.Task>();
        authTasks.add(StandardArgs.task(AuthTool.SIGNING_KEY_TASK, List.of("EC", "256"), ""));
        ServiceHelpers.runAuthTool(tracDir, platformConfigUrl, keystoreKey, authTasks);

        // Authentication is mandatory, so we need to build a token in order to test at the API level
        // To create a valid token, we need to get the auth signing keys out of the secrets file
        // Tokens must be signed with the same key used by the platform services

        var pluginMgr = new PluginManager();
        pluginMgr.initConfigPlugins();

        var configMgr = new ConfigManager(
                platformConfigUrl.toString(),
                Paths.get(platformConfigUrl.toString()).getParent(),
                pluginMgr, keystoreKey);

        configMgr.prepareSecrets();

        var platformConfig = configMgr.loadRootConfigObject(PlatformConfig.class);
        var authConfig = platformConfig.getAuthentication();

        var publicKey = configMgr.loadPublicKey(ConfigKeys.TRAC_AUTH_PUBLIC_KEY);
        var privateKey = configMgr.loadPrivateKey(ConfigKeys.TRAC_AUTH_PRIVATE_KEY);
        var keyPair = new KeyPair(publicKey, privateKey);
        var jwt = JwtProcessor.configure(authConfig, keyPair);

        var userInfo = new UserInfo();
        userInfo.setUserId("platform_testing");
        userInfo.setDisplayName("Platform testing user");

        authToken = jwt.encodeToken(userInfo);
    }

    void prepareDatabase() {

        log.info("Deploy database schema...");

        var databaseTasks = new ArrayList<StandardArgs.Task>();
        databaseTasks.add(StandardArgs.task(DeployMetaDB.DEPLOY_SCHEMA_TASK, "", ""));

        for (var tenant : tenants) {

            // Run both add and alter tenant tasks as part of the standard setup
            // (just to run both tasks, not strictly necessary)

            var description = "Test tenant [" + tenant + "]";
            databaseTasks.add(StandardArgs.task(DeployMetaDB.ADD_TENANT_TASK, List.of(tenant, description), ""));
            databaseTasks.add(StandardArgs.task(DeployMetaDB.ALTER_TENANT_TASK, List.of(tenant, description), ""));
        }

        ServiceHelpers.runDbDeploy(tracDir, platformConfigUrl, keystoreKey, databaseTasks);
    }

    void preparePlugins() throws Exception {

        // TODO: Allow running whole-platform tests over different backend configurations

        if (startData) {
            Files.createDirectory(tracDir.resolve("unit_test_storage"));
        }

        if (startOrch) {

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
    }

    void startServices() {

        if (startMeta)
            metaSvc = ServiceHelpers.startService(TracMetadataService.class, tracDir, platformConfigUrl, keystoreKey);

        if (startData)
            dataSvc = ServiceHelpers.startService(TracDataService.class, tracDir, platformConfigUrl, keystoreKey);

        if (startOrch)
            orchSvc = ServiceHelpers.startService(TracOrchestratorService.class, tracDir, platformConfigUrl, keystoreKey);
    }

    void stopServices() {

        if (orchSvc != null)
            orchSvc.stop();

        if (dataSvc != null)
            dataSvc.stop();

        if (metaSvc != null)
            metaSvc.stop();
    }

    void startClients() {

        if (startMeta)
            metaChannel = channelForInstance(platformConfig.getInstances().getMeta(0));

        if (startData)
            dataChannel = channelForInstance(platformConfig.getInstances().getData(0));

        if (startOrch)
            orchChannel = channelForInstance(platformConfig.getInstances().getOrch(0));
    }

    void stopClients() throws Exception {

        if (orchChannel != null)
            orchChannel.shutdown().awaitTermination(10, TimeUnit.SECONDS);

        if (dataChannel != null)
            dataChannel.shutdown().awaitTermination(10, TimeUnit.SECONDS);

        if (metaChannel != null)
            metaChannel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
    }

    ManagedChannel channelForInstance(InstanceConfig instance) {

        var builder = NettyChannelBuilder.forAddress(instance.getHost(), instance.getPort());

        if (instance.getScheme().equalsIgnoreCase("HTTP"))
            builder.usePlaintext();

        builder.directExecutor();

        return builder.build();
    }
}
