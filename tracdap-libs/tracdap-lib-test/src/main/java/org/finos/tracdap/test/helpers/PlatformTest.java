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

package org.finos.tracdap.test.helpers;

import org.finos.tracdap.api.ConfigWriteRequest;
import org.finos.tracdap.api.TracAdminApiGrpc;
import org.finos.tracdap.api.TracDataApiGrpc;
import org.finos.tracdap.api.TracMetadataApiGrpc;
import org.finos.tracdap.api.TracOrchestratorApiGrpc;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.middleware.CommonConcerns;
import org.finos.tracdap.common.middleware.CommonGrpcConcerns;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.TracServiceBase;
import org.finos.tracdap.common.startup.StandardArgs;
import org.finos.tracdap.common.util.RoutingUtils;
import org.finos.tracdap.config.DynamicConfig;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.tools.deploy.DeployTool;
import org.finos.tracdap.tools.secrets.SecretTool;
import org.finos.tracdap.test.config.ConfigHelpers;

import io.grpc.*;
import io.grpc.stub.AbstractStub;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;


public class PlatformTest implements BeforeAllCallback, AfterAllCallback {

    public static final Map<String, String> SERVICE_KEYS = Map.of(
            "TracPlatformGateway", ConfigKeys.GATEWAY_SERVICE_KEY,
            "TracMetadataService", ConfigKeys.METADATA_SERVICE_KEY,
            "TracDataService", ConfigKeys.DATA_SERVICE_KEY,
            "TracOrchestratorService", ConfigKeys.ORCHESTRATOR_SERVICE_KEY,
            "TracAdminService", ConfigKeys.ADMIN_SERVICE_KEY);

    public static final String TRAC_EXEC_DIR = "TRAC_EXEC_DIR";
    public static final String STORAGE_ROOT_DIR = "storage_root";
    public static final String DEFAULT_STORAGE_FORMAT = "ARROW_FILE";

    private static final String SECRET_KEY_ENV_VAR = "TRAC_SECRET_KEY";
    private static final String SECRET_KEY_DEFAULT = "d7xbeK-julOi8-bBwd9k";

    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
    private static final String PYTHON_EXE = IS_WINDOWS ? "python.exe" : "python";
    private static final String VENV_BIN_SUBDIR = IS_WINDOWS ? "Scripts" : "bin";
    private static final String VENV_ENV_VAR = "VIRTUAL_ENV";
    private static final String TRAC_RUNTIME_DIST_DIR = "tracdap-runtime/python/build/dist";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String testConfig;
    private final Map<String, String> tenants;
    private final String storageFormat;

    private final boolean runDbDeploy;
    private final boolean manageDataPrefix;
    private final boolean localExecutor;

    private final List<Class<? extends TracServiceBase>> serviceClasses;
    private final Map<String, String> serviceKeys;
    private final GrpcConcern clientConcerns;
    private final List<Consumer<PlatformTest>> preStartActions;

    private PlatformTest(
            String testConfig, String secretKey, Map<String, String> tenants, String storageFormat,
            boolean runDbDeploy, boolean manageDataPrefix, boolean localExecutor,
            List<Class<? extends TracServiceBase>> serviceClasses, Map<String, String> serviceKeys,
            GrpcConcern clientConcerns, List<Consumer<PlatformTest>> preStartActions) {

        this.testConfig = testConfig;
        this.tenants = tenants;
        this.storageFormat = storageFormat;
        this.runDbDeploy = runDbDeploy;
        this.manageDataPrefix = manageDataPrefix;
        this.localExecutor = localExecutor;
        this.serviceClasses = serviceClasses;
        this.serviceKeys = serviceKeys;
        this.clientConcerns = clientConcerns;
        this.preStartActions = preStartActions;

        // Secret key can be set here, otherwise it is discovered later
        this.secretKey = secretKey;

        this.services = new ArrayList<>(serviceClasses.size());
        this.serviceChannels = new HashMap<>(serviceClasses.size());
    }

    public static Builder forConfig(String testConfig) {
        return forConfig(testConfig, "");
    }

    public static Builder forConfig(String testConfig, String secretKey) {
        var builder = new Builder();
        builder.testConfig = testConfig;
        builder.secretKey = secretKey;
        return builder;
    }

    public static class Builder {

        private String testConfig;
        private String secretKey;
        private final Map<String, String> tenants = new HashMap<>();
        private String storageFormat = DEFAULT_STORAGE_FORMAT;
        private boolean runDbDeploy = false;
        private boolean manageDataPrefix = false;
        private boolean localExecutor = false;
        private final List<Class<? extends TracServiceBase>> serviceClasses = new ArrayList<>();
        private final CommonConcerns<GrpcConcern> clientConcerns = new CommonGrpcConcerns("client_concerns");
        private final List<Consumer<PlatformTest>> preStartActions = new ArrayList<>();
        private final Map<String, String> serviceKeys = new HashMap<>();

        // Should client concerns be pre-configured? If so what is the right configuration for testing?

        public Builder addTenant(String testTenant) { this.tenants.put(testTenant, null); return this; }
        public Builder bootstrapTenant(String testTenant, String bootstrapConfig) { this.tenants.put(testTenant, bootstrapConfig); return this; }
        public Builder storageFormat(String storageFormat) { this.storageFormat = storageFormat; return this; }
        public Builder runDbDeploy(boolean runDbDeploy) { this.runDbDeploy = runDbDeploy; return this; }
        public Builder manageDataPrefix(boolean manageDataPrefix) { this.manageDataPrefix = manageDataPrefix; return this; }
        public Builder prepareLocalExecutor(boolean localExecutor) { this.localExecutor = localExecutor; return this; }
        public Builder startService(Class<? extends TracServiceBase> serviceClass) { addService(serviceClass, null); return this; }
        public Builder startService(Class<? extends TracServiceBase> serviceClass, String serviceKey) { addService(serviceClass, serviceKey); return this; }
        public Builder clientConcern(GrpcConcern concern) { clientConcerns.addLast(concern); return this; }
        public Builder preStartAction(Consumer<PlatformTest> action) { this.preStartActions.add(action); return this; }

        public PlatformTest build() {

            return new PlatformTest(
                    testConfig, secretKey, tenants, storageFormat,
                    runDbDeploy, manageDataPrefix, localExecutor,
                    serviceClasses, serviceKeys,
                    clientConcerns.build(), preStartActions);
        }

        private void addService(Class<? extends TracServiceBase> serviceClass, String serviceKey) {
            serviceClasses.add(serviceClass);
            if (serviceKey != null && !serviceKey.isEmpty())
                serviceKeys.put(serviceClass.getSimpleName(), serviceKey);
            else if (SERVICE_KEYS.containsKey(serviceClass.getSimpleName()))
                serviceKeys.put(serviceClass.getSimpleName(), SERVICE_KEYS.get(serviceClass.getSimpleName()));
            else
                throw new ETracInternal("Service key not provided for service class [" + serviceClass.getSimpleName() + "]");
        }
    }

    private final List<TracServiceBase> services;
    private final Map<String, ManagedChannel> serviceChannels;

    private String testId;
    private Path workingDir;
    private Path storageDir;
    private Path executorDir;
    private Path tracRepoDir;
    private URL platformConfigUrl;
    private String secretKey;
    private PluginManager pluginManager;
    private ConfigManager configManager;
    private PlatformConfig platformConfig;

    public boolean hasService(String serviceKey) {
        return serviceKeys.containsValue(serviceKey);
    }

    public <TClient extends AbstractStub<TClient>> TClient
    createClient(String serviceKey, Function<Channel, TClient> clientFactory) {
        if (!serviceChannels.containsKey(serviceKey))
            throw new ETracInternal("Client not started for service key [" + serviceKey + "]");
        var channel = serviceChannels.get(serviceKey);
        var client = clientFactory.apply(channel);
        return clientConcerns.configureClient(client);
    }

    public TracMetadataApiGrpc.TracMetadataApiFutureStub metaClientFuture() {
        return createClient(ConfigKeys.METADATA_SERVICE_KEY, TracMetadataApiGrpc::newFutureStub);
    }

    public TracMetadataApiGrpc.TracMetadataApiBlockingStub metaClientBlocking() {
        return createClient(ConfigKeys.METADATA_SERVICE_KEY, TracMetadataApiGrpc::newBlockingStub);
    }

    public InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClientInternalBlocking() {
        return createClient(ConfigKeys.METADATA_SERVICE_KEY, InternalMetadataApiGrpc::newBlockingStub);
    }

    public TracDataApiGrpc.TracDataApiStub dataClient() {
        return createClient(ConfigKeys.DATA_SERVICE_KEY, TracDataApiGrpc::newStub);
    }

    public TracDataApiGrpc.TracDataApiBlockingStub dataClientBlocking() {
        return createClient(ConfigKeys.DATA_SERVICE_KEY, TracDataApiGrpc::newBlockingStub);
    }

    public TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClientBlocking() {
        return createClient(ConfigKeys.ORCHESTRATOR_SERVICE_KEY, TracOrchestratorApiGrpc::newBlockingStub);
    }

    public TracAdminApiGrpc.TracAdminApiBlockingStub adminClientBlocking() {
        var channel = serviceChannels.get(ConfigKeys.METADATA_SERVICE_KEY);
        var client = TracAdminApiGrpc.newBlockingStub(channel);
        return clientConcerns.configureClient(client);
    }

    public Path workingDir() {
        return workingDir;
    }

    public Path tracRepoDir() {
        return tracRepoDir;
    }

    public URL platformConfigUrl() {
        return platformConfigUrl;
    }

    public PluginManager pluginManager() {
        return pluginManager;
    }

    public ConfigManager configManager() {
        return configManager;
    }

    public PlatformConfig platformConfig() {
        return platformConfig;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        setTestId();
        findDirectories();
        createTestConfig();
        runPreStartActions();

        loadPluginsAndConfig();

        if (runDbDeploy)
            prepareDatabase();

        prepareDataAndExecutor();

        if (manageDataPrefix)
            prepareDataPrefix();

        startServices();
        startClients();

        loadDynamicConfig();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {

        stopClients();
        stopServices();

        if (manageDataPrefix)
            cleanupDataPrefix();

        cleanupDirectories();
    }

    void setTestId() {

        var timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()).replace(':', '.');
        var random = new Random().nextLong();

        testId = String.format("%s_0x%h", timestamp, random);
    }

    void findDirectories() throws Exception {

        workingDir = Files.createTempDirectory("trac_platform_test_");

        storageDir = workingDir.resolve(STORAGE_ROOT_DIR);
        Files.createDirectory(storageDir);

        executorDir = System.getenv().containsKey(TRAC_EXEC_DIR)
                ? Paths.get(System.getenv(TRAC_EXEC_DIR))
                : workingDir;

        tracRepoDir = Paths.get(".").toAbsolutePath();

        while (!Files.exists(tracRepoDir.resolve("tracdap-api")) && !Files.exists(tracRepoDir.resolve(".git")))
            tracRepoDir = tracRepoDir.getParent();
    }

    void cleanupDirectories() {

        try (var walk = Files.walk(workingDir)) {

            walk.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        catch (IOException e) {

            log.warn("Failed to clean up test files: " + e.getMessage(), e);
        }
    }

    void createTestConfig() throws Exception {

        log.info("Prepare config for platform testing...");

        // Git is not available in CI for tests run inside containers
        // So, only look up the current repo if it is needed by the orchestrator
        // To run orchestrator tests in a container, we'd need to pass the repo URL in, e.g. with an env var from CI
        String currentGitOrigin = localExecutor
                ? getCurrentGitOrigin()
                : "git_repo_not_configured";

        // Substitutions are used by template config files in test resources
        // But it is harmless to apply them to fully defined config files as well

        // The substitutions have some special handling in PlatformTest to set them up
        var staticSubstitutions = Map.of(
                "${TRAC_DIR}", workingDir.toString().replace("\\", "\\\\"),
                "${TRAC_STORAGE_DIR}", storageDir.toString().replace("\\", "\\\\"),
                "${TRAC_STORAGE_FORMAT}", storageFormat,
                "${TRAC_EXEC_DIR}", executorDir.toString().replace("\\", "\\\\"),
                "${TRAC_LOCAL_REPO}", tracRepoDir.toString(),
                "${TRAC_GIT_REPO}", currentGitOrigin,
                "${TRAC_TEST_ID}", testId);

        var substitutions = new HashMap<>(staticSubstitutions);

        // Also allow developers to put whatever substitutions they need into the environment
        for (var envVar : System.getenv().entrySet()) {
            if (envVar.getKey().startsWith("TRAC_") && !substitutions.containsKey(envVar.getKey())) {
                var key = String.format("${%s}", envVar.getKey());
                substitutions.put(key, envVar.getValue());
            }
        }

        // Mark all services that are being started as enabled
        for (var serviceKey : serviceKeys.values()) {
            var substitutionKey = String.format("${%s_ENABLED}", serviceKey);
            substitutions.put(substitutionKey, "true");
        }

        // Disable any core services that are in the config but not started for the current test
        for (var serviceKey : SERVICE_KEYS.values()) {
            var substitutionKey = String.format("${%s_ENABLED}", serviceKey);
            if (!substitutions.containsKey(substitutionKey))
                substitutions.put(substitutionKey, "false");
        }

        var configInputs = new ArrayList<String>();
        configInputs.add(testConfig);
        tenants.values().stream().filter(Objects::nonNull).forEach(configInputs::add);
        var configOutputs = ConfigHelpers.prepareConfig(configInputs, workingDir, substitutions);
        platformConfigUrl = configOutputs.get(0);

        // The Secret key is used for storing and accessing secrets
        // If secrets are set up externally, a key can be passed in the env to access the secret store
        // Otherwise the default is used, which is fine if the store is being initialised here

        if (secretKey == null || secretKey.isEmpty()) {
            var env = System.getenv();
            secretKey = env.getOrDefault(SECRET_KEY_ENV_VAR, SECRET_KEY_DEFAULT);
        }
    }

    private void runPreStartActions() {

        for (var action : preStartActions)
            action.accept(this);
    }

    private void loadPluginsAndConfig() {

        pluginManager = new PluginManager();
        pluginManager.registerExtensions();
        pluginManager.initConfigPlugins();

        configManager = new ConfigManager(platformConfigUrl.toString(), workingDir, pluginManager, secretKey);
        platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);

        // If the config uses JKS secrets, create an empty store if none is provided
        var secretType = platformConfig.containsConfig(ConfigKeys.SECRET_TYPE_KEY)
                ? platformConfig.getConfigOrThrow(ConfigKeys.SECRET_TYPE_KEY)
                : null;

        if (secretType != null && List.of("PKCS12", "JCEKS").contains(secretType)) {

            var secretFile = platformConfig.getConfigOrThrow(ConfigKeys.SECRET_URL_KEY);
            var secretUrl = configManager.resolveConfigFile(URI.create(secretFile));

            if (!Files.exists(Paths.get(secretUrl))) {

                log.info("Running secret tool to create an empty secret store...");

                var secretTasks = new ArrayList<StandardArgs.Task>();
                secretTasks.add(StandardArgs.task(SecretTool.INIT_SECRETS, List.of(), "Init secret store"));
                PlatformTestHelpers.runSecretTool(workingDir, platformConfigUrl, secretKey, secretTasks);
            }
        }

        configManager.prepareSecrets();
        pluginManager.initRegularPlugins();
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

    void prepareDatabase() {

        log.info("Deploy database schema...");

        var databaseTasks = new ArrayList<StandardArgs.Task>();
        databaseTasks.add(StandardArgs.task(DeployTool.DEPLOY_SCHEMA_TASK, "", ""));

        for (var tenant : tenants.keySet()) {

            // Run both add and alter tenant tasks as part of the standard setup
            // (just to run both tasks, not strictly necessary)

            var description = "Test tenant [" + tenant + "]";
            databaseTasks.add(StandardArgs.task(DeployTool.ADD_TENANT_TASK, List.of(tenant, description), ""));
            databaseTasks.add(StandardArgs.task(DeployTool.ALTER_TENANT_TASK, List.of(tenant, description), ""));
        }

        PlatformTestHelpers.runDbDeploy(workingDir, platformConfigUrl, secretKey, databaseTasks);
    }

    void prepareDataPrefix() throws Exception {

        var elg = new NioEventLoopGroup(2);

        for (var bootstrapEntry : tenants.entrySet()) {
            if (bootstrapEntry.getValue() != null)
                StorageTestHelpers.createStoragePrefix(configManager, pluginManager, elg, bootstrapEntry.getValue());
        }

        elg.shutdownGracefully();
    }

    void cleanupDataPrefix() throws Exception {

        var elg = new NioEventLoopGroup(2);

        for (var bootstrapEntry : tenants.entrySet()) {
            if (bootstrapEntry.getValue() != null)
                StorageTestHelpers.deleteStoragePrefix(configManager, pluginManager, elg, bootstrapEntry.getValue());
        }

        elg.shutdownGracefully();
    }

    void prepareDataAndExecutor() throws Exception {

        // TODO: Allow running whole-platform tests over different backend configurations

        if (hasService(ConfigKeys.DATA_SERVICE_KEY)) {
            Files.createDirectory(workingDir.resolve("unit_test_storage"));
        }

        if (hasService(ConfigKeys.ORCHESTRATOR_SERVICE_KEY) && localExecutor) {

            var venvDir = executorDir.resolve("venv").normalize();

            if (Files.exists(venvDir)) {

                log.info("Using existing venv: [{}]", venvDir);
            }
            else {
                log.info("Creating a new venv: [{}]", venvDir);

                var venvPath = workingDir.resolve("venv");
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

                // Include optional packages needed for end-to-end testing
                var pipPB = new ProcessBuilder();
                pipPB.command(pythonExe, "-m", "pip", "install", tracRtWhl.get().toString(), "pandas", "polars");
                pipPB.environment().put(VENV_ENV_VAR, venvPath.toString());

                var pipP = pipPB.start();
                pipP.waitFor(2, TimeUnit.MINUTES);

            }
        }
    }

    void startServices() {

        for (var serviceClass : serviceClasses) {
            var service = PlatformTestHelpers.startService(serviceClass, workingDir, platformConfigUrl, secretKey);
            services.add(service);
        }
    }

    void stopServices() {

        for (var i = services.iterator(); i.hasNext(); i.remove()) {
            try {
                var service = i.next();
                service.stop();
            }
            catch (Exception e) {
                log.error("Error stopping service: {}", e.getMessage(), e);
            }
        }
    }

    void startClients() {

        for (var serviceClass : serviceClasses) {
            var serviceKey = serviceKeys.get(serviceClass.getSimpleName());
            var channel = channelForService(platformConfig, serviceKey);
            serviceChannels.put(serviceKey, channel);
        }
    }

    void stopClients() {

        for (var i = serviceChannels.entrySet().iterator(); i.hasNext(); i.remove()) {
            try {
                var channel = i.next().getValue();
                channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                log.error("Error stopping client: {}", e.getMessage(), e);
            }
        }
    }

    ManagedChannel channelForService(PlatformConfig platformConfig, String serviceKey) {

        var serviceTarget = RoutingUtils.serviceTarget(platformConfig, serviceKey);

        var builder = NettyChannelBuilder.forAddress(serviceTarget.getHost(), serviceTarget.getPort());
        // Tests run on basic HTTP for now
        builder.usePlaintext();
        builder.directExecutor();

        return builder.build();
    }

    private void loadDynamicConfig() throws Exception {

        if (tenants.values().stream().allMatch(Objects::isNull))
            return;

        if (!hasService(ConfigKeys.ADMIN_SERVICE_KEY))
            throw new ETracInternal("Admin service is required to bootstrap tenant config");

        var adminClient = createClient(ConfigKeys.ADMIN_SERVICE_KEY, TracAdminApiGrpc::newBlockingStub);

        for (var bootstrapEntry : tenants.entrySet()) {
            if (bootstrapEntry.getValue() != null)
                loadDynamicConfig(bootstrapEntry.getKey(), bootstrapEntry.getValue(), adminClient);
        }
    }

    private void loadDynamicConfig(String tenant, String configPath, TracAdminApiGrpc.TracAdminApiBlockingStub adminClient) throws Exception {

        // Assuming config path is in the same dir as the root config file
        var configSep = configPath.lastIndexOf("/");
        var configFile = configPath.substring(configSep + 1);

        var bootstrap = configManager.loadConfigObject(configFile, DynamicConfig.class);

        for (var config : bootstrap.getConfigMap().entrySet()) {

            var configObject = ObjectDefinition.newBuilder()
                    .setObjectType(ObjectType.CONFIG)
                    .setConfig(config.getValue())
                    .build();

            var writeRequest = ConfigWriteRequest.newBuilder()
                    .setTenant(tenant)
                    .setConfigClass(bootstrap.getConfigClass())
                    .setConfigKey(config.getKey())
                    .setDefinition(configObject)
                    .build();

            var writeResponse = adminClient.createConfigObject(writeRequest);

            log.info("Created config entry: config class = {}, config key = {}",
                    writeResponse.getEntry().getConfigClass(),
                    writeResponse.getEntry().getConfigKey());
        }

        for (var resource : bootstrap.getResourcesMap().entrySet()) {

            var resourceObject = ObjectDefinition.newBuilder()
                    .setObjectType(ObjectType.RESOURCE)
                    .setResource(resource.getValue())
                    .build();

            var writeRequest = ConfigWriteRequest.newBuilder()
                    .setTenant(tenant)
                    .setConfigClass(bootstrap.getResourceClass())
                    .setConfigKey(resource.getKey())
                    .setDefinition(resourceObject)
                    .build();

            var writeResponse = adminClient.createConfigObject(writeRequest);

            log.info("Created resource entry: config class = {}, config key = {}",
                    writeResponse.getEntry().getConfigClass(),
                    writeResponse.getEntry().getConfigKey());
        }

        // Allow some time for config changes to propagate
        Thread.sleep(100);
    }
}
