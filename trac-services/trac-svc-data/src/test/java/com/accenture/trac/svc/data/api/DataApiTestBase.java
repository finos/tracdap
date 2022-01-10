/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.data.api;

import com.accenture.trac.api.TracDataApiGrpc;
import com.accenture.trac.api.TrustedMetadataApiGrpc;

import com.accenture.trac.common.codec.CodecManager;
import com.accenture.trac.common.startup.Startup;
import com.accenture.trac.common.startup.StandardArgs;
import com.accenture.trac.common.concurrent.ExecutionContext;
import com.accenture.trac.common.concurrent.ExecutionRegister;
import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.storage.StorageManager;
import com.accenture.trac.config.PlatformConfig;
import com.accenture.trac.deploy.metadb.DeployMetaDB;
import com.accenture.trac.svc.data.EventLoopChannel;
import com.accenture.trac.svc.data.TracDataService;
import com.accenture.trac.svc.data.service.DataService;
import com.accenture.trac.svc.data.service.FileService;
import com.accenture.trac.svc.meta.TracMetadataService;
import com.accenture.trac.test.config.ConfigHelpers;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;

import org.apache.arrow.memory.NettyAllocationManager;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


abstract  class DataApiTestBase {

    protected static final String TRAC_UNIT_CONFIG = "config/trac-unit.yaml";
    protected static final short METADATA_SVC_PORT = 8081;
    protected static final String STORAGE_ROOT_DIR = "unit_test_storage";

    protected static final String TEST_TENANT = "ACME_CORP";
    protected static final String TEST_TENANT_2 = "SOME_OTHER_CORP";

    protected static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);


    @TempDir
    static Path staticTempDir;

    private static TracMetadataService metaSvc;
    private static ManagedChannel metaClientChannel;
    protected static TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient;

    @BeforeAll
    static void setupClass() throws Exception {

        var substitutions = Map.of("${TRAC_RUN_DIR}", staticTempDir.toString().replace("\\", "\\\\"));

        var configPath = ConfigHelpers.prepareConfig(
                TRAC_UNIT_CONFIG, staticTempDir,
                substitutions);

        var keystoreKey = "";  // not yet used

        runDbDeploy(configPath, keystoreKey);
        startMetadataSvc(configPath, keystoreKey);

        metaClientChannel = NettyChannelBuilder.forAddress("localhost", METADATA_SVC_PORT)
            .directExecutor()
            .usePlaintext()
            .build();

        metaClient = TrustedMetadataApiGrpc.newFutureStub(metaClientChannel);
    }

    private static void runDbDeploy(URL configPath, String keystoreKey) {

        var startup = Startup.useConfigFile(TracMetadataService.class, staticTempDir, configPath.toString(), keystoreKey);
        startup.runStartupSequence();

        var config = startup.getConfig();
        var deployDb = new DeployMetaDB(config);

        var deploy_schema_task = StandardArgs.task(DeployMetaDB.DEPLOY_SCHEMA_TASK_NAME, "", "");
        var add_tenant_task = StandardArgs.task(DeployMetaDB.ADD_TENANT_TASK_NAME, TEST_TENANT, "");
        var add_tenant_2_task = StandardArgs.task(DeployMetaDB.ADD_TENANT_TASK_NAME, TEST_TENANT_2, "");

        deployDb.runDeployment(List.of(deploy_schema_task, add_tenant_task, add_tenant_2_task));
    }

    private static void startMetadataSvc(URL configPath, String keystoreKey) {

        var startup = Startup.useConfigFile(TracMetadataService.class, staticTempDir, configPath.toString(), keystoreKey);
        startup.runStartupSequence();

        var plugins = startup.getPlugins();
        var config = startup.getConfig();

        metaSvc = new TracMetadataService(plugins, config);
        metaSvc.start();
    }

    @AfterAll
    static void teardownClass() throws Exception {

        metaClientChannel.shutdown();
        metaClientChannel.awaitTermination(TEST_TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        metaSvc.stop();
    }


    @TempDir
    Path tempDir;

    protected StorageManager storage;
    protected CodecManager formats;

    private EventLoopGroup workerGroup;
    protected IExecutionContext execContext;

    private ManagedChannel dataSvcClientChannel;
    protected Server dataService;

    private ManagedChannel dataClientChannel;
    protected TracDataApiGrpc.TracDataApiStub dataClient;

    @BeforeEach
    void setup() throws Exception {

        // Create storage root dir referenced in config
        Files.createDirectory(tempDir.resolve(STORAGE_ROOT_DIR));

        var substitutions = Map.of("${TRAC_RUN_DIR}", tempDir.toString().replace("\\", "\\\\"));

        var configPath = ConfigHelpers.prepareConfig(TRAC_UNIT_CONFIG, tempDir, substitutions);

        var keystoreKey = "";  // not yet used

        var startup = Startup.useConfigFile(TracDataService.class, tempDir, configPath.toString(), keystoreKey);
        startup.runStartupSequence();

        var plugins = startup.getPlugins();
        plugins.initRegularPlugins();

        var config = startup.getConfig();
        var platformConfig = config.loadRootConfigObject(PlatformConfig.class);
        var dataSvcConfig = platformConfig.getServices().getData();

        formats = new CodecManager(plugins);
        storage = new StorageManager(plugins);
        storage.initStorage(dataSvcConfig.getStorageMap(), formats);

        execContext = new ExecutionContext(new DefaultEventExecutor());

        var arrowAllocatorConfig = RootAllocator
                .configBuilder()
                .allocationManagerFactory(NettyAllocationManager.FACTORY)
                .build();

        var arrowAllocator = new RootAllocator(arrowAllocatorConfig);

        var dataSvcName = InProcessServerBuilder.generateName();

        workerGroup = new NioEventLoopGroup(6, new DefaultThreadFactory("data-svc"));
        var execRegister = new ExecutionRegister(workerGroup);

        var dataSvcClientChannelBuilder = NettyChannelBuilder.forAddress("localhost", METADATA_SVC_PORT)
                .channelType(NioSocketChannel.class)
                .eventLoopGroup(workerGroup)
                .directExecutor()
                .usePlaintext();

        dataSvcClientChannel = EventLoopChannel.wrapChannel(dataSvcClientChannelBuilder, workerGroup);

        var metaApi = TrustedMetadataApiGrpc.newFutureStub(dataSvcClientChannel);

        var dataRwSvc = new DataService(dataSvcConfig, arrowAllocator, storage, formats, metaApi);
        var fileRwSvc = new FileService(dataSvcConfig, storage, metaApi);
        var publicApiImpl =  new TracDataApi(dataRwSvc, fileRwSvc);

        dataService = InProcessServerBuilder.forName(dataSvcName)
                .addService(publicApiImpl)
                .executor(workerGroup)
                .intercept(execRegister.registerExecContext())
                .build()
                .start();

        // Create a client channel and register for automatic graceful shutdown.

        dataClientChannel = InProcessChannelBuilder.forName(dataSvcName)
                .directExecutor()
                .build();

        dataClient = TracDataApiGrpc.newStub(dataClientChannel);
    }

    @AfterEach
    void teardown() throws Exception {

        dataClientChannel.shutdown();

        dataService.shutdown();
        dataSvcClientChannel.shutdown();
        workerGroup.shutdownGracefully();

        // Clean shutdown of channels and event loops takes about 2 seconds
        // This is fine for the real service, but here it is added to the time of each individual test
        // We want to run lots of test cases!

        // So, just fire and forget the shutdown methods
        // A 10 ms delay helps avoid any lingering file locks (assuming most functional tests use the local file impl)

        Thread.sleep(10);

        // Separate test cases are needed to look at the shutdown sequence explicitly
        // These probably belong in the top level package for the service, i.e. testing the service entry point class
    }
}
