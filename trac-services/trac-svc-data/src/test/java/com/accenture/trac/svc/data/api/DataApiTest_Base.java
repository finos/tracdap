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
import com.accenture.trac.api.config.RootConfig;
import com.accenture.trac.common.config.ConfigBootstrap;
import com.accenture.trac.common.config.StandardArgs;
import com.accenture.trac.common.eventloop.ExecutionContext;
import com.accenture.trac.common.eventloop.ExecutionRegister;
import com.accenture.trac.common.eventloop.IExecutionContext;
import com.accenture.trac.common.storage.StorageManager;
import com.accenture.trac.deploy.metadb.DeployMetaDB;
import com.accenture.trac.svc.data.TracDataService;
import com.accenture.trac.svc.data.service.DataReadService;
import com.accenture.trac.svc.data.service.DataWriteService;
import com.accenture.trac.svc.meta.TracMetadataService;
import com.accenture.trac.test.config.ConfigHelpers;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


abstract  class DataApiTest_Base {

    protected static final String TRAC_UNIT_CONFIG = "config/trac-unit.properties";
    protected static final String TRAC_UNIT_CONFIG_YAML = "config/trac-unit.yaml";
    protected static final String TEST_TENANT = "ACME_CORP";
    protected static final short METADATA_SVC_PORT = 8081;
    protected static final String STORAGE_ROOT_DIR = "unit_test_storage";

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

        var testConfig = ConfigBootstrap.useConfigFile(
                TracMetadataService.class, staticTempDir,
                configPath.toString(), keystoreKey);

        var deploy_schema_task = StandardArgs.task(DeployMetaDB.DEPLOY_SCHEMA_TASK_NAME, "", "");
        var add_tenant_task = StandardArgs.task(DeployMetaDB.ADD_TENANT_TASK_NAME, TEST_TENANT, "");
        var deployDb = new DeployMetaDB(testConfig);
        deployDb.runDeployment(List.of(deploy_schema_task, add_tenant_task));

        metaSvc = new TracMetadataService(testConfig);
        metaSvc.start();

        metaClientChannel = NettyChannelBuilder.forAddress("localhost", METADATA_SVC_PORT)
            .directExecutor()
            .usePlaintext()
            .build();

        metaClient = TrustedMetadataApiGrpc.newFutureStub(metaClientChannel);
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

        var configPath = ConfigHelpers.prepareConfig(
                TRAC_UNIT_CONFIG_YAML, tempDir,
                substitutions);

        var keystoreKey = "";  // not yet used

        var configManager = ConfigBootstrap.useConfigFile(
                TracDataService.class, tempDir,
                configPath.toString(), keystoreKey);

        var rootConfig = configManager.loadRootConfig(RootConfig.class);
        var dataSvcConfig = rootConfig.getTrac().getServices().getData();

        storage = new StorageManager();
        storage.initStoragePlugins();
        storage.initStorage(dataSvcConfig.getStorage());

        execContext = new ExecutionContext(new DefaultEventExecutor());

        var dataSvcName = InProcessServerBuilder.generateName();

        workerGroup = new NioEventLoopGroup(6, new DefaultThreadFactory("data-svc"));
        var execRegister = new ExecutionRegister(workerGroup);

        dataSvcClientChannel = NettyChannelBuilder.forAddress("localhost", METADATA_SVC_PORT)
                .directExecutor()
                .usePlaintext()
                .build();

        var metaApi = TrustedMetadataApiGrpc.newFutureStub(dataSvcClientChannel);

        var readService = new DataReadService(storage, metaApi);
        var writeService = new DataWriteService(storage, metaApi);
        var publicApiImpl =  new TracDataApi(readService, writeService);

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
