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

import com.accenture.trac.api.*;
import com.accenture.trac.api.config.RootConfig;
import com.accenture.trac.common.config.ConfigBootstrap;
import com.accenture.trac.common.config.StandardArgs;
import com.accenture.trac.common.eventloop.ExecutionRegister;
import com.accenture.trac.common.eventloop.IExecutionContext;
import com.accenture.trac.common.storage.StorageManager;
import com.accenture.trac.common.util.Concurrent;
import com.accenture.trac.common.util.Futures;
import com.accenture.trac.common.util.GrpcStreams;
import com.accenture.trac.deploy.metadb.DeployMetaDB;
import com.accenture.trac.metadata.ObjectDefinition;
import com.accenture.trac.metadata.ObjectType;
import com.accenture.trac.metadata.TagSelector;
import com.accenture.trac.svc.data.TracDataService;
import com.accenture.trac.svc.data.service.DataReadService;
import com.accenture.trac.svc.data.service.DataWriteService;

import com.accenture.trac.svc.meta.TracMetadataService;
import com.accenture.trac.test.config.ConfigHelpers;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.accenture.trac.common.metadata.MetadataUtil.selectorFor;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.*;
import static com.accenture.trac.test.storage.StorageTestHelpers.readFile;


public class DataApiTest_File {

    private static final String TRAC_UNIT_CONFIG = "config/trac-unit.properties";
    private static final String TRAC_UNIT_CONFIG_YAML = "config/trac-unit.yaml";
    private static final String TEST_TENANT = "ACME_CORP";
    private static final short METADATA_SVC_PORT = 8081;
    private static final String STORAGE_ROOT_DIR = "unit_test_storage";

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    @TempDir
    static Path staticTempDir;
    static TracMetadataService metaSvc;

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
    }

    @AfterAll
    static void teardownClass() {

        metaSvc.stop();
    }



    @Rule
    final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @TempDir
    Path tempDir;

    StorageManager storage;
    IExecutionContext execContext;

    TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi;
    TracDataApiGrpc.TracDataApiStub dataApi;

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

        metaApi = TrustedMetadataApiGrpc.newFutureStub(grpcCleanup.register(
                NettyChannelBuilder.forAddress("localhost", METADATA_SVC_PORT)
                .directExecutor()
                .usePlaintext()
                .build()));

        var dataSvcName = InProcessServerBuilder.generateName();

        var workerGroup = new NioEventLoopGroup(6, new DefaultThreadFactory("worker"));
        var execRegister = new ExecutionRegister(workerGroup);

        var readService = new DataReadService(storage, metaApi);
        var writeService = new DataWriteService(storage, metaApi);
        var publicApiImpl =  new TracDataApi(readService, writeService);

        grpcCleanup.register(InProcessServerBuilder.forName(dataSvcName)
                .addService(publicApiImpl)
                .executor(workerGroup)
                .intercept(execRegister.registerExecContext())
                .build()
                .start());

        // Create a client channel and register for automatic graceful shutdown.

        dataApi = TracDataApiGrpc.newStub(grpcCleanup.register(
                InProcessChannelBuilder.forName(dataSvcName)
                .directExecutor()
                .build()));

    }

    @Test
    void testRoundTrip_basic() throws Exception {

        var random = new Random();
        var content = List.of(
                new byte[4096],
                new byte[4096],
                new byte[4096]);

        for (var buf: content)
            random.nextBytes(buf);

        roundTripTest(content, true);
        roundTripTest(content, false);
    }

    @Test
    void testRoundTrip_heterogeneousChunks() throws Exception {

        var random = new Random();
        var content = List.of(
                new byte[3],
                new byte[10000],
                new byte[42],
                new byte[4097],
                new byte[1],
                new byte[2000]);

        for (var buf: content)
            random.nextBytes(buf);

        roundTripTest(content, true);
        roundTripTest(content, false);
    }

    @Test
    void testRoundTrip_megabyteChunk() throws Exception {

        var content = List.of(new byte[1024 * 1024]);

        var random = new Random();
        random.nextBytes(content.get(0));

        roundTripTest(content, true);
        roundTripTest(content, false);
    }

    @Test
    void testRoundTrip_singleByte() throws Exception {

        var content = List.of(new byte[1]);
        content.get(0)[0] = 0;

        roundTripTest(content, true);
        roundTripTest(content, false);
    }

    @Test
    void testRoundTrip_smallTextFile() throws Exception {

        var contentText = "Hello world!\n";
        var contentBytes = contentText.getBytes(StandardCharsets.UTF_8);
        var content = List.of(contentBytes);

        roundTripTest(content, true);
        roundTripTest(content, false);
    }

    private void roundTripTest(List<byte[]> content, boolean dataInChunkZero) throws Exception {

        // Set up a request stream and client streaming call, wait for the call to complete

        var createFileRequest = fileWriteRequest(content, dataInChunkZero);
        var createFile = clientStreaming(dataApi::createFile, createFileRequest);

        waitFor(TEST_TIMEOUT, createFile);
        var objHeader = resultOf(createFile);

        // Fetch metadata for the file and storage objects that should be created

        var fileDef = fetchDefinition(TEST_TENANT, selectorFor(objHeader), ObjectDefinition::getFile);
        var storageDef = fetchDefinition(TEST_TENANT, fileDef.getStorageId(), ObjectDefinition::getStorage);

        var dataItem = fileDef.getDataItem();
        var storageItem = storageDef.getDataItemsOrThrow(dataItem);
        var incarnation = storageItem.getIncarnations(0);
        var copy = incarnation.getCopies(0);

        Assertions.assertEquals(ObjectType.FILE, objHeader.getObjectType());
        Assertions.assertEquals(1, objHeader.getObjectVersion());
        Assertions.assertEquals(1, objHeader.getTagVersion());

        // TODO: More assert checks on stored metadata

        // Use storage impl directly to check file has arrived in the storage back end

        var storageImpl = storage.getFileStorage(copy.getStorageKey());
        var exists = storageImpl.exists(copy.getStoragePath());
        var size = storageImpl.size(copy.getStoragePath());
        var storedContent = readFile(copy.getStoragePath(), storageImpl, execContext);

        waitFor(TEST_TIMEOUT, exists, size, storedContent);

        var originalSize = content.stream()
                .mapToLong(b -> b.length)
                .sum();

        var originalBytes = ByteString.copyFrom(
            content.stream()
            .map(ByteString::copyFrom)
            .collect(Collectors.toList()));

        var storedBytes = ByteString.copyFrom(
            resultOf(storedContent).nioBuffer());

        Assertions.assertTrue(resultOf(exists));
        Assertions.assertEquals(originalSize, resultOf(size));
        Assertions.assertEquals(originalBytes, storedBytes);

        resultOf(storedContent).release();

        // Set up a server-streaming request to read the file back

        var readRequest = FileReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selectorFor(objHeader))
                .build();

        var readResponse = Concurrent.<FileReadResponse>hub();
        var readResponse0 = Concurrent.first(readResponse);
        var readByteStream = Concurrent.map(readResponse, FileReadResponse::getContent);
        var readBytes = Concurrent.fold(readByteStream, ByteString::concat, ByteString.EMPTY);

        serverStreaming(dataApi::readFile, readRequest, readResponse);

        waitFor(TEST_TIMEOUT, readResponse0, readBytes);
        var roundTripTag = resultOf(readResponse0).getFileTag();
        var roundTripBytes = resultOf(readBytes);

        // TODO: Compare tag / definition

        Assertions.assertEquals(originalBytes, roundTripBytes);
    }

    private Flow.Publisher<FileWriteRequest>
    fileWriteRequest(List<byte[]> content, boolean dataInChunkZero) {

        var chunkZeroBytes = dataInChunkZero
                ? ByteString.copyFrom(content.get(0))
                : ByteString.EMPTY;

        var requestZero = FileWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setName("test_file.dat")
                .setExtension("dat")
                .setMimeType("application/octet-stream")
                .setContent(chunkZeroBytes)
                .build();

        var remainingContent = dataInChunkZero
                ? content.subList(1, content.size())
                : content;

        var requestStream = remainingContent.stream().map(bytes ->
                FileWriteRequest.newBuilder()
                .setContent(ByteString.copyFrom(bytes))
                .build());

        return Concurrent.publish(Streams.concat(
                Stream.of(requestZero),
                requestStream));
    }

    private <TReq, TResp>
    Flow.Publisher<TResp> serverStreaming(
            BiConsumer<TReq, StreamObserver<TResp>> grpcMethod,
            TReq request) {

        var response = Concurrent.<TResp>hub();
        var responseGrpc = GrpcStreams.relay(response);

        grpcMethod.accept(request, responseGrpc);

        return response;
    }

    private <TReq, TResp>
    void serverStreaming(
            BiConsumer<TReq, StreamObserver<TResp>> grpcMethod,
            TReq request, Flow.Subscriber<TResp> response) {

        var responseGrpc = GrpcStreams.relay(response);
        grpcMethod.accept(request, responseGrpc);
    }

    private <TReq, TResp>
    CompletableFuture<TResp> clientStreaming(
            Function<StreamObserver<TResp>, StreamObserver<TReq>> grpcMethod,
            Flow.Publisher<TReq> requestPublisher) {

        var response = new CompletableFuture<TResp>();

        var responseGrpc = GrpcStreams.unaryResult(response);
        var requestGrpc = grpcMethod.apply(responseGrpc);
        var requestSubscriber = GrpcStreams.relay(requestGrpc);

        requestPublisher.subscribe(requestSubscriber);

        return response;
    }

    private <TDef>
    TDef fetchDefinition(
            String tenant, TagSelector selector,
            Function<ObjectDefinition, TDef> defTypeFunc)
            throws Exception {

        var tagGrpc = metaApi.readObject(MetadataReadRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(selector)
                .build());

        var tag = Futures.javaFuture(tagGrpc);

        waitFor(TEST_TIMEOUT, tag);

        var objDef = resultOf(tag).getDefinition();

        return defTypeFunc.apply(objDef);
    }
}
