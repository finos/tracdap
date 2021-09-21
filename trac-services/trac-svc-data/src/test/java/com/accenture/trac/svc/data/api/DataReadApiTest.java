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
import com.accenture.trac.common.eventloop.ExecutionRegister;
import com.accenture.trac.common.storage.StorageManager;
import com.accenture.trac.common.util.Concurrent;
import com.accenture.trac.common.util.GrpcStreams;
import com.accenture.trac.svc.data.service.DataReadService;
import com.accenture.trac.svc.data.service.DataWriteService;

import com.google.protobuf.ByteString;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;


public class DataReadApiTest {

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private static StorageManager storage;

    private TracDataApiGrpc.TracDataApiBlockingStub dataApi;
    private TracDataApiGrpc.TracDataApiStub dataApiStreams;

    @BeforeAll
    public static void setupClass() throws Exception {
        storage = new StorageManager();
        storage.initStoragePlugins();
    }

    @BeforeEach
    public void setup() throws Exception {

        var serverName = InProcessServerBuilder.generateName();

        var workerGroup = new NioEventLoopGroup(6, new DefaultThreadFactory("worker"));
        var execRegister = new ExecutionRegister(workerGroup);

        var metaApi = (TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub) null;

        var readService = new DataReadService(storage, metaApi);
        var writeService = new DataWriteService(storage, metaApi);
        var publicApiImpl =  new TracDataApi(readService, writeService);

        grpcCleanup.register(InProcessServerBuilder.forName(serverName)
                .addService(publicApiImpl)
                .executor(workerGroup)
                .intercept(execRegister.registerExecContext())
                .build()
                .start());

        // Create a client channel and register for automatic graceful shutdown.

        dataApi = TracDataApiGrpc.newBlockingStub(
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));

        dataApiStreams = TracDataApiGrpc.newStub(
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    }

    @Test
    public void test1() {

        var request = DataReadRequest.newBuilder().build();
        var responseStream = dataApi.readFile(request);

        var fileContent = ByteString.EMPTY;
        var fileSize = 0;

        while (responseStream.hasNext()) {

            var responseMsg = responseStream.next();

            fileContent = fileContent.concat(responseMsg.getContent());
            fileSize += responseMsg.getSize();
        }

        Assertions.assertTrue(fileSize > 0);
        Assertions.assertEquals("Hello World!", fileContent.toStringUtf8());
    }

    @Test
    public void test2() throws Exception {

        var content = ByteString.copyFromUtf8("Hello world!\n");
        var message = DataWriteRequest.newBuilder().setContent(content).build();

        var messages = Stream.of(message);
        var requestStream = Concurrent.javaStreamPublisher(messages);
        var response = new CompletableFuture<DataWriteResponse>();

        var responseGrpc = GrpcStreams.resultObserver(response);
        var requestGrpc = dataApiStreams.createFile(responseGrpc);
        requestStream.subscribe(GrpcStreams.relay(requestGrpc));

        var result = response.get();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getSize() > 0);
    }
}
