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

import com.accenture.trac.api.DataReadRequest;
import com.accenture.trac.api.DataWriteRequest;
import com.accenture.trac.api.DataWriteResponse;
import com.accenture.trac.api.TracDataApiGrpc;

import com.accenture.trac.common.util.Concurrent;
import com.accenture.trac.common.util.GrpcStreams;
import com.accenture.trac.svc.data.service.DataReadService;
import com.accenture.trac.svc.data.service.DataWriteService;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;


public class DataReadApiTest {

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private TracDataApiGrpc.TracDataApiBlockingStub dataApi;
    private TracDataApiGrpc.TracDataApiStub dataApiStreams;

    @BeforeEach
    public void setup() throws Exception {

        var serverName = InProcessServerBuilder.generateName();

        var readService = new DataReadService();
        var writeService = new DataWriteService();

        var publicApiImpl =  new TracDataApi(readService, writeService);

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(publicApiImpl)
                .build()
                .start());

        dataApi = TracDataApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
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
    public void test2() {

       var messages = Stream.of(DataWriteRequest.newBuilder().build());
       var request = Concurrent.javaStreamPublisher(messages);

       var response = Concurrent.<DataWriteResponse>hub();
       var responseGrpc = GrpcStreams.relay(response);

       var requestGrpc = dataApiStreams.createFile(responseGrpc);
       request.subscribe(GrpcStreams.relay(requestGrpc));
    }
}
