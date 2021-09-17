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
import com.accenture.trac.api.TracDataApiGrpc;

import com.accenture.trac.svc.data.service.DataReadService;
import com.accenture.trac.svc.data.service.DataWriteService;
import com.google.protobuf.ByteString;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;


public class DataReadApiTest {

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private TracDataApiGrpc.TracDataApiBlockingStub readApi;

    @BeforeEach
    public void setup() throws Exception {

        var serverName = InProcessServerBuilder.generateName();

        var readService = new DataReadService();
        var writeService = new DataWriteService();

        var publicApiImpl = new TracDataApi(readService, writeService);

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(publicApiImpl)
                .build()
                .start());

        readApi = TracDataApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    }

    @Test
    public void test1() {

        var request = DataReadRequest.newBuilder().build();
        var responseStream = readApi.readFile(request);

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
}
