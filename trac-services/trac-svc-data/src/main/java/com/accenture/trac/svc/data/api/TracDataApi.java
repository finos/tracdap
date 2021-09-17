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
import com.accenture.trac.common.util.Bytes;
import com.accenture.trac.common.util.Concurrent;
import com.accenture.trac.svc.data.service.DataReadService;
import com.accenture.trac.svc.data.service.DataWriteService;
import io.grpc.stub.StreamObserver;


public class TracDataApi extends TracDataApiGrpc.TracDataApiImplBase {

    private final DataReadService readService;
    private final DataWriteService writeService;

    public TracDataApi(DataReadService readService, DataWriteService writeService) {
        this.readService = readService;
        this.writeService = writeService;
    }

    @Override
    public StreamObserver<DataWriteRequest> createData(StreamObserver<DataWriteResponse> responseObserver) {
        return super.createData(responseObserver);
    }

    @Override
    public void createDataUnary(DataWriteRequest request, StreamObserver<DataWriteResponse> responseObserver) {
        super.createDataUnary(request, responseObserver);
    }

    @Override
    public StreamObserver<DataWriteRequest> updateData(StreamObserver<DataWriteResponse> responseObserver) {
        return super.updateData(responseObserver);
    }

    @Override
    public void updateDataUnary(DataWriteRequest request, StreamObserver<DataWriteResponse> responseObserver) {
        super.updateDataUnary(request, responseObserver);
    }

    @Override
    public void readData(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {
        super.readData(request, responseObserver);
    }

    @Override
    public StreamObserver<DataWriteRequest> createFile(StreamObserver<DataWriteResponse> responseObserver) {
        return super.createFile(responseObserver);
    }

    @Override
    public StreamObserver<DataWriteRequest> updateFile(StreamObserver<DataWriteResponse> responseObserver) {
        return super.updateFile(responseObserver);
    }

    @Override
    public void readFile(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {

        var dataStream = readService.readFile();

        var responseStream = Concurrent.map(dataStream, chunk ->
                DataReadResponse.newBuilder()
                .setSize(chunk.readableBytes())
                .setContent(Bytes.toProtoBytes(chunk))
                .build());

        var responsePublisher = Concurrent.grpcStreamSubscriber(responseObserver);

        responseStream.subscribe(responsePublisher);
    }

    @Override
    public void getData(DataGetRequest request, StreamObserver<DataGetResponse> responseObserver) {
        super.getData(request, responseObserver);
    }

    @Override
    public void getFile(DataGetRequest request, StreamObserver<DataGetResponse> responseObserver) {
        super.getFile(request, responseObserver);
    }


}
