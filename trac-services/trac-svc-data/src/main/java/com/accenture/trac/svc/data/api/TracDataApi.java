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
import com.accenture.trac.common.eventloop.ExecutionContext;
import com.accenture.trac.common.util.Bytes;
import com.accenture.trac.common.util.Concurrent;
import com.accenture.trac.common.util.GrpcStreams;
import com.accenture.trac.metadata.TagHeader;
import com.accenture.trac.svc.data.service.DataReadService;
import com.accenture.trac.svc.data.service.DataWriteService;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TracDataApi extends TracDataApiGrpc.TracDataApiImplBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

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
    public StreamObserver<FileWriteRequest> createFile(StreamObserver<TagHeader> responseObserver) {

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var requestHub = Concurrent.<FileWriteRequest>hub(execCtx);
        var firstMessage = Concurrent.first(requestHub);

        // TODO: Validation - firstMessage.thenApply(validationFunc);

        var protoContent = Concurrent.map(requestHub, FileWriteRequest::getContent);
        var byteBufContent = Concurrent.map(protoContent, Bytes::fromProtoBytes);

        var fileObjectHeader = firstMessage.thenCompose(msg -> writeService.createFile(
                msg.getTenant(),
                byteBufContent, execCtx));

        fileObjectHeader.whenComplete(GrpcStreams.resultHandler(responseObserver));

        return GrpcStreams.relay(requestHub);
    }

    @Override
    public StreamObserver<FileWriteRequest> updateFile(StreamObserver<TagHeader> responseObserver) {
        return super.updateFile(responseObserver);
    }

    @Override
    public void readFile(FileReadRequest request, StreamObserver<FileReadResponse> responseObserver) {

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();
        log.info("Got ctx: {}", execCtx);

        // TODO: Validation

        log.info("In read call");

        var tenant = request.getTenant();
        var selector = request.getSelector();

        var dataStream = Concurrent.<ByteBuf>hub(execCtx);
        var response = Concurrent.map(dataStream, chunk ->
                FileReadResponse.newBuilder()
                .setContent(Bytes.toProtoBytes(chunk))
                .build());

        response.subscribe(GrpcStreams.relay(responseObserver));

        readService.readFile(tenant, selector, dataStream, execCtx);
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
