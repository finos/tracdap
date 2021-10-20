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
import com.accenture.trac.common.concurrent.ExecutionContext;
import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.util.Bytes;
import com.accenture.trac.common.concurrent.Flows;
import com.accenture.trac.common.util.GrpcStreams;
import com.accenture.trac.metadata.FileDefinition;
import com.accenture.trac.metadata.TagHeader;
import com.accenture.trac.svc.data.service.DataReadService;
import com.accenture.trac.svc.data.service.DataWriteService;

import com.accenture.trac.svc.data.validation.Validator;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;


public class TracDataApi extends TracDataApiGrpc.TracDataApiImplBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataReadService readService;
    private final DataWriteService writeService;

    private final Validator validator = new Validator();


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

        var method = TracDataApiGrpc.getCreateFileMethod();

        return clientStreaming(method, responseObserver,
                FileWriteRequest::getContent,
                this::doCreateFile);
    }

    private CompletionStage<TagHeader> doCreateFile(
            FileWriteRequest request,
            Flow.Publisher<ByteBuf> byteStream,
            IExecutionContext execCtx) {

        var tenant = request.getTenant();
        var tagUpdates = request.getTagUpdatesList();
        var fileName = request.getName();
        var mimeType = request.getMimeType();

        var expectedSize = request.hasSize()
                ? request.getSize()
                : null;

        return writeService.createFile(
                tenant, tagUpdates,
                fileName, mimeType, expectedSize,
                byteStream, execCtx);
    }

    @Override
    public StreamObserver<FileWriteRequest> updateFile(StreamObserver<TagHeader> responseObserver) {

        var method = TracDataApiGrpc.getUpdateFileMethod();

        return clientStreaming(method, responseObserver,
                FileWriteRequest::getContent,
                this::doUpdateFile);
    }

    private CompletionStage<TagHeader> doUpdateFile(
            FileWriteRequest request,
            Flow.Publisher<ByteBuf> byteStream,
            IExecutionContext execCtx) {

        var tenant = request.getTenant();
        var tagUpdates = request.getTagUpdatesList();
        var priorVersion = request.getPriorVersion();
        var fileName = request.getName();
        var mimeType = request.getMimeType();

        var expectedSize = request.hasSize()
                ? request.getSize()
                : null;

        return writeService.updateFile(
                tenant, tagUpdates,
                priorVersion, fileName, mimeType, expectedSize,
                byteStream, execCtx);
    }


    @Override
    public void readFile(FileReadRequest request, StreamObserver<FileReadResponse> responseObserver) {

        var method = TracDataApiGrpc.getReadFileMethod();

        serverStreaming(method, request, responseObserver,
                FileReadResponse::newBuilder,
                FileReadResponse.Builder::setFileDefinition,
                FileReadResponse.Builder::setContent,
                this::doReadFile);
    }

    private void doReadFile(
            FileReadRequest request,
            CompletableFuture<FileDefinition> fileDef,
            Flow.Subscriber<ByteBuf> byteStream,
            IExecutionContext execCtx) {

        var tenant = request.getTenant();
        var selector = request.getSelector();

        readService.readFile(tenant, selector, fileDef, byteStream, execCtx);
    }

    @Override
    public void getData(DataGetRequest request, StreamObserver<DataGetResponse> responseObserver) {
        super.getData(request, responseObserver);
    }

    @Override
    public void getFile(DataGetRequest request, StreamObserver<DataGetResponse> responseObserver) {
        super.getFile(request, responseObserver);
    }


    private <TReq extends Message, TResp extends Message>
    StreamObserver<TReq> clientStreaming(
            MethodDescriptor<TReq, TResp> method,
            StreamObserver<TResp> responseObserver,
            Function<TReq, ByteString> getContent,
            ClientStreamingMethod<TReq, TResp> serviceMethod) {

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var requestHub = Flows.<TReq>hub(execCtx);

        var firstMessage = Flows.first(requestHub);
        var validRequest = firstMessage.thenApply(msg -> validateRequest(method, msg));

        var protoContent = Flows.map(requestHub, getContent);
        var byteBufContent = Flows.map(protoContent, Bytes::fromProtoBytes);

        var response = validRequest.thenCompose(msg -> serviceMethod.execute(msg, byteBufContent, execCtx));
        response.whenComplete(GrpcStreams.serverResponseHandler(method, responseObserver));

        return GrpcStreams.serverRequestStream(requestHub);
    }

    private <TReq extends Message, TResp extends Message, TDef extends Message, TBuilder extends Message.Builder>
    void serverStreaming(
            MethodDescriptor<TReq, TResp> method, TReq request,
            StreamObserver<TResp> responseObserver,
            Supplier<TBuilder> responseSupplier,
            BiFunction<TBuilder, TDef, TBuilder> putDefinition,
            BiFunction<TBuilder, ByteString, TBuilder> putContent,
            ServerStreamingMethod<TReq, TDef> serviceMethod) {

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var definition = new CompletableFuture<TDef>();
        var byteStream = Flows.<ByteBuf>hub(execCtx);
        var protoByteStream = Flows.map(byteStream, Bytes::toProtoBytes);

        @SuppressWarnings("unchecked")
        var message0 = definition.thenApply(def_ ->
                (TResp) putDefinition
                .apply(responseSupplier.get(), def_)
                .build());

        @SuppressWarnings("unchecked")
        var content = Flows.map(protoByteStream, bytes ->
                (TResp) putContent
                .apply(responseSupplier.get(), bytes)
                .build());

        var response = Flows.concat(message0, content);
        response.subscribe(GrpcStreams.serverResponseStream(method, responseObserver));

        // Handle synchronous exceptions during validation

        try {
            validateRequest(method, request);
            serviceMethod.execute(request, definition, byteStream, execCtx);
        }
        catch (Exception e) {
            definition.completeExceptionally(e);
        }
    }


    private <TReq extends Message, TResp extends Message>
    TReq validateRequest(MethodDescriptor<TReq, TResp> method, TReq msg) {

        var protoMethod = Data.getDescriptor()
                .getFile()
                .findServiceByName("TracDataApi")
                .findMethodByName(method.getBareMethodName());

        validator.validateFixedMethod(msg, protoMethod);

        return msg;
    }

    @FunctionalInterface
    private interface ClientStreamingMethod<
            TReq extends Message,
            TResp extends Message> {

        CompletionStage<TResp> execute(
                TReq request,
                Flow.Publisher<ByteBuf> byteStream,
                IExecutionContext execCtx);
    }

    @FunctionalInterface
    private interface ServerStreamingMethod<
            TReq extends Message, TDef extends Message> {

        void execute(
                TReq request,
                CompletableFuture<TDef> def,
                Flow.Subscriber<ByteBuf> byteStream,
                IExecutionContext execCtx);
    }
}
