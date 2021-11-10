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
import com.accenture.trac.common.validation.Validator;
import com.accenture.trac.metadata.FileDefinition;
import com.accenture.trac.metadata.SchemaDefinition;
import com.accenture.trac.metadata.TagHeader;
import com.accenture.trac.svc.data.service.DataRwService;
import com.accenture.trac.svc.data.service.FileRwService;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;


public class TracDataApi extends TracDataApiGrpc.TracDataApiImplBase {

    private static final MethodDescriptor<DataWriteRequest, TagHeader> CREATE_DATASET_METHOD = TracDataApiGrpc.getCreateDatasetMethod();
    private static final MethodDescriptor<DataWriteRequest, TagHeader> UPDATE_DATASET_METHOD = TracDataApiGrpc.getUpdateDatasetMethod();
    private static final MethodDescriptor<DataReadRequest, DataReadResponse> READ_DATASET_METHOD = TracDataApiGrpc.getReadDatasetMethod();

    private static final MethodDescriptor<DataWriteRequest, TagHeader> CREATE_DATASET_UNARY_METHOD = TracDataApiGrpc.getCreateDatasetUnaryMethod();
    private static final MethodDescriptor<DataWriteRequest, TagHeader> UPDATE_DATASET_UNARY_METHOD = TracDataApiGrpc.getUpdateDatasetUnaryMethod();

    private static final MethodDescriptor<FileWriteRequest, TagHeader> CREATE_FILE_METHOD = TracDataApiGrpc.getCreateFileMethod();
    private static final MethodDescriptor<FileWriteRequest, TagHeader> UPDATE_FILE_METHOD = TracDataApiGrpc.getUpdateFileMethod();
    private static final MethodDescriptor<FileReadRequest, FileReadResponse> READ_FILE_METHOD = TracDataApiGrpc.getReadFileMethod();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataRwService dataRwService;
    private final FileRwService fileService;

    private final Validator validator = new Validator();


    public TracDataApi(DataRwService dataRwService, FileRwService fileService) {
        this.dataRwService = dataRwService;
        this.fileService = fileService;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // API calls
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public StreamObserver<DataWriteRequest> createDataset(StreamObserver<TagHeader> responseObserver) {

        return clientStreaming(CREATE_DATASET_METHOD, responseObserver,
                DataWriteRequest::getContent,
                this::doCreateDataset);
    }

    @Override
    public void createDatasetUnary(DataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        var inputStream = clientStreaming(CREATE_DATASET_UNARY_METHOD, responseObserver,
                DataWriteRequest::getContent,
                this::doCreateDataset);

        inputStream.onNext(request);
        inputStream.onCompleted();
    }

    @Override
    public StreamObserver<DataWriteRequest> updateDataset(StreamObserver<TagHeader> responseObserver) {

        return clientStreaming(UPDATE_DATASET_METHOD, responseObserver,
                DataWriteRequest::getContent,
                this::doUpdateDataset);
    }

    @Override
    public void updateDatasetUnary(DataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        var inputStream = clientStreaming(UPDATE_DATASET_UNARY_METHOD, responseObserver,
                DataWriteRequest::getContent,
                this::doUpdateDataset);

        inputStream.onNext(request);
        inputStream.onCompleted();
    }

    @Override
    public void readDataset(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {

        serverStreaming(READ_DATASET_METHOD, request, responseObserver,
                DataReadResponse::newBuilder,
                DataReadResponse.Builder::setSchema,
                DataReadResponse.Builder::setContent,
                this::doReadDataset);
    }

    @Override
    public StreamObserver<FileWriteRequest> createFile(StreamObserver<TagHeader> responseObserver) {

        return clientStreaming(CREATE_FILE_METHOD, responseObserver,
                FileWriteRequest::getContent,
                this::doCreateFile);
    }

    @Override
    public StreamObserver<FileWriteRequest> updateFile(StreamObserver<TagHeader> responseObserver) {

        return clientStreaming(UPDATE_FILE_METHOD, responseObserver,
                FileWriteRequest::getContent,
                this::doUpdateFile);
    }

    @Override
    public void readFile(FileReadRequest request, StreamObserver<FileReadResponse> responseObserver) {

        serverStreaming(READ_FILE_METHOD, request, responseObserver,
                FileReadResponse::newBuilder,
                FileReadResponse.Builder::setFileDefinition,
                FileReadResponse.Builder::setContent,
                this::doReadFile);
    }

    @Override
    public void getData(DataGetRequest request, StreamObserver<DataGetResponse> responseObserver) {
        super.getData(request, responseObserver);
    }

    @Override
    public void getFile(DataGetRequest request, StreamObserver<DataGetResponse> responseObserver) {
        super.getFile(request, responseObserver);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // API implementation
    // -----------------------------------------------------------------------------------------------------------------

    private CompletionStage<TagHeader> doCreateDataset(
            String methodName,
            DataWriteRequest request,
            Flow.Publisher<ByteBuf> byteStream,
            IExecutionContext execCtx) {

        validateRequest(methodName, request);

        return dataRwService.createDataset(request, byteStream, execCtx);
    }

    private CompletionStage<TagHeader> doUpdateDataset(
            String methodName,
            DataWriteRequest request,
            Flow.Publisher<ByteBuf> byteStream,
            IExecutionContext execCtx) {

        validateRequest(methodName, request);

        return dataRwService.updateDataset(request, byteStream, execCtx);
    }

    private void doReadDataset(
            String methodName,
            DataReadRequest request,
            CompletableFuture<SchemaDefinition> schema,
            Flow.Subscriber<ByteBuf> byteStream,
            IExecutionContext execCtx) {

        validateRequest(methodName, request);

        dataRwService.readDataset(request, schema, byteStream, execCtx);
    }

    private CompletionStage<TagHeader> doCreateFile(
            String methodName,
            FileWriteRequest request,
            Flow.Publisher<ByteBuf> byteStream,
            IExecutionContext execCtx) {

        validateRequest(methodName, request);

        var tenant = request.getTenant();
        var tagUpdates = request.getTagUpdatesList();
        var fileName = request.getName();
        var mimeType = request.getMimeType();

        var expectedSize = request.hasSize()
                ? request.getSize()
                : null;

        return fileService.createFile(
                tenant, tagUpdates,
                fileName, mimeType, expectedSize,
                byteStream, execCtx);
    }

    private CompletionStage<TagHeader> doUpdateFile(
            String methodName,
            FileWriteRequest request,
            Flow.Publisher<ByteBuf> byteStream,
            IExecutionContext execCtx) {

        validateRequest(methodName, request);

        var tenant = request.getTenant();
        var tagUpdates = request.getTagUpdatesList();
        var priorVersion = request.getPriorVersion();
        var fileName = request.getName();
        var mimeType = request.getMimeType();

        var expectedSize = request.hasSize()
                ? request.getSize()
                : null;

        return fileService.updateFile(
                tenant, tagUpdates,
                priorVersion, fileName, mimeType, expectedSize,
                byteStream, execCtx);
    }

    private void doReadFile(
            String methodName,
            FileReadRequest request,
            CompletableFuture<FileDefinition> fileDef,
            Flow.Subscriber<ByteBuf> byteStream,
            IExecutionContext execCtx) {

        validateRequest(methodName, request);

        var tenant = request.getTenant();
        var selector = request.getSelector();

        fileService.readFile(tenant, selector, fileDef, byteStream, execCtx);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Common scaffolding for client and server streaming
    // -----------------------------------------------------------------------------------------------------------------

    private <TReq extends Message, TResp extends Message>
    StreamObserver<TReq> clientStreaming(
            MethodDescriptor<TReq, TResp> method,
            StreamObserver<TResp> responseObserver,
            Function<TReq, ByteString> getContent,
            ClientStreamingMethod<TReq, TResp> apiMethod) {

        log_start(method);

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var requestHub = Flows.<TReq>hub(execCtx);
        var message0 = Flows.first(requestHub);
        var protoContent = Flows.map(requestHub, getContent);
        var content = Flows.map(protoContent, Bytes::fromProtoBytes);

        message0.thenCompose(msg0 -> apiMethod.execute(
                    method.getBareMethodName(), msg0, content, execCtx))

                .whenComplete(GrpcStreams.serverResponseHandler(method, responseObserver))
                .whenComplete((result, error) -> log_finish(method, error));

        return GrpcStreams.serverRequestStream(requestHub);
    }

    @SuppressWarnings("unchecked")
    private <
            TReq extends Message, TResp extends Message,
            TDef extends Message, TBuilder extends Message.Builder>
    void serverStreaming(
            MethodDescriptor<TReq, TResp> method, TReq request,
            StreamObserver<TResp> responseObserver,
            Supplier<TBuilder> responseSupplier,
            BiFunction<TBuilder, TDef, TBuilder> putDefinition,
            BiFunction<TBuilder, ByteString, TBuilder> putContent,
            ServerStreamingMethod<TReq, TDef> apiMethod) {

        log_start(method);

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var definition = new CompletableFuture<TDef>();
        var message0 = definition.thenApply(def_ ->
                (TResp) putDefinition
                .apply(responseSupplier.get(), def_)
                .build());

        var byteStream = Flows.<ByteBuf>hub(execCtx);
        var protoByteStream = Flows.map(byteStream, Bytes::toProtoBytes);
        var content = Flows.map(protoByteStream, bytes ->
                (TResp) putContent
                .apply(responseSupplier.get(), bytes)
                .build());

        var response = Flows.concat(message0, content);

        response.subscribe(GrpcStreams.serverResponseStream(method, responseObserver));

        try {
            apiMethod.execute(
                    method.getBareMethodName(), request,
                    definition, byteStream, execCtx);
        }
        catch (Exception e) {

            // Handle synchronous exceptions during validation

            if (!definition.isDone())
                definition.completeExceptionally(e);
            else
                byteStream.onError(e);
        }
    }

    private <TReq extends Message>
    void validateRequest(String methodName, TReq msg) {

        var protoMethod = Data.getDescriptor()
                .getFile()
                .findServiceByName("TracDataApi")
                .findMethodByName(methodName);

        validator.validateFixedMethod(msg, protoMethod);
    }

    @FunctionalInterface
    private interface ClientStreamingMethod<
            TReq extends Message,
            TResp extends Message> {

        CompletionStage<TResp> execute(
                String methodName, TReq request,
                Flow.Publisher<ByteBuf> byteStream,
                IExecutionContext execCtx);
    }

    @FunctionalInterface
    private interface ServerStreamingMethod<
            TReq extends Message, TDef extends Message> {

        void execute(
                String methodName, TReq request,
                CompletableFuture<TDef> def,
                Flow.Subscriber<ByteBuf> byteStream,
                IExecutionContext execCtx);
    }

    private void log_start(MethodDescriptor<?, ?> method) {

        log.info("API CALL START [{}] ({})", method.getBareMethodName(), prettyMethodType(method.getType()));
    }

    private void log_finish(MethodDescriptor<?, ?> method, Throwable error) {

        if (error == null)
            log.info("API CALL SUCCEEDED [{}]", method.getBareMethodName());

        else if (error instanceof CompletionException) {
            var innerError = error.getCause();
            log.error("API CALL FAILED [{}]: {}", method.getBareMethodName(), innerError.getMessage(), innerError);
        }

        else
            log.error("API CALL FAILED [{}]: {}", method.getBareMethodName(), error.getMessage(), error);
    }

    private String prettyMethodType(MethodDescriptor.MethodType methodType) {

        return methodType.name().toLowerCase().replace("_", " ");
    }
}
