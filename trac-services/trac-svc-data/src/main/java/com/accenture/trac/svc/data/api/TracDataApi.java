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
import com.accenture.trac.common.grpc.GrpcServerWrap;
import com.accenture.trac.common.util.Bytes;
import com.accenture.trac.common.concurrent.Flows;
import com.accenture.trac.common.grpc.GrpcStreams;
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
import java.util.function.Function;


public class TracDataApi extends TracDataApiGrpc.TracDataApiImplBase {

    private static final MethodDescriptor<DataWriteRequest, TagHeader> CREATE_DATASET_METHOD = TracDataApiGrpc.getCreateDatasetMethod();
    private static final MethodDescriptor<DataWriteRequest, TagHeader> UPDATE_DATASET_METHOD = TracDataApiGrpc.getUpdateDatasetMethod();
    private static final MethodDescriptor<DataReadRequest, DataReadResponse> READ_DATASET_METHOD = TracDataApiGrpc.getReadDatasetMethod();

    private static final MethodDescriptor<DataWriteRequest, TagHeader> CREATE_SMALL_DATASET_METHOD = TracDataApiGrpc.getCreateSmallDatasetMethod();
    private static final MethodDescriptor<DataWriteRequest, TagHeader> UPDATE_SMALL_DATASET_METHOD = TracDataApiGrpc.getUpdateSmallDatasetMethod();
    private static final MethodDescriptor<DataReadRequest, DataReadResponse> READ_SMALL_DATASET_METHOD = TracDataApiGrpc.getReadSmallDatasetMethod();

    private static final MethodDescriptor<FileWriteRequest, TagHeader> CREATE_FILE_METHOD = TracDataApiGrpc.getCreateFileMethod();
    private static final MethodDescriptor<FileWriteRequest, TagHeader> UPDATE_FILE_METHOD = TracDataApiGrpc.getUpdateFileMethod();
    private static final MethodDescriptor<FileReadRequest, FileReadResponse> READ_FILE_METHOD = TracDataApiGrpc.getReadFileMethod();

    private static final MethodDescriptor<FileWriteRequest, TagHeader> CREATE_SMALL_FILE_METHOD = TracDataApiGrpc.getCreateSmallFileMethod();
    private static final MethodDescriptor<FileWriteRequest, TagHeader> UPDATE_SMALL_FILE_METHOD = TracDataApiGrpc.getUpdateSmallFileMethod();
    private static final MethodDescriptor<FileReadRequest, FileReadResponse> READ_SMALL_FILE_METHOD = TracDataApiGrpc.getReadSmallFileMethod();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataRwService dataRwService;
    private final FileRwService fileService;

    private final Validator validator;
    private final GrpcServerWrap grpcWrap;


    public TracDataApi(DataRwService dataRwService, FileRwService fileService) {

        this.dataRwService = dataRwService;
        this.fileService = fileService;

        this.validator = new Validator();
        this.grpcWrap = new GrpcServerWrap(getClass());
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
    public void createSmallDataset(DataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        var inputStream = clientStreaming(CREATE_SMALL_DATASET_METHOD, responseObserver,
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
    public void updateSmallDataset(DataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        var inputStream = clientStreaming(UPDATE_SMALL_DATASET_METHOD, responseObserver,
                DataWriteRequest::getContent,
                this::doUpdateDataset);

        inputStream.onNext(request);
        inputStream.onCompleted();
    }

    @Override
    public void readDataset(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {

        grpcWrap.serverStreaming(READ_DATASET_METHOD, request, responseObserver, this::readDataset);
    }

    @Override
    public void readSmallDataset(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {

        grpcWrap.unaryCall(READ_SMALL_DATASET_METHOD, request, responseObserver, this::readSmallDataset);
    }

    @Override
    public StreamObserver<FileWriteRequest> createFile(StreamObserver<TagHeader> responseObserver) {

        return clientStreaming(CREATE_FILE_METHOD, responseObserver,
                FileWriteRequest::getContent,
                this::doCreateFile);
    }

    @Override
    public void createSmallFile(FileWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        var inputStream = clientStreaming(CREATE_SMALL_FILE_METHOD, responseObserver,
                FileWriteRequest::getContent,
                this::doCreateFile);

        inputStream.onNext(request);
        inputStream.onCompleted();
    }

    @Override
    public StreamObserver<FileWriteRequest> updateFile(StreamObserver<TagHeader> responseObserver) {

        return clientStreaming(UPDATE_FILE_METHOD, responseObserver,
                FileWriteRequest::getContent,
                this::doUpdateFile);
    }

    @Override
    public void updateSmallFile(FileWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        var inputStream = clientStreaming(UPDATE_SMALL_FILE_METHOD, responseObserver,
                FileWriteRequest::getContent,
                this::doUpdateFile);

        inputStream.onNext(request);
        inputStream.onCompleted();
    }

    @Override
    public void readFile(FileReadRequest request, StreamObserver<FileReadResponse> responseObserver) {

        grpcWrap.serverStreaming(READ_FILE_METHOD, request, responseObserver, this::readFile);
    }

    @Override
    public void readSmallFile(FileReadRequest request, StreamObserver<FileReadResponse> responseObserver) {

        grpcWrap.unaryCall(READ_SMALL_FILE_METHOD, request, responseObserver, this::readSmallFile);
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

    private Flow.Publisher<DataReadResponse> readDataset(DataReadRequest request) {

        validateRequest(READ_DATASET_METHOD.getBareMethodName(), request);

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var schemaResult = new CompletableFuture<SchemaDefinition>();
        var dataStream = Flows.<ByteBuf>hub(execCtx);

        dataRwService.readDataset(request, schemaResult, dataStream, execCtx);

        var schemaMsg = schemaResult.thenApply(schema ->
                DataReadResponse.newBuilder()
                .setSchema(schema)
                .build());

        var protoDataStream = Flows.map(dataStream, Bytes::toProtoBytes);
        var dataMsg = Flows.map(protoDataStream, chunk ->
                DataReadResponse.newBuilder()
                .setContent(chunk)
                .build());

        // Flows.concat will only request from the data pipeline if schemaMsg completes successfully

        return Flows.concat(schemaMsg, dataMsg);
    }

    private CompletionStage<DataReadResponse> readSmallDataset(DataReadRequest request) {

        validateRequest(READ_SMALL_DATASET_METHOD.getBareMethodName(), request);

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var schemaResult = new CompletableFuture<SchemaDefinition>();
        var dataStream = Flows.<ByteBuf>hub(execCtx);

        dataRwService.readDataset(request, schemaResult, dataStream, execCtx);

        return schemaResult.thenCompose(schema -> {

            // Flows.fold triggers a request from the data pipeline
            // So, only do this if schemaResult completes successfully

            var protoDataStream = Flows.map(dataStream, Bytes::toProtoBytes);
            var protoAggregate = Flows.fold(protoDataStream, ByteString::concat, ByteString.EMPTY);

            return protoAggregate.thenApply(data -> DataReadResponse.newBuilder()
                    .setSchema(schema)
                    .setContent(data)
                    .build());
        });
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

    private Flow.Publisher<FileReadResponse> readFile(FileReadRequest request) {

        validateRequest(READ_FILE_METHOD.getBareMethodName(), request);

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var fileResult = new CompletableFuture<FileDefinition>();
        var dataStream = Flows.<ByteBuf>hub(execCtx);

        fileService.readFile(
                request.getTenant(), request.getSelector(),
                fileResult, dataStream, execCtx);

        var msg0 = fileResult.thenApply(file ->
                FileReadResponse.newBuilder()
                .setFileDefinition(file)
                .build());

        var protoDataStream = Flows.map(dataStream, Bytes::toProtoBytes);
        var contentMsg = Flows.map(protoDataStream, chunk ->
                FileReadResponse.newBuilder()
                .setContent(chunk)
                .build());

        // Flows.concat will only request from the data pipeline if msg0 completes successfully

        return Flows.concat(msg0, contentMsg);
    }

    private CompletionStage<FileReadResponse> readSmallFile(FileReadRequest request) {

        validateRequest(READ_SMALL_FILE_METHOD.getBareMethodName(), request);

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var fileResult = new CompletableFuture<FileDefinition>();
        var dataStream = Flows.<ByteBuf>hub(execCtx);

        fileService.readFile(
                request.getTenant(), request.getSelector(),
                fileResult, dataStream, execCtx);

        return fileResult.thenCompose(file -> {

            // Flows.fold triggers a request from the data pipeline
            // So, only do this if fileResult completes successfully

            var protoDataStream = Flows.map(dataStream, Bytes::toProtoBytes);
            var protoAggregate = Flows.fold(protoDataStream, ByteString::concat, ByteString.EMPTY);

            return protoAggregate.thenApply(content -> FileReadResponse.newBuilder()
                    .setFileDefinition(file)
                    .setContent(content)
                    .build());
        });
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
