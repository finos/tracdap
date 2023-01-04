/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.data.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.auth.AuthConstants;
import org.finos.tracdap.common.concurrent.ExecutionContext;
import org.finos.tracdap.common.grpc.GrpcServerWrap;
import org.finos.tracdap.common.util.Bytes;
import org.finos.tracdap.common.concurrent.Flows;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.FileDefinition;
import org.finos.tracdap.metadata.SchemaDefinition;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.svc.data.service.DataService;
import org.finos.tracdap.svc.data.service.FileService;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


public class TracDataApi extends TracDataApiGrpc.TracDataApiImplBase {

    private static final MethodDescriptor<DataWriteRequest, TagHeader> CREATE_DATASET_METHOD = TracDataApiGrpc.getCreateDatasetMethod();
    private static final MethodDescriptor<DataWriteRequest, TagHeader> UPDATE_DATASET_METHOD = TracDataApiGrpc.getUpdateDatasetMethod();
    private static final MethodDescriptor<DataReadRequest, DataReadResponse> READ_DATASET_METHOD = TracDataApiGrpc.getReadDatasetMethod();

    private static final MethodDescriptor<DataWriteRequest, TagHeader> CREATE_SMALL_DATASET_METHOD = TracDataApiGrpc.getCreateSmallDatasetMethod();
    private static final MethodDescriptor<DataReadRequest, DataReadResponse> READ_SMALL_DATASET_METHOD = TracDataApiGrpc.getReadSmallDatasetMethod();

    private static final MethodDescriptor<FileWriteRequest, TagHeader> CREATE_FILE_METHOD = TracDataApiGrpc.getCreateFileMethod();
    private static final MethodDescriptor<FileWriteRequest, TagHeader> UPDATE_FILE_METHOD = TracDataApiGrpc.getUpdateFileMethod();
    private static final MethodDescriptor<FileReadRequest, FileReadResponse> READ_FILE_METHOD = TracDataApiGrpc.getReadFileMethod();

    private static final MethodDescriptor<FileWriteRequest, TagHeader> CREATE_SMALL_FILE_METHOD = TracDataApiGrpc.getCreateSmallFileMethod();
    private static final MethodDescriptor<FileWriteRequest, TagHeader> UPDATE_SMALL_FILE_METHOD = TracDataApiGrpc.getUpdateSmallFileMethod();
    private static final MethodDescriptor<FileReadRequest, FileReadResponse> READ_SMALL_FILE_METHOD = TracDataApiGrpc.getReadSmallFileMethod();

    private final DataService dataRwService;
    private final FileService fileService;

    private final Validator validator;
    private final GrpcServerWrap grpcWrap;


    public TracDataApi(DataService dataRwService, FileService fileService) {

        this.dataRwService = dataRwService;
        this.fileService = fileService;

        this.validator = new Validator();
        this.grpcWrap = new GrpcServerWrap();
    }


    // -----------------------------------------------------------------------------------------------------------------
    // API calls
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public StreamObserver<DataWriteRequest> createDataset(StreamObserver<TagHeader> responseObserver) {

        return grpcWrap.clientStreaming(responseObserver, this::createDataset);
    }

    @Override
    public void createSmallDataset(DataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        grpcWrap.unaryAsync(request, responseObserver, this::createSmallDataset);
    }

    @Override
    public StreamObserver<DataWriteRequest> updateDataset(StreamObserver<TagHeader> responseObserver) {

        return grpcWrap.clientStreaming(responseObserver, this::updateDataset);
    }

    @Override
    public void updateSmallDataset(DataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        grpcWrap.unaryAsync(request, responseObserver, this::updateSmallDataset);
    }

    @Override
    public void readDataset(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {

        grpcWrap.serverStreaming(request, responseObserver, this::readDataset);
    }

    @Override
    public void readSmallDataset(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {

        grpcWrap.unaryAsync(request, responseObserver, this::readSmallDataset);
    }

    @Override
    public StreamObserver<FileWriteRequest> createFile(StreamObserver<TagHeader> responseObserver) {

        return grpcWrap.clientStreaming(responseObserver, this::createFile);
    }

    @Override
    public void createSmallFile(FileWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        grpcWrap.unaryAsync(request, responseObserver, this::createSmallFile);
    }

    @Override
    public StreamObserver<FileWriteRequest> updateFile(StreamObserver<TagHeader> responseObserver) {

        return grpcWrap.clientStreaming(responseObserver, this::updateFile);
    }

    @Override
    public void updateSmallFile(FileWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        grpcWrap.unaryAsync(request, responseObserver, this::updateSSmallFile);
    }

    @Override
    public void readFile(FileReadRequest request, StreamObserver<FileReadResponse> responseObserver) {

        grpcWrap.serverStreaming(request, responseObserver, this::readFile);
    }

    @Override
    public void readSmallFile(FileReadRequest request, StreamObserver<FileReadResponse> responseObserver) {

        grpcWrap.unaryAsync(request, responseObserver, this::readSmallFile);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // API implementation
    // -----------------------------------------------------------------------------------------------------------------

    private CompletionStage<TagHeader> createDataset(Flow.Publisher<DataWriteRequest> requestStream) {

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var requestHub = Flows.<DataWriteRequest>hub(execCtx);
        requestStream.subscribe(requestHub);

        var firstMessage = Flows.first(requestHub);
        var dataStream = Flows.map(requestHub, msg -> Bytes.fromProtoBytes(msg.getContent()));

        return firstMessage
                .thenApply(request -> validateRequest(CREATE_DATASET_METHOD, request))
                .thenCompose(request -> dataRwService.createDataset(request, dataStream, execCtx, authToken));
    }

    private CompletionStage<TagHeader> createSmallDataset(DataWriteRequest request) {

        validateRequest(CREATE_SMALL_DATASET_METHOD, request);

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var dataBytes = Bytes.fromProtoBytes(request.getContent());
        var dataStream = Flows.publish(List.of(dataBytes));

        return dataRwService.createDataset(request, dataStream, execCtx, authToken);
    }

    private CompletionStage<TagHeader> updateDataset(Flow.Publisher<DataWriteRequest> requestStream) {

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var requestHub = Flows.<DataWriteRequest>hub(execCtx);
        requestStream.subscribe(requestHub);

        var firstMessage = Flows.first(requestHub);
        var dataStream = Flows.map(requestHub, msg -> Bytes.fromProtoBytes(msg.getContent()));

        return firstMessage
                .thenApply(request -> validateRequest(UPDATE_DATASET_METHOD, request))
                .thenCompose(request -> dataRwService.updateDataset(request, dataStream, execCtx, authToken));
    }

    private CompletionStage<TagHeader> updateSmallDataset(DataWriteRequest request) {

        validateRequest(UPDATE_DATASET_METHOD, request);

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var dataBytes = Bytes.fromProtoBytes(request.getContent());
        var dataStream = Flows.publish(List.of(dataBytes));

        return dataRwService.updateDataset(request, dataStream, execCtx, authToken);
    }

    private Flow.Publisher<DataReadResponse> readDataset(DataReadRequest request) {

        validateRequest(READ_DATASET_METHOD, request);

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var schemaResult = new CompletableFuture<SchemaDefinition>();
        var dataStream = Flows.<ByteBuf>hub(execCtx);

        dataRwService.readDataset(request, schemaResult, dataStream, execCtx, authToken);

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

        validateRequest(READ_SMALL_DATASET_METHOD, request);

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var schemaResult = new CompletableFuture<SchemaDefinition>();
        var dataStream = Flows.<ByteBuf>hub(execCtx);

        dataRwService.readDataset(request, schemaResult, dataStream, execCtx, authToken);

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

    private CompletionStage<TagHeader> createFile(Flow.Publisher<FileWriteRequest> requestStream) {

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var requestHub = Flows.<FileWriteRequest>hub(execCtx);
        requestStream.subscribe(requestHub);

        var firstMessage = Flows.first(requestHub);
        var dataStream = Flows.map(requestHub, msg -> Bytes.fromProtoBytes(msg.getContent()));

        return firstMessage
                .thenApply(request -> validateRequest(CREATE_FILE_METHOD, request))
                .thenCompose(request -> fileService.createFile(
                        request.getTenant(),
                        request.getTagUpdatesList(),
                        request.getName(),
                        request.getMimeType(),
                        request.hasSize() ? request.getSize() : null,
                        dataStream, execCtx, authToken));
    }

    private CompletionStage<TagHeader> createSmallFile(FileWriteRequest request) {

        validateRequest(CREATE_SMALL_FILE_METHOD, request);

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var dataBytes = Bytes.fromProtoBytes(request.getContent());
        var dataStream = Flows.publish(List.of(dataBytes));

        return fileService.createFile(
                request.getTenant(),
                request.getTagUpdatesList(),
                request.getName(),
                request.getMimeType(),
                request.hasSize() ? request.getSize() : null,
                dataStream, execCtx, authToken);
    }

    private CompletionStage<TagHeader> updateFile(Flow.Publisher<FileWriteRequest> requestStream) {

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var requestHub = Flows.<FileWriteRequest>hub(execCtx);
        requestStream.subscribe(requestHub);

        var firstMessage = Flows.first(requestHub);
        var dataStream = Flows.map(requestHub, msg -> Bytes.fromProtoBytes(msg.getContent()));

        return firstMessage
                .thenApply(request -> validateRequest(UPDATE_FILE_METHOD, request))
                .thenCompose(request -> fileService.updateFile(
                        request.getTenant(),
                        request.getTagUpdatesList(),
                        request.getPriorVersion(),
                        request.getName(),
                        request.getMimeType(),
                        request.hasSize() ? request.getSize() : null,
                        dataStream, execCtx, authToken));
    }

    private CompletionStage<TagHeader> updateSSmallFile(FileWriteRequest request) {

        validateRequest(UPDATE_SMALL_FILE_METHOD, request);

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var dataBytes = Bytes.fromProtoBytes(request.getContent());
        var dataStream = Flows.publish(List.of(dataBytes));

        return fileService.updateFile(
                request.getTenant(),
                request.getTagUpdatesList(),
                request.getPriorVersion(),
                request.getName(),
                request.getMimeType(),
                request.hasSize() ? request.getSize() : null,
                dataStream, execCtx, authToken);
    }

    private Flow.Publisher<FileReadResponse> readFile(FileReadRequest request) {

        validateRequest(READ_FILE_METHOD, request);

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var fileResult = new CompletableFuture<FileDefinition>();
        var dataStream = Flows.<ByteBuf>hub(execCtx);

        fileService.readFile(
                request.getTenant(), request.getSelector(),
                fileResult, dataStream, execCtx, authToken);

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

        validateRequest(READ_SMALL_FILE_METHOD, request);

        var authToken = AuthConstants.AUTH_TOKEN_KEY.get();
        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();

        var fileResult = new CompletableFuture<FileDefinition>();
        var dataStream = Flows.<ByteBuf>hub(execCtx);

        fileService.readFile(
                request.getTenant(), request.getSelector(),
                fileResult, dataStream, execCtx, authToken);

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

    private <TReq extends Message>
    TReq validateRequest(MethodDescriptor<TReq, ?> method, TReq request) {

        var protoMethod = Data.getDescriptor()
                .getFile()
                .findServiceByName("TracDataApi")
                .findMethodByName(method.getBareMethodName());

        validator.validateFixedMethod(request, protoMethod);

        return request;
    }
}
