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
import org.finos.tracdap.common.auth.internal.AuthHelpers;
import org.finos.tracdap.common.concurrent.ExecutionContext;
import org.finos.tracdap.common.data.pipeline.GrpcDownloadSink;
import org.finos.tracdap.common.data.pipeline.GrpcUploadSource;
import org.finos.tracdap.common.data.util.Bytes;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.FileDefinition;
import org.finos.tracdap.metadata.SchemaDefinition;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.svc.data.service.DataService;
import org.finos.tracdap.svc.data.service.FileService;

import com.google.protobuf.Message;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;


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

    private final DataService dataRwService;
    private final FileService fileService;
    private final Validator validator;


    public TracDataApi(DataService dataRwService, FileService fileService) {

        this.dataRwService = dataRwService;
        this.fileService = fileService;
        this.validator = new Validator();
    }


    // -----------------------------------------------------------------------------------------------------------------
    // API calls
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public StreamObserver<DataWriteRequest> createDataset(StreamObserver<TagHeader> responseObserver) {

        var upload = new GrpcUploadSource<>(DataWriteRequest.class, responseObserver);

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();
        var userInfo = AuthHelpers.currentUser();

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(msg -> Bytes.fromProtoBytes(msg.getContent()));

        firstMessage
                .thenApply(req -> validateRequest(CREATE_DATASET_METHOD, req))
                .thenCompose(req -> dataRwService.createDataset(req, dataStream, execCtx, userInfo))
                .thenAccept(upload::succeeded)
                .exceptionally(upload::failed);

        return upload.start();
    }

    @Override
    public void createSmallDataset(DataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        validateRequest(CREATE_SMALL_DATASET_METHOD, request);

        var upload = createDataset(responseObserver);

        upload.onNext(request);
        upload.onCompleted();
    }

    @Override
    public StreamObserver<DataWriteRequest> updateDataset(StreamObserver<TagHeader> responseObserver) {

        var upload = new GrpcUploadSource<>(DataWriteRequest.class, responseObserver);

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();
        var userInfo = AuthHelpers.currentUser();

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(msg -> Bytes.fromProtoBytes(msg.getContent()));

        firstMessage
                .thenApply(req -> validateRequest(UPDATE_DATASET_METHOD, req))
                .thenCompose(req -> dataRwService.updateDataset(req, dataStream, execCtx, userInfo))
                .thenAccept(upload::succeeded)
                .exceptionally(upload::failed);

        return upload.start();
    }

    @Override
    public void updateSmallDataset(DataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        validateRequest(UPDATE_SMALL_DATASET_METHOD, request);

        var upload = updateDataset(responseObserver);

        upload.onNext(request);
        upload.onCompleted();
    }

    @Override
    public void readDataset(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {

        var download = new GrpcDownloadSink<>(responseObserver, DataReadResponse::newBuilder, GrpcDownloadSink.STREAMING);
        readDataset(READ_DATASET_METHOD, request, download);
    }

    @Override
    public void readSmallDataset(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {

        var download = new GrpcDownloadSink<>(responseObserver, DataReadResponse::newBuilder, GrpcDownloadSink.AGGREGATED);
        readDataset(READ_SMALL_DATASET_METHOD, request, download);
    }

    private void readDataset(
            MethodDescriptor<DataReadRequest, DataReadResponse> method, DataReadRequest request,
            GrpcDownloadSink<DataReadResponse, DataReadResponse.Builder> download) {

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();
        var userInfo = AuthHelpers.currentUser();

        var firstMessage = download.<SchemaDefinition>firstMessage(DataReadResponse.Builder::setSchema);
        var dataStream = download.dataStream((msg, chunk) -> msg.setContent(Bytes.toProtoBytes(chunk)));

        download.start(request)
                .thenApply(req -> validateRequest(method, request))
                .thenAccept(req -> dataRwService.readDataset(request, firstMessage, dataStream, execCtx, userInfo))
                .exceptionally(download::failed);
    }

    @Override
    public StreamObserver<FileWriteRequest> createFile(StreamObserver<TagHeader> responseObserver) {

        var upload = new GrpcUploadSource<>(FileWriteRequest.class, responseObserver);

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();
        var userInfo = AuthHelpers.currentUser();

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(msg -> Bytes.fromProtoBytes(msg.getContent()));

        firstMessage
                .thenApply(req -> validateRequest(CREATE_FILE_METHOD, req))
                .thenCompose(req -> fileService.createFile(
                        req.getTenant(),
                        req.getTagUpdatesList(),
                        req.getName(),
                        req.getMimeType(),
                        req.hasSize() ? req.getSize() : null,
                        dataStream, execCtx, userInfo))
                .thenAccept(upload::succeeded)
                .exceptionally(upload::failed);

        return upload.start();
    }

    @Override
    public void createSmallFile(FileWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        validateRequest(CREATE_SMALL_FILE_METHOD, request);

        var upload = createFile(responseObserver);

        upload.onNext(request);
        upload.onCompleted();
    }

    @Override
    public StreamObserver<FileWriteRequest> updateFile(StreamObserver<TagHeader> responseObserver) {

        var upload = new GrpcUploadSource<>(FileWriteRequest.class, responseObserver);

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();
        var userInfo = AuthHelpers.currentUser();

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(msg -> Bytes.fromProtoBytes(msg.getContent()));

        firstMessage
                .thenApply(req -> validateRequest(UPDATE_FILE_METHOD, req))
                .thenCompose(req -> fileService.updateFile(
                        req.getTenant(),
                        req.getTagUpdatesList(),
                        req.getPriorVersion(),
                        req.getName(),
                        req.getMimeType(),
                        req.hasSize() ? req.getSize() : null,
                        dataStream, execCtx, userInfo))
                .thenAccept(upload::succeeded)
                .exceptionally(upload::failed);

        return upload.start();
    }

    @Override
    public void updateSmallFile(FileWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        validateRequest(UPDATE_SMALL_FILE_METHOD, request);

        var upload = updateFile(responseObserver);

        upload.onNext(request);
        upload.onCompleted();
    }

    @Override
    public void readFile(FileReadRequest request, StreamObserver<FileReadResponse> responseObserver) {

        var download = new GrpcDownloadSink<>(responseObserver, FileReadResponse::newBuilder, GrpcDownloadSink.STREAMING);
        readFile(READ_FILE_METHOD, request, download);
    }

    @Override
    public void readSmallFile(FileReadRequest request, StreamObserver<FileReadResponse> responseObserver) {

        var download = new GrpcDownloadSink<>(responseObserver, FileReadResponse::newBuilder, GrpcDownloadSink.AGGREGATED);
        readFile(READ_SMALL_FILE_METHOD, request, download);
    }

    private void readFile(
            MethodDescriptor<FileReadRequest, FileReadResponse> method, FileReadRequest request,
            GrpcDownloadSink<FileReadResponse, FileReadResponse.Builder> download) {

        var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();
        var userInfo = AuthHelpers.currentUser();

        var firstMessage = download.<FileDefinition>firstMessage(FileReadResponse.Builder::setFileDefinition);
        var dataStream = download.dataStream((msg, chunk) -> msg.setContent(Bytes.toProtoBytes(chunk)));

        download.start(request)
                .thenApply(req -> validateRequest(method, req))
                .thenAccept(req -> fileService.readFile(
                        request.getTenant(),
                        request.getSelector(),
                        firstMessage, dataStream,
                        execCtx, userInfo))
                .exceptionally(download::failed);
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
