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
import org.finos.tracdap.api.Data;
import org.finos.tracdap.common.auth.internal.AuthHelpers;
import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.data.pipeline.GrpcDownloadSink;
import org.finos.tracdap.common.data.pipeline.GrpcUploadSource;
import org.finos.tracdap.common.util.LoggingHelpers;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.data.EventLoopRegister;
import org.finos.tracdap.svc.data.service.DataService;
import org.finos.tracdap.svc.data.service.FileService;

import com.google.protobuf.Message;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;


public class TracDataApi extends TracDataApiGrpc.TracDataApiImplBase {

    private static final long DEFAULT_INITIAL_ALLOCATION = 16 * 1024 * 1024;
    private static final long DEFAULT_MAX_ALLOCATION = 128 * 1024 * 1024;

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

    private static final MethodDescriptor<DownloadRequest, DownloadResponse> DOWNLOAD_FILE_METHOD = TracDataApiGrpc.getDownloadFileMethod();
    private static final MethodDescriptor<DownloadRequest, DownloadResponse> DOWNLOAD_LATEST_FILE_METHOD = TracDataApiGrpc.getDownloadLatestFileMethod();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataService dataRwService;
    private final FileService fileService;
    private final Validator validator;

    private final EventLoopRegister eventLoops;
    private final BufferAllocator rootAllocator;

    private final AtomicLong nextReqId;
    private final long reqInitAllocation;
    private final long reqMaxAllocation;


    public TracDataApi(
            DataService dataRwService, FileService fileService,
            EventLoopRegister eventLoops, BufferAllocator allocator) {

        this.dataRwService = dataRwService;
        this.fileService = fileService;
        this.eventLoops = eventLoops;
        this.rootAllocator = allocator;

        this.validator = new Validator();

        this.nextReqId = new AtomicLong();
        this.reqInitAllocation = DEFAULT_INITIAL_ALLOCATION;
        this.reqMaxAllocation = DEFAULT_MAX_ALLOCATION;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // API calls
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public StreamObserver<DataWriteRequest> createDataset(StreamObserver<TagHeader> responseObserver) {

        var dataContext = prepareDataContext();
        var userInfo = AuthHelpers.currentUser();

        var upload = new GrpcUploadSource<>(DataWriteRequest.class, responseObserver);
        upload.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(DataWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenApply(req -> validateRequest(CREATE_DATASET_METHOD, req))
                .thenCompose(req -> dataRwService.createDataset(req, dataStream, dataContext, userInfo))
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

        var dataContext = prepareDataContext();
        var userInfo = AuthHelpers.currentUser();

        var upload = new GrpcUploadSource<>(DataWriteRequest.class, responseObserver);
        upload.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(DataWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenApply(req -> validateRequest(UPDATE_DATASET_METHOD, req))
                .thenCompose(req -> dataRwService.updateDataset(req, dataStream, dataContext, userInfo))
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

        readDataset(READ_DATASET_METHOD, request, responseObserver, GrpcDownloadSink.STREAMING);
    }

    @Override
    public void readSmallDataset(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {

        readDataset(READ_SMALL_DATASET_METHOD, request, responseObserver, GrpcDownloadSink.AGGREGATED);
    }

    private void readDataset(
            MethodDescriptor<DataReadRequest, DataReadResponse> method,
            DataReadRequest request, StreamObserver<DataReadResponse> responseObserver,
            boolean streamingMode) {

        var dataContext = prepareDataContext();
        var userInfo = AuthHelpers.currentUser();

        var download = new GrpcDownloadSink<>(responseObserver, DataReadResponse::newBuilder, streamingMode);
        download.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = download.firstMessage(DataReadResponse.Builder::setSchema, SchemaDefinition.class);
        var dataStream = download.dataStream(DataReadResponse.Builder::setContent);

        download.start(request)
                .thenApply(req -> validateRequest(method, req))
                .thenAccept(req -> dataRwService.readDataset(req, firstMessage, dataStream, dataContext, userInfo))
                .exceptionally(download::failed);
    }

    @Override
    public StreamObserver<FileWriteRequest> createFile(StreamObserver<TagHeader> responseObserver) {

        var dataContext = prepareDataContext();
        var userInfo = AuthHelpers.currentUser();

        var upload = new GrpcUploadSource<>(FileWriteRequest.class, responseObserver);
        upload.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(FileWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenApply(req -> validateRequest(CREATE_FILE_METHOD, req))
                .thenCompose(req -> fileService.createFile(
                        req.getTenant(),
                        req.getTagUpdatesList(),
                        req.getName(),
                        req.getMimeType(),
                        req.hasSize() ? req.getSize() : null,
                        dataStream, dataContext, userInfo))
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

        var dataContext = prepareDataContext();
        var userInfo = AuthHelpers.currentUser();

        var upload = new GrpcUploadSource<>(FileWriteRequest.class, responseObserver);
        upload.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(FileWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenApply(req -> validateRequest(UPDATE_FILE_METHOD, req))
                .thenCompose(req -> fileService.updateFile(
                        req.getTenant(),
                        req.getTagUpdatesList(),
                        req.getPriorVersion(),
                        req.getName(),
                        req.getMimeType(),
                        req.hasSize() ? req.getSize() : null,
                        dataStream, dataContext, userInfo))
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

        var dataContext = prepareDataContext();
        var userInfo = AuthHelpers.currentUser();

        download.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = download.firstMessage(FileReadResponse.Builder::setFileDefinition, FileDefinition.class);
        var dataStream = download.dataStream(FileReadResponse.Builder::setContent);

        download.start(request)
                .thenApply(req -> validateRequest(method, req))
                .thenAccept(req -> fileService.readFile(
                        request.getTenant(),
                        request.getSelector(),
                        firstMessage, dataStream,
                        dataContext, userInfo))
                .exceptionally(download::failed);
    }

    @Override
    public void downloadFile(DownloadRequest request, StreamObserver<DownloadResponse> responseObserver) {

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.FILE)
                .setObjectId(request.getObjectId())
                .setObjectVersion(request.getObjectVersion())
                .setLatestTag(true)
                .build();

        var download = new GrpcDownloadSink<>(responseObserver, DownloadResponse::newBuilder, GrpcDownloadSink.STREAMING);
        downloadFile(DOWNLOAD_FILE_METHOD, request, selector, download);
    }

    @Override
    public void downloadLatestFile(DownloadRequest request, StreamObserver<DownloadResponse> responseObserver) {

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.FILE)
                .setObjectId(request.getObjectId())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var download = new GrpcDownloadSink<>(responseObserver, DownloadResponse::newBuilder, GrpcDownloadSink.STREAMING);
        downloadFile(DOWNLOAD_LATEST_FILE_METHOD, request, selector, download);
    }

    private void downloadFile(
            MethodDescriptor<DownloadRequest, DownloadResponse> method,
            DownloadRequest request, TagSelector selector,
            GrpcDownloadSink<DownloadResponse, DownloadResponse.Builder> download) {

        var dataContext = prepareDataContext();
        var userInfo = AuthHelpers.currentUser();

        download.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = download.firstMessage((response, fileDef) -> response
                .setContentType(fileDef.getMimeType())
                .setContentLength(fileDef.getSize()),
                FileDefinition.class);

        var dataStream = download.dataStream(DownloadResponse.Builder::setContent);

        download.start(request)
                .thenApply(req -> validateRequest(method, req))
                .thenAccept(req -> fileService.readFile(
                        request.getTenant(), selector,
                        firstMessage, dataStream,
                        dataContext, userInfo))
                .exceptionally(download::failed);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Common scaffolding for client and server streaming
    // -----------------------------------------------------------------------------------------------------------------

    private DataContext prepareDataContext() {

        // TODO: Universal request ID
        // This basic req-id is enough to create a unique allocator name for each request
        // Each service should generate a req-id for every inbound request
        // If service A calls service B, then B should also record "source-req-id" to link back
        // req-id and source-req-id should be included with every log message

        // Enforce strict requirement on the event loop
        // All processing for the request must happen on the EL originally assigned to the request

        var requestId = String.format("REQ-%d", nextReqId.incrementAndGet());
        var eventLoop = eventLoops.currentEventLoop(/* strict = */ true);
        var allocator = rootAllocator.newChildAllocator(requestId, reqInitAllocation, reqMaxAllocation);

        log.info("OPEN data context for [{}]", requestId);

        return new DataContext(eventLoop, allocator);
    }

    private void closeDataContext(IDataContext dataContext) {

        // this method is normally triggered by the last onComplete or onError event in the pipeline
        // However there can be clean-up that still needs to execute, often in finally blocks
        // Posting back to the event loop lets clean-up complete before the context is closed

        var eventLoop = dataContext.eventLoopExecutor();
        eventLoop.submit(() -> closeDataContextLater(dataContext));
    }

    private void closeDataContextLater(IDataContext dataContext) {

        try (var allocator = dataContext.arrowAllocator()) {

            var peak = allocator.getPeakMemoryAllocation();
            var retained = allocator.getAllocatedMemory();

            if (retained == 0)
                log.info("CLOSE data context for [{}], peak = [{}], retained = [{}]",
                        allocator.getName(),
                        LoggingHelpers.formatFileSize(peak),
                        LoggingHelpers.formatFileSize(retained));
            else
                log.warn("CLOSE data context for [{}], peak = [{}], retained = [{}] (memory leak)",
                        allocator.getName(),
                        LoggingHelpers.formatFileSize(peak),
                        LoggingHelpers.formatFileSize(retained));
        }
    }

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
