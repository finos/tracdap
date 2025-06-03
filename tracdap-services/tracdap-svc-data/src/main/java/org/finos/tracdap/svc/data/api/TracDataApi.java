/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
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
import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.data.pipeline.GrpcDownloadSink;
import org.finos.tracdap.common.data.pipeline.GrpcUploadSource;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.netty.EventLoopResolver;
import org.finos.tracdap.common.util.LoggingHelpers;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.data.service.DataService;
import org.finos.tracdap.svc.data.service.FileService;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.memory.BufferAllocator;
import org.finos.tracdap.svc.data.service.TenantServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TracDataApi extends TracDataApiGrpc.TracDataApiImplBase {

    private static final long DEFAULT_INITIAL_ALLOCATION = 16 * 1024 * 1024;
    private static final long DEFAULT_MAX_ALLOCATION = 128 * 1024 * 1024;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final TenantServices.Map services;

    private final EventLoopResolver eventLoopResolver;
    private final BufferAllocator rootAllocator;
    private final GrpcConcern commonConcerns;

    private final long reqInitAllocation;
    private final long reqMaxAllocation;


    public TracDataApi(
            TenantServices.Map services,
            EventLoopResolver eventLoopResolver,
            BufferAllocator allocator,
            GrpcConcern commonConcerns) {

        this.services = services;

        this.eventLoopResolver = eventLoopResolver;
        this.rootAllocator = allocator;
        this.commonConcerns = commonConcerns;

        this.reqInitAllocation = DEFAULT_INITIAL_ALLOCATION;
        this.reqMaxAllocation = DEFAULT_MAX_ALLOCATION;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // API calls
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public StreamObserver<DataWriteRequest> createDataset(StreamObserver<TagHeader> responseObserver) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var clientConfig = commonConcerns.prepareClientCall(Context.current());
        var dataContext = prepareDataContext(requestMetadata);

        var upload = new GrpcUploadSource<>(DataWriteRequest.class, responseObserver);
        upload.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(DataWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenCompose(req -> tenantDataService(req).createDataset(req, dataStream, dataContext, requestMetadata, clientConfig))
                .thenAccept(upload::succeeded)
                .exceptionally(upload::failed);

        return upload.start();
    }

    @Override
    public void createSmallDataset(DataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        var upload = createDataset(responseObserver);

        upload.onNext(request);
        upload.onCompleted();
    }

    @Override
    public StreamObserver<DataWriteRequest> updateDataset(StreamObserver<TagHeader> responseObserver) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var clientConfig = commonConcerns.prepareClientCall(Context.current());

        var dataContext = prepareDataContext(requestMetadata);
        var upload = new GrpcUploadSource<>(DataWriteRequest.class, responseObserver);
        upload.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(DataWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenCompose(req -> tenantDataService(req).updateDataset(req, dataStream, dataContext, requestMetadata, clientConfig))
                .thenAccept(upload::succeeded)
                .exceptionally(upload::failed);

        return upload.start();
    }

    @Override
    public void updateSmallDataset(DataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        var upload = updateDataset(responseObserver);

        upload.onNext(request);
        upload.onCompleted();
    }

    @Override
    public void readDataset(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {

        readDataset(request, responseObserver, GrpcDownloadSink.STREAMING);
    }

    @Override
    public void readSmallDataset(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {

        readDataset(request, responseObserver, GrpcDownloadSink.AGGREGATED);
    }

    private void readDataset(
            DataReadRequest request, StreamObserver<DataReadResponse> responseObserver,
            boolean streamingMode) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var clientConfig = commonConcerns.prepareClientCall(Context.current());
        var dataContext = prepareDataContext(requestMetadata);

        var download = new GrpcDownloadSink<>(responseObserver, DataReadResponse::newBuilder, streamingMode);
        download.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = download.firstMessage(DataReadResponse.Builder::setSchema, SchemaDefinition.class);
        var dataStream = download.dataStream(DataReadResponse.Builder::setContent);

        download.start(request)
                .thenAccept(req -> tenantDataService(req).readDataset(req, firstMessage, dataStream, dataContext, requestMetadata, clientConfig))
                .exceptionally(download::failed);
    }

    @Override
    public StreamObserver<FileWriteRequest> createFile(StreamObserver<TagHeader> responseObserver) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var clientConfig = commonConcerns.prepareClientCall(Context.current());
        var dataContext = prepareDataContext(requestMetadata);

        var upload = new GrpcUploadSource<>(FileWriteRequest.class, responseObserver);
        upload.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(FileWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenCompose(request -> tenantFileService(request).createFile(
                        request, requestMetadata,
                        dataStream, dataContext, clientConfig))
                .thenAccept(upload::succeeded)
                .exceptionally(upload::failed);

        return upload.start();
    }

    @Override
    public void createSmallFile(FileWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        var upload = createFile(responseObserver);

        upload.onNext(request);
        upload.onCompleted();
    }

    @Override
    public StreamObserver<FileWriteRequest> updateFile(StreamObserver<TagHeader> responseObserver) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var clientConfig = commonConcerns.prepareClientCall(Context.current());
        var dataContext = prepareDataContext(requestMetadata);

        var upload = new GrpcUploadSource<>(FileWriteRequest.class, responseObserver);
        upload.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(FileWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenCompose(request -> tenantFileService(request).updateFile(
                        request,  requestMetadata,
                        dataStream, dataContext, clientConfig))
                .thenAccept(upload::succeeded)
                .exceptionally(upload::failed);

        return upload.start();
    }

    @Override
    public void updateSmallFile(FileWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        var upload = updateFile(responseObserver);

        upload.onNext(request);
        upload.onCompleted();
    }

    @Override
    public void readFile(FileReadRequest request, StreamObserver<FileReadResponse> responseObserver) {

        var download = new GrpcDownloadSink<>(responseObserver, FileReadResponse::newBuilder, GrpcDownloadSink.STREAMING);
        readFile(request, download);
    }

    @Override
    public void readSmallFile(FileReadRequest request, StreamObserver<FileReadResponse> responseObserver) {

        var download = new GrpcDownloadSink<>(responseObserver, FileReadResponse::newBuilder, GrpcDownloadSink.AGGREGATED);
        readFile(request, download);
    }

    private void readFile(FileReadRequest request, GrpcDownloadSink<FileReadResponse, FileReadResponse.Builder> download) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var clientConfig = commonConcerns.prepareClientCall(Context.current());
        var dataContext = prepareDataContext(requestMetadata);

        download.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = download.firstMessage(FileReadResponse.Builder::setFileDefinition, FileDefinition.class);
        var dataStream = download.dataStream(FileReadResponse.Builder::setContent);

        download.start(request)
                .thenAccept(req -> tenantFileService(req).readFile(
                        request, requestMetadata,
                        firstMessage, dataStream,
                        dataContext, clientConfig))
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
        downloadFile(request, selector, download);
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
        downloadFile(request, selector, download);
    }

    private void downloadFile(
            DownloadRequest downloadRequest, TagSelector selector,
            GrpcDownloadSink<DownloadResponse, DownloadResponse.Builder> download) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var clientConfig = commonConcerns.prepareClientCall(Context.current());
        var dataContext = prepareDataContext(requestMetadata);

        download.whenComplete(() -> closeDataContext(dataContext));

        var firstMessage = download.firstMessage((response, fileDef) -> response
                .setContentType(fileDef.getMimeType())
                .setContentLength(fileDef.getSize()),
                FileDefinition.class);

        var dataStream = download.dataStream(DownloadResponse.Builder::setContent);

        // Translate into a regular file read request for the service layer
        var readRequest = FileReadRequest.newBuilder()
                .setTenant(downloadRequest.getTenant())
                .setSelector(selector)
                .build();

        download.start(readRequest)
                .thenAccept(request -> tenantFileService(request).readFile(
                        request, requestMetadata,
                        firstMessage, dataStream,
                        dataContext, clientConfig))
                .exceptionally(download::failed);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Common scaffolding for client and server streaming
    // -----------------------------------------------------------------------------------------------------------------

    private DataContext prepareDataContext(RequestMetadata requestMetadata) {

        // Enforce strict requirement on the event loop
        // All processing for the request must happen on the EL originally assigned to the request

        var requestId = requestMetadata.requestId();
        var eventLoop = eventLoopResolver.currentEventLoop(/* strict = */ true);
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

    private DataService tenantDataService(DataWriteRequest request) {

        return services.lookupTenant(request.getTenant()).getDataService();
    }

    private DataService tenantDataService(DataReadRequest request) {

        return services.lookupTenant(request.getTenant()).getDataService();
    }

    private FileService tenantFileService(FileWriteRequest request) {

        return services.lookupTenant(request.getTenant()).getFileService();
    }

    private FileService tenantFileService(FileReadRequest request) {

        return services.lookupTenant(request.getTenant()).getFileService();
    }
}
