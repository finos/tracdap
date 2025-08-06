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
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.data.pipeline.GrpcDownloadSink;
import org.finos.tracdap.common.data.pipeline.GrpcUploadSource;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.netty.EventLoopResolver;
import org.finos.tracdap.svc.data.service.DataService;
import org.finos.tracdap.svc.data.service.FileService;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.LoggerFactory;


public class TracDataApi extends TracDataApiGrpc.TracDataApiImplBase {

    private final DataService dataService;
    private final FileService fileService;
    private final ICodecManager formats;

    private final GrpcConcern commonConcerns;
    private final DataContextHelpers helpers;


    public TracDataApi(
            DataService dataService, FileService fileService,
            ICodecManager formats,
            EventLoopResolver eventLoopResolver,
            BufferAllocator allocator,
            GrpcConcern commonConcerns) {

        this.dataService = dataService;
        this.fileService = fileService;
        this.formats = formats;
        this.commonConcerns = commonConcerns;

        var log = LoggerFactory.getLogger(getClass());
        this.helpers = new DataContextHelpers(log, eventLoopResolver, allocator);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // API calls
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public StreamObserver<DataWriteRequest> createDataset(StreamObserver<TagHeader> responseObserver) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var clientConfig = commonConcerns.prepareClientCall(Context.current());
        var dataContext = helpers.prepareDataContext(requestMetadata);

        var upload = new GrpcUploadSource<>(DataWriteRequest.class, responseObserver);
        upload.whenComplete(() -> helpers.closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(DataWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenCompose(req -> dataService.createDataset(req, dataStream, dataContext, requestMetadata, clientConfig))
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

        var dataContext = helpers.prepareDataContext(requestMetadata);
        var upload = new GrpcUploadSource<>(DataWriteRequest.class, responseObserver);
        upload.whenComplete(() -> helpers.closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(DataWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenCompose(req -> dataService.updateDataset(req, dataStream, dataContext, requestMetadata, clientConfig))
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
        var dataContext = helpers.prepareDataContext(requestMetadata);

        var download = new GrpcDownloadSink<>(responseObserver, DataReadResponse::newBuilder, streamingMode);
        download.whenComplete(() -> helpers.closeDataContext(dataContext));

        var firstMessage = download.firstMessage(DataReadResponse.Builder::setSchema, SchemaDefinition.class);
        var dataStream = download.dataStream(DataReadResponse.Builder::setContent);

        download.start(request)
                .thenAccept(req -> dataService.readDataset(req, firstMessage, dataStream, dataContext, requestMetadata, clientConfig))
                .exceptionally(download::failed);
    }

    @Override
    public StreamObserver<FileWriteRequest> createFile(StreamObserver<TagHeader> responseObserver) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var clientConfig = commonConcerns.prepareClientCall(Context.current());
        var dataContext = helpers.prepareDataContext(requestMetadata);

        var upload = new GrpcUploadSource<>(FileWriteRequest.class, responseObserver);
        upload.whenComplete(() -> helpers.closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(FileWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenCompose(request -> fileService.createFile(
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
        var dataContext = helpers.prepareDataContext(requestMetadata);

        var upload = new GrpcUploadSource<>(FileWriteRequest.class, responseObserver);
        upload.whenComplete(() -> helpers.closeDataContext(dataContext));

        var firstMessage = upload.firstMessage();
        var dataStream = upload.dataStream(FileWriteRequest::getContent, dataContext.arrowAllocator());

        firstMessage
                .thenCompose(request -> fileService.updateFile(
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
        var dataContext = helpers.prepareDataContext(requestMetadata);

        download.whenComplete(() -> helpers.closeDataContext(dataContext));

        var firstMessage = download.firstMessage(FileReadResponse.Builder::setFileDefinition, FileDefinition.class);
        var dataStream = download.dataStream(FileReadResponse.Builder::setContent);

        download.start(request)
                .thenAccept(req -> fileService.readFile(
                        request, requestMetadata,
                        firstMessage, dataStream,
                        dataContext, clientConfig))
                .exceptionally(download::failed);
    }

    @Override
    public void downloadFile(FileDownloadRequest request, StreamObserver<DownloadResponse> responseObserver) {

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
    public void downloadLatestFile(FileDownloadRequest request, StreamObserver<DownloadResponse> responseObserver) {

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
            FileDownloadRequest downloadRequest, TagSelector selector,
            GrpcDownloadSink<DownloadResponse, DownloadResponse.Builder> download) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var clientConfig = commonConcerns.prepareClientCall(Context.current());
        var dataContext = helpers.prepareDataContext(requestMetadata);

        download.whenComplete(() -> helpers.closeDataContext(dataContext));

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
                .thenAccept(request -> fileService.readFile(
                        request, requestMetadata,
                        firstMessage, dataStream,
                        dataContext, clientConfig))
                .exceptionally(download::failed);
    }

    @Override
    public void downloadData(DataDownloadRequest request, StreamObserver<DownloadResponse> responseObserver) {

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(request.getObjectId())
                .setObjectVersion(request.getObjectVersion())
                .setLatestTag(true)
                .build();

        var download = new GrpcDownloadSink<>(responseObserver, DownloadResponse::newBuilder, GrpcDownloadSink.STREAMING);
        downloadData(request, selector, download);
    }

    @Override
    public void downloadLatestData(DataDownloadRequest request, StreamObserver<DownloadResponse> responseObserver) {

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(request.getObjectId())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var download = new GrpcDownloadSink<>(responseObserver, DownloadResponse::newBuilder, GrpcDownloadSink.STREAMING);
        downloadData(request, selector, download);
    }

    private void downloadData(
            DataDownloadRequest downloadRequest, TagSelector selector,
            GrpcDownloadSink<DownloadResponse, DownloadResponse.Builder> download) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var clientConfig = commonConcerns.prepareClientCall(Context.current());

        var dataContext = helpers.prepareDataContext(requestMetadata);
        download.whenComplete(() -> helpers.closeDataContext(dataContext));

        var firstMessage = download.firstMessage(
                (response, schema) -> downloadDataFirstMessage(downloadRequest, response),
                SchemaDefinition.class);

        var dataStream = download.dataStream(DownloadResponse.Builder::setContent);

        // Translate into a regular file read request for the service layer
        var readRequest = DataReadRequest.newBuilder()
                .setTenant(downloadRequest.getTenant())
                .setSelector(selector)
                .setFormat(downloadRequest.getFormat())
                .build();

        download.start(readRequest)
                .thenAccept(req -> dataService.readDataset(req, firstMessage, dataStream, dataContext, requestMetadata, clientConfig))
                .exceptionally(download::failed);

    }

    private DownloadResponse.Builder downloadDataFirstMessage(
            DataDownloadRequest downloadRequest,
            DownloadResponse.Builder response) {

        var mimeType = formats.getDefaultMimeType(downloadRequest.getFormat());

        return response
                .setContentType(mimeType)
                .clearContentLength();
    }
}
