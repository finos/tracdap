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
import org.finos.tracdap.common.data.pipeline.GrpcDownloadSink;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.netty.EventLoopResolver;
import org.finos.tracdap.svc.data.service.StorageService;

import org.apache.arrow.memory.BufferAllocator;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.slf4j.LoggerFactory;


public class TracStorageApi extends TracStorageApiGrpc.TracStorageApiImplBase {

    private final StorageService storageService;

    private final DataContextHelpers helpers;

    public TracStorageApi(
            StorageService storageService,
            EventLoopResolver eventLoopResolver,
            BufferAllocator allocator) {

        this.storageService = storageService;

        var log = LoggerFactory.getLogger(getClass());
        this.helpers = new DataContextHelpers(log, eventLoopResolver, allocator);
    }

    @Override
    public void exists(StorageRequest request, StreamObserver<StorageExistsResponse> responseObserver) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var dataContext = helpers.prepareDataContext(requestMetadata);

        storageService.exists(request, dataContext)
                .whenComplete((result, error) -> unaryResponse(result, error, responseObserver, dataContext));
    }

    @Override
    public void size(StorageRequest request, StreamObserver<StorageSizeResponse> responseObserver) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var dataContext = helpers.prepareDataContext(requestMetadata);

        storageService.size(request, dataContext)
                .whenComplete((result, error) -> unaryResponse(result, error, responseObserver, dataContext));
    }

    @Override
    public void stat(StorageRequest request, StreamObserver<StorageStatResponse> responseObserver) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var dataContext = helpers.prepareDataContext(requestMetadata);

        storageService.stat(request, dataContext)
                .whenComplete((result, error) -> unaryResponse(result, error, responseObserver, dataContext));
    }

    @Override
    public void ls(StorageRequest request, StreamObserver<StorageLsResponse> responseObserver) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var dataContext = helpers.prepareDataContext(requestMetadata);

        storageService.ls(request, dataContext)
                .whenComplete((result, error) -> unaryResponse(result, error, responseObserver, dataContext));
    }

    @Override
    public void readFile(StorageReadRequest request, StreamObserver<StorageReadResponse> responseObserver) {

        var download = new GrpcDownloadSink<>(responseObserver, StorageReadResponse::newBuilder, GrpcDownloadSink.STREAMING);
        readFile(request, download);
    }

    @Override
    public void readSmallFile(StorageReadRequest request, StreamObserver<StorageReadResponse> responseObserver) {

        var download = new GrpcDownloadSink<>(responseObserver, StorageReadResponse::newBuilder, GrpcDownloadSink.AGGREGATED);
        readFile(request, download);
    }

    private void readFile(StorageReadRequest request, GrpcDownloadSink<StorageReadResponse, StorageReadResponse.Builder> download) {

        var requestMetadata = RequestMetadata.get(Context.current());
        var dataContext = helpers.prepareDataContext(requestMetadata);

        download.whenComplete(() -> helpers.closeDataContext(dataContext));

        var firstMessage = download.firstMessage(StorageReadResponse.Builder::setStat, FileStat.class);
        var dataStream = download.dataStream(StorageReadResponse.Builder::setContent);

        download.start(request)
                .thenAccept(req -> storageService.readFile(request, firstMessage, dataStream, dataContext))
                .exceptionally(download::failed);
    }

    private <TResponse> void unaryResponse(
            TResponse result, Throwable error,
            StreamObserver<TResponse> responseObserver,
            DataContext dataContext) {

        try {
            if (error != null) {
                responseObserver.onError(error);
            }
            else {
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }
        }
        finally {
            helpers.closeDataContext(dataContext);
        }
    }
}
