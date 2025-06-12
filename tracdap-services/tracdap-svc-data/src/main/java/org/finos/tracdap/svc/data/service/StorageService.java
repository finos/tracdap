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

package org.finos.tracdap.svc.data.service;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.data.IExecutionContext;
import org.finos.tracdap.common.metadata.MetadataCodec;

import org.apache.arrow.memory.ArrowBuf;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;


public class StorageService {

    private final TenantStorageManager storageManager;

    public StorageService(TenantStorageManager storageManager) {
        this.storageManager = storageManager;
    }

    public CompletionStage<StorageExistsResponse>
    exists(StorageRequest request, IExecutionContext context) {

        var storage = storageManager
                .getTenantStorage(request.getTenant())
                .getFileStorage(request.getStorageKey());

        return storage
                .exists(request.getStoragePath(), context)
                .thenApply(exists -> StorageExistsResponse.newBuilder().setExists(exists).build());
    }

    public CompletionStage<StorageSizeResponse>
    size(StorageRequest request, IExecutionContext context) {

        var storage = storageManager
                .getTenantStorage(request.getTenant())
                .getFileStorage(request.getStorageKey());

        return storage
                .size(request.getStoragePath(), context)
                .thenApply(size -> StorageSizeResponse.newBuilder().setSize(size).build());
    }

    public CompletionStage<StorageStatResponse>
    stat(StorageRequest request, IExecutionContext context) {

        var storage = storageManager
                .getTenantStorage(request.getTenant())
                .getFileStorage(request.getStorageKey());

        return storage
                .stat(request.getStoragePath(), context)
                .thenApply(StorageService::convertFileStat)
                .thenApply(stat -> StorageStatResponse.newBuilder().setStat(stat).build());
    }

    public CompletionStage<StorageLsResponse>
    ls(StorageRequest request, IExecutionContext context) {

        var storage = storageManager
                .getTenantStorage(request.getTenant())
                .getFileStorage(request.getStorageKey());

        return storage
                .ls(request.getStoragePath(), context)
                .thenApply(StorageService::convertFileStatList)
                .thenApply(ls -> StorageLsResponse.newBuilder().addAllStat(ls).build());
    }

    private static List<FileStat> convertFileStatList(List<org.finos.tracdap.common.storage.FileStat> fileStatList) {

        return fileStatList.stream()
                .map(StorageService::convertFileStat)
                .collect(Collectors.toList());
    }

    private static FileStat convertFileStat(org.finos.tracdap.common.storage.FileStat fileStat) {

        return FileStat.newBuilder()
                .setStoragePath(fileStat.storagePath)
                .setFileName(fileStat.fileName)
                .setFileType(convertFileType(fileStat.fileType))
                .setFileSize(fileStat.size)
                .setMtime(MetadataCodec.encodeDatetime(fileStat.mtime))
                .setAtime(MetadataCodec.encodeDatetime(fileStat.atime))
                .build();
    }

    public void readFile(
            StorageReadRequest request,
            CompletableFuture<FileStat> statResult,
            Flow.Subscriber<ArrowBuf> contentStream,
            IDataContext dataCtx) {

        var storage = storageManager
                .getTenantStorage(request.getTenant())
                .getFileStorage(request.getStorageKey());

        CompletableFuture.completedFuture(0)

                .thenCompose(x -> storage.stat(request.getStoragePath(), dataCtx))
                .thenApply(StorageService::convertFileStat)
                .thenAccept(statResult::complete)

                .thenApply(x -> storage.reader(request.getStoragePath(), dataCtx))
                .thenAccept(byteStream -> byteStream.subscribe(contentStream))

                .exceptionally(error -> Helpers.reportError(error, statResult, contentStream));
    }

    private static FileType convertFileType(org.finos.tracdap.common.storage.FileType fileType) {

        switch (fileType) {

            case FILE:
                return FileType.FILE;

            case DIRECTORY:
                return FileType.DIRECTORY;

            default:
                return FileType.UNRECOGNIZED;
        }
    }
}
