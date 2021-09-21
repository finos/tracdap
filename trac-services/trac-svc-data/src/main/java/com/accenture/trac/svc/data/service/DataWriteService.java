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

package com.accenture.trac.svc.data.service;

import com.accenture.trac.api.TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub;
import com.accenture.trac.common.eventloop.IExecutionContext;
import com.accenture.trac.common.storage.IStorageManager;
import com.accenture.trac.metadata.*;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


public class DataWriteService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IStorageManager storageManager;
    private final TrustedMetadataApiFutureStub metaApi;

    public DataWriteService(
            IStorageManager storageManager,
            TrustedMetadataApiFutureStub metaApi) {

        this.storageManager = storageManager;
        this.metaApi = metaApi;
    }

    public CompletionStage<Long> createFile(
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execContext) {

        log.info("In service method...");

        var storage = storageManager.getFileStorage("DUMMY_STORAGE");
        var storagePath = UUID.randomUUID() + ".txt";

        var signal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, signal, execContext);
        contentStream.subscribe(writer);

        return signal.thenApply(nBytes -> nBytes);  // TagHeader.newBuilder().build());

//        return CompletableFuture.failedFuture(new Exception("Not implemented yet"));
    }


}
