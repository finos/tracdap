/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.plugins.gcp.storage;

import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.storage.CommonFileReader;
import org.finos.tracdap.common.storage.StorageErrors;

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.StateCheckingResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.storage.v2.*;
import io.grpc.CallOptions;


public class GcsObjectReader extends CommonFileReader {

    private static final long DEFAULT_CHUNK_SIZE = 2 * 1048576;  // 2 MB
    private static final int DEFAULT_CHUNK_BUFFER = 2;
    private static final int DEFAULT_CLIENT_BUFFER = 2;

    private final StorageClient storageClient;
    private final IDataContext dataContext;

    private final BucketName bucketName;
    private final String objectKey;
    private final long offset;
    private final long limit;

    private StreamController gcpController;
    private int gcpRequested;


    GcsObjectReader(
            StorageClient storageClient, IDataContext dataContext, StorageErrors errors,
            String storageKey, String storagePath, BucketName bucketName, String objectKey,
            long offset, long limit, long chunkSize, int chunkBuffer, int clientBuffer) {

        super(dataContext, errors, storageKey, storagePath,
                chunkSize, chunkBuffer, clientBuffer);

        this.storageClient = storageClient;
        this.dataContext = dataContext;

        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.offset = offset;
        this.limit = limit;
    }

    GcsObjectReader(
            StorageClient storageClient, IDataContext dataContext, StorageErrors errors,
            String storageKey, String storagePath, BucketName bucketName, String objectKey,
            long offset, long limit, long chunkSize) {

        this(storageClient, dataContext, errors,
                storageKey, storagePath, bucketName, objectKey, offset, limit,
                chunkSize, DEFAULT_CHUNK_BUFFER, DEFAULT_CLIENT_BUFFER);
    }

    GcsObjectReader(
            StorageClient storageClient, IDataContext dataContext, StorageErrors errors,
            String storageKey, String storagePath, BucketName bucketName, String objectKey,
            long offset, long limit) {

        this(storageClient, dataContext, errors,
                storageKey, storagePath, bucketName, objectKey, offset, limit,
                DEFAULT_CHUNK_SIZE);
    }

    GcsObjectReader(
            StorageClient storageClient, IDataContext dataContext, StorageErrors errors,
            String storageKey, String storagePath, BucketName bucketName, String objectKey) {

        this(storageClient, dataContext, errors, storageKey, storagePath, bucketName, objectKey, 0, 0);
    }

    @Override
    protected void clientStart() {

        var request = ReadObjectRequest.newBuilder()
                .setBucket(bucketName.toString())
                .setObject(objectKey)
                .setReadOffset(offset)
                .setReadLimit(limit)
                .build();

        var readCall = storageClient.readObjectCallable();
        var readStream = new ApiResponseStream();

        var callOptions = CallOptions.DEFAULT.withExecutor(dataContext.eventLoopExecutor());
        var callCtx = GrpcCallContext.createDefault().withCallOptions(callOptions);

        readCall.call(request, readStream, callCtx);
    }

    @Override
    protected void clientRequest(long n) {

        if (gcpController != null)
            gcpController.request((int) n);
        else
            gcpRequested += (int) n;
    }

    @Override
    protected void clientCancel() {

        if (gcpController != null) {
            gcpController.cancel();
            gcpController = null;
        }
    }

    private class ApiResponseStream extends StateCheckingResponseObserver<ReadObjectResponse> {

        @Override
        protected void onStartImpl(StreamController controller) {

            gcpController = controller;
            gcpController.disableAutoInboundFlowControl();

            if (gcpRequested > 0) {
                gcpController.request(gcpRequested);
                gcpRequested = 0;
            }
        }

        @Override
        protected void onResponseImpl(ReadObjectResponse response) {

            var data = response.getChecksummedData().getContent();

            GcsObjectReader.this.onChunk(data.asReadOnlyByteBuffer());
        }

        @Override
        protected void onErrorImpl(Throwable t) {

            GcsObjectReader.this.onError(t);
        }

        @Override
        protected void onCompleteImpl() {

            GcsObjectReader.this.onComplete();
        }
    }
}
