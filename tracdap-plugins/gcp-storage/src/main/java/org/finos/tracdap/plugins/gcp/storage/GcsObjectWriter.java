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

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ApiStreamObserver;
import com.google.api.gax.rpc.CancelledException;
import com.google.api.gax.rpc.ClientStreamingCallable;
import com.google.protobuf.ByteString;
import com.google.storage.v2.*;
import com.google.storage.v2.Object;
import io.grpc.CallOptions;

import org.apache.arrow.memory.ArrowBuf;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;


public class GcsObjectWriter implements Flow.Subscriber<ArrowBuf> {

    private final StorageClient storageClient;
    private final IDataContext dataContext;
    private final WriteObjectSpec objectSpec;
    private final CompletableFuture<Long> signal;

    private Flow.Subscription subscription;
    private ApiStreamObserver<WriteObjectRequest> apiStream;
    private long bytesSent;

    private Throwable upstreamError;

    GcsObjectWriter(
            StorageClient storageClient, IDataContext dataContext,
            BucketName bucketName, String objectKey,
            CompletableFuture<Long> signal) {

        this.storageClient = storageClient;
        this.dataContext = dataContext;
        this.signal = signal;

        this.objectSpec = WriteObjectSpec.newBuilder()
                .setResource(Object.newBuilder()
                .setBucket(bucketName.toString())
                .setName(objectKey))
                .build();
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        var callOptions = CallOptions.DEFAULT
                .withExecutor(dataContext.eventLoopExecutor());

        var callCtx = GrpcCallContext.createDefault()
                .withCallOptions(callOptions);

        var responseStream = new GcpUnaryResponse<WriteObjectResponse>();

        var apiCall = addMissingRequestParams(storageClient.writeObjectCallable());
        this.apiStream = apiCall.clientStreamingCall(responseStream, callCtx);
        this.subscription = subscription;

        responseStream.getResult().whenComplete(this::writeCompleteHandler);

        subscription.request(1);
    }

    @Override
    public void onNext(ArrowBuf item) {

        try (item) {

            System.out.println("Sending next");

            var MAX_CHUNK_SIZE = 2 * 1048576;  // 2 MB

            var buffer = item.nioBuffer();

            while (buffer.remaining() > 0) {

                var chunkSize = Math.min(buffer.remaining(), MAX_CHUNK_SIZE);
                var protoBytes = ByteString.copyFrom(buffer, chunkSize);

                var data = ChecksummedData.newBuilder()
                        .setContent(protoBytes);

                var request = WriteObjectRequest.newBuilder()
                        .setWriteObjectSpec(objectSpec)
                        .setChecksummedData(data)
                        .setWriteOffset(bytesSent)
                        .build();

                bytesSent += protoBytes.size();

                apiStream.onNext(request);
            }

            subscription.request(1);

            System.out.println("Sent next, bytes = " + bytesSent);
        }
    }

    @Override
    public void onError(Throwable throwable) {

        if (upstreamError == null) {

            upstreamError = throwable;

            // onError() can be called before onSubscribe() if there was a problem during subscription

            if (apiStream != null)
                apiStream.onError(throwable);
            else
                signal.completeExceptionally(throwable);
        }
        else {

            // todo: log warning for any subsequent errors
        }
    }

    @Override
    public void onComplete() {

        System.out.println("Sending complete");

        var request = WriteObjectRequest.newBuilder()
                .setWriteObjectSpec(objectSpec)
                .setWriteOffset(bytesSent)
                .setFinishWrite(true)
                .build();

        apiStream.onNext(request);
        apiStream.onCompleted();

        System.out.println("Sent complete, bytes = " + bytesSent);
    }

    private void writeCompleteHandler(WriteObjectResponse result, Throwable error) {

        System.out.println("In the callback");

        if (error != null) {

            // If the error is a cancellation due to an up-stream error,
            // Then the client will want the original error instead of the cancellation signal

            if (error instanceof CancelledException && upstreamError != null)
                signal.completeExceptionally(upstreamError);
            else
                signal.completeExceptionally(error);

            return;
        }

        // result.getPersistedSize() only contains a value if the operation is not finalized
        // This is normally zero, so, use bytesSent, which counted as chunks were dispatched

        signal.complete(bytesSent);
    }

    private <TRequest, TResponse>
    ClientStreamingCallable<TRequest, TResponse>
    addMissingRequestParams(ClientStreamingCallable<TRequest, TResponse> callable) {

        // https://github.com/googleapis/java-storage/blob/main/google-cloud-storage/src/main/java/com/google/cloud/storage/WriteFlushStrategy.java#L89

        // GCP SDK adds in this required header
        // For some API calls / usage patterns, the header does not get added

        var bucket = objectSpec.getResource().getBucket();
        System.out.println(bucket);

        var callParams = String.format("bucket=%s", objectSpec.getResource().getBucket());
        var callMetadata = Map.of("x-goog-request-params", List.of(callParams));

        var defaultContext = GrpcCallContext.createDefault().withExtraHeaders(callMetadata);

        return callable.withDefaultCallContext(defaultContext);
    }
}
