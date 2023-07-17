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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;


public class GcsObjectWriter implements Flow.Subscriber<ArrowBuf> {

    // gRPC has a default max message size of 4 MB, message over this size will be rejected
    // Using a max chunk size of 3 MB is safely inside the limit
    // Allowing a range of chunk sizes should allow chunks to flow with less fragmentation

    private final static long MAX_CHUNK_SIZE = 3145728;  // 3 MB
    private final static long MIN_CHUNK_SIZE = 1572864;  // 1.5 MB

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final StorageClient storageClient;
    private final IDataContext dataContext;
    private final CompletableFuture<Long> signal;

    private final WriteObjectSpec gcsObjectSpec;
    private ApiStreamObserver<WriteObjectRequest> gcsWriteStream;

    private Flow.Subscription subscription;
    private ByteString pendingChunk;
    private long bytesSent;
    private Throwable upstreamError;

    GcsObjectWriter(
            StorageClient storageClient, IDataContext dataContext,
            BucketName bucketName, String objectKey,
            CompletableFuture<Long> signal) {

        this.storageClient = storageClient;
        this.dataContext = dataContext;
        this.signal = signal;

        this.gcsObjectSpec = WriteObjectSpec.newBuilder()
                .setResource(Object.newBuilder()
                .setBucket(bucketName.toString())
                .setName(objectKey))
                .build();

        this.pendingChunk = ByteString.empty();
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        if (this.subscription != null)
            throw new IllegalStateException();
        else
            this.subscription = subscription;

        var callOptions = CallOptions.DEFAULT
                .withExecutor(dataContext.eventLoopExecutor());

        var callCtx = GrpcCallContext.createDefault()
                .withCallOptions(callOptions);

        var apiCall = addMissingRequestParams(storageClient.writeObjectCallable());

        var gcsResponseStream = new GcpUnaryResponse<WriteObjectResponse>();
        this.gcsWriteStream = apiCall.clientStreamingCall(gcsResponseStream, callCtx);
        gcsResponseStream.getResult().whenComplete(this::writeCompleteHandler);

        var initialRequest = WriteObjectRequest.newBuilder()
                .setWriteObjectSpec(gcsObjectSpec)
                .build();

        gcsWriteStream.onNext(initialRequest);

        subscription.request(2);
    }

    @Override
    public void onNext(ArrowBuf item) {

        try (item) {  // auto-release buffer content

            var buffer = item.nioBuffer();

            while (buffer.remaining() > 0) {

                var chunkRemaining = MAX_CHUNK_SIZE - pendingChunk.size();
                var nBytes = (int) Math.min(buffer.remaining(), chunkRemaining);

                // There is no easy way to release bytes once the chunk is written to the stream
                // So we have to copy and let ByteString handle cleanup, which relies on the Java GC

                var protoBytes = ByteString.copyFrom(buffer, nBytes);
                pendingChunk = pendingChunk.concat(protoBytes);

                if (pendingChunk.size() >= MIN_CHUNK_SIZE) {

                    var data = ChecksummedData.newBuilder()
                            .setContent(protoBytes);

                    var request = WriteObjectRequest.newBuilder()
                            .setChecksummedData(data)
                            .setWriteOffset(bytesSent)
                            .build();

                    gcsWriteStream.onNext(request);

                    bytesSent += pendingChunk.size();
                    pendingChunk = ByteString.empty();
                }
            }

            subscription.request(1);
        }
    }

    @Override
    public void onError(Throwable throwable) {

        // A call to onError() means an error occurred upstream in the data pipeline
        // It is not an error that occurred in the storage client

        // If the GCS stream is open, we can pass the onError signal into the stream
        // This will cause the stream to cancel and call back the write complete handler
        // Otherwise, we have to notify the write complete signal directly

        // Only one error can be sent down the pipe and ultimately get passed back to the client
        // There can be multiple errors if the stream doesn't fail cleanly, in this case log a warning

        if (upstreamError == null) {

            upstreamError = throwable;

            // onError() can be called before onSubscribe() if there was a problem during subscription

            if (gcsWriteStream != null)
                gcsWriteStream.onError(throwable);
            else
                signal.completeExceptionally(throwable);
        }
        else {

            log.warn("Another error was reported after the write operation already failed");
            log.warn(throwable.getMessage(), throwable);
        }
    }

    @Override
    public void onComplete() {

        var request = WriteObjectRequest.newBuilder()
                .setFinishWrite(true);

        if (pendingChunk.size() > 0) {

            var data = ChecksummedData.newBuilder().setContent(pendingChunk);
            request.setWriteOffset(bytesSent).setChecksummedData(data);

            bytesSent += pendingChunk.size();
            pendingChunk = ByteString.empty();
        }

        gcsWriteStream.onNext(request.build());
        gcsWriteStream.onCompleted();
    }

    private void writeCompleteHandler(WriteObjectResponse result, Throwable error) {

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

        var callParams = String.format("bucket=%s", gcsObjectSpec.getResource().getBucket());
        var callMetadata = Map.of("x-goog-request-params", List.of(callParams));

        var defaultContext = GrpcCallContext.createDefault().withExtraHeaders(callMetadata);

        return callable.withDefaultCallContext(defaultContext);
    }
}
