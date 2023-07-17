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

package org.finos.tracdap.plugins.aws.storage;

import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.storage.CommonFileReader;
import org.finos.tracdap.common.storage.StorageErrors;

import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import io.netty.util.concurrent.OrderedEventExecutor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static org.finos.tracdap.common.storage.CommonFileStorage.READ_OPERATION;
import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.OBJECT_SIZE_TOO_SMALL;


public class S3ObjectReader extends CommonFileReader {

    private static final long DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024;
    private static final int CHUNK_BUFFER_WINDOW = 2;
    private static final int AWS_REQUEST_WINDOW = 32;

    private final String storageKey;
    private final String storagePath;
    private final String bucket;
    private final String objectKey;

    private final boolean useRange;
    private final long offset;
    private final int size;

    private final S3AsyncClient client;
    private final OrderedEventExecutor executor;
    private final StorageErrors errors;

    private Subscription awsSubscription;
    private long awsRequested;


    public S3ObjectReader(
            String storageKey, String storagePath,
            String bucket, String objectKey,
            boolean useRange, long offset, int size,
            S3AsyncClient client,
            IDataContext dataContext,
            long chunkSize,
            StorageErrors errors) {

        super(dataContext, errors, storageKey, storagePath, chunkSize, CHUNK_BUFFER_WINDOW, AWS_REQUEST_WINDOW);

        this.storageKey = storageKey;
        this.storagePath = storagePath;
        this.bucket = bucket;
        this.objectKey = objectKey;

        this.useRange = useRange;
        this.offset = offset;
        this.size = size;

        this.client = client;
        this.executor = dataContext.eventLoopExecutor();
        this.errors = errors;
    }

    public S3ObjectReader(
            String storageKey, String storagePath,
            String bucket, String objectKey,
            long offset, int size,
            S3AsyncClient client,
            IDataContext dataContext,
            long chunkSize,
            StorageErrors errors) {

        this(storageKey, storagePath, bucket, objectKey, true, offset, size,
                client, dataContext, chunkSize, errors);
    }

    public S3ObjectReader(
            String storageKey, String storagePath,
            String bucket, String objectKey,
            S3AsyncClient client,
            IDataContext dataContext,
            long chunkSize,
            StorageErrors errors) {

        this(storageKey, storagePath, bucket, objectKey, false, 0, 0,
                client, dataContext, chunkSize, errors);
    }

    public S3ObjectReader(
            String storageKey, String storagePath,
            String bucket, String objectKey,
            S3AsyncClient client,
            IDataContext dataContext,
            StorageErrors errors) {

        this(storageKey, storagePath, bucket, objectKey, client, dataContext, DEFAULT_CHUNK_SIZE, errors);
    }

    @Override
    protected void clientStart() {

        var request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey);

        if (useRange) {
            var range = String.format("bytes=%d-%d", offset, offset + size - 1);
            request.range(range);
        }

        var handler = new ResponseHandler();
        client.getObject(request.build(), handler);
    }

    @Override
    protected void clientRequest(long n) {

        if (awsSubscription != null)
            awsSubscription.request(n);
        else
            awsRequested += n;
    }

    @Override
    protected void clientCancel() {

        if (awsSubscription != null) {
            awsSubscription.cancel();
        }

        else {
            log.warn("{} {} [{}]: Read operation cancelled before connection was established",
                    READ_OPERATION, storageKey, storagePath);
        }
    }

    private void _onPrepare(CompletableFuture<Void> signal) {

        signal.complete(null);
    }

    private void _onResponse(GetObjectResponse response) {

        if (useRange && response.contentLength() != size) {

            var error = errors.explicitError(READ_OPERATION, storagePath, OBJECT_SIZE_TOO_SMALL);
            onError(error);
        }
    }

    private void _onStream(SdkPublisher<ByteBuffer> publisher) {

        publisher.subscribe(new ResponseStream());
    }

    private void _onSubscribe(Subscription awsSubscription) {

        if (isDone()) {
            awsSubscription.cancel();
            return;
        }

        this.awsSubscription = awsSubscription;

        if (awsRequested > 0) {
            awsSubscription.request(awsRequested);
            awsRequested = 0;
        }
    }

    // Call back events from the AWS SDK are not necessarily in the event loop, so post them back
    // The AWS client is running with the main ELG, but requests are not sent to a specific EL

    private class ResponseHandler implements AsyncResponseTransformer<GetObjectResponse, Void> {

        @Override
        public CompletableFuture<Void> prepare() {
            var prepareResult = new CompletableFuture<Void>();
            executor.submit(() -> _onPrepare(prepareResult));
            return prepareResult;
        }

        @Override
        public void onResponse(GetObjectResponse response) {
            executor.submit(() -> _onResponse(response));
        }

        @Override
        public void onStream(SdkPublisher<ByteBuffer> publisher) {
            executor.submit(() -> _onStream(publisher));
        }

        @Override
        public void exceptionOccurred(Throwable error) {
            executor.submit(() -> onError(error));
        }
    }

    private class ResponseStream implements Subscriber<ByteBuffer> {

        @Override
        public void onSubscribe(Subscription s) {
            executor.submit(() -> _onSubscribe(s));
        }

        @Override
        public void onNext(ByteBuffer byteBuffer) {
            executor.submit(() -> onChunk(byteBuffer));
        }

        @Override
        public void onError(Throwable error) {
            executor.submit(() -> onError(error));
        }

        @Override
        public void onComplete() {
            executor.submit(S3ObjectReader.this::onComplete);
        }
    }

}
