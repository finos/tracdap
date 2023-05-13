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

import org.apache.arrow.memory.BufferAllocator;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.storage.StorageErrors;
import org.finos.tracdap.common.util.LoggingHelpers;


import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.netty.util.concurrent.OrderedEventExecutor;
import org.apache.arrow.memory.ArrowBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.finos.tracdap.common.storage.CommonFileStorage.READ_OPERATION;
import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.DUPLICATE_SUBSCRIPTION;
import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.OBJECT_SIZE_TOO_SMALL;


public class S3ObjectReader implements Flow.Publisher<ArrowBuf> {

    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storageKey;
    private final String storagePath;
    private final String bucket;
    private final String objectKey;

    private final boolean useRange;
    private final long offset;
    private final int size;

    private final S3AsyncClient client;
    private final OrderedEventExecutor executor;
    private final BufferAllocator allocator;
    private final StorageErrors errors;

    private final AtomicBoolean subscriberSet;
    private Flow.Subscriber<? super ArrowBuf> subscriber;
    private Subscription awsSubscription;

    private final int chunkSize;
    private ArrowBuf currentChunk;

    private int requestBuffer;
    private boolean gotError;
    private boolean gotCancel;

    public S3ObjectReader(
            String storageKey, String storagePath,
            String bucket, String objectKey,
            S3AsyncClient client,
            IDataContext dataContext,
            StorageErrors errors) {

        this(storageKey, storagePath, bucket, objectKey, client, dataContext, DEFAULT_CHUNK_SIZE, errors);
    }

    public S3ObjectReader(
            String storageKey, String storagePath,
            String bucket, String objectKey,
            S3AsyncClient client,
            IDataContext dataContext,
            int chunkSize,
            StorageErrors errors) {

        this(storageKey, storagePath, bucket, objectKey, false, 0, 0,
                client, dataContext, chunkSize, errors);
    }

    public S3ObjectReader(
            String storageKey, String storagePath,
            String bucket, String objectKey,
            long offset, int size,
            S3AsyncClient client,
            IDataContext dataContext,
            int chunkSize,
            StorageErrors errors) {

        this(storageKey, storagePath, bucket, objectKey, true, offset, size,
                client, dataContext, chunkSize, errors);
    }

    public S3ObjectReader(
            String storageKey, String storagePath,
            String bucket, String objectKey,
            boolean useRange, long offset, int size,
            S3AsyncClient client,
            IDataContext dataContext,
            int chunkSize,
            StorageErrors errors) {

        this.storageKey = storageKey;
        this.storagePath = storagePath;
        this.bucket = bucket;
        this.objectKey = objectKey;

        this.useRange = useRange;
        this.offset = offset;
        this.size = size;

        this.client = client;
        this.executor = dataContext.eventLoopExecutor();
        this.allocator = dataContext.arrowAllocator();
        this.errors = errors;

        this.subscriberSet = new AtomicBoolean();
        this.subscriber = null;

        this.chunkSize = chunkSize;
        this.currentChunk = null;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ArrowBuf> subscriber) {

        var subscribeOk = subscriberSet.compareAndSet(false, true);

        if (!subscribeOk) {

            // According to Java API docs, errors in subscribe() should be reported as IllegalStateException
            // https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/Flow.Publisher.html#subscribe(java.util.concurrent.Flow.Subscriber)

            var eStorage = errors.explicitError(READ_OPERATION, storagePath, DUPLICATE_SUBSCRIPTION);
            var eFlowState = new IllegalStateException(eStorage.getMessage(), eStorage);
            subscriber.onError(eFlowState);
            return;
        }

        this.subscriber = subscriber;

        // Make sure the doStart action goes into the event loop before calling subscriber.onSubscribe()
        // This makes sure that doStart is called before any requests from the subscription get processed

        executor.submit(this::doStart);

        // Now activate the subscription, before doStart gets executed
        // This approach allows errors to be reported normally during onStart (e.g. file not found)
        // Otherwise, if the subscription is not yet active, errors should be reported with IllegalStateException
        // File not found is an expected error, reporting it with EStorage makes for cleaner error handling

        var subscription = new ClientSubscription();
        subscriber.onSubscribe(subscription);
    }

    private void doStart() {

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

    private void _onPrepare(CompletableFuture<Void> signal) {
        signal.complete(null);
    }

    private void _onResponse(GetObjectResponse response) {

        if (useRange && response.contentLength() != size) {
            var error = errors.explicitError(READ_OPERATION, storagePath, OBJECT_SIZE_TOO_SMALL);
            subscriber.onError(error);
            gotError = true;
        }

        log.info("{} {} [{}]: Object size is [{}]",
                READ_OPERATION, storageKey, storagePath,
                LoggingHelpers.formatFileSize(response.contentLength()));
    }

    private void _onStream(SdkPublisher<ByteBuffer> publisher) {
        publisher.subscribe(new ResponseStream());
    }

    private void _onSubscribe(Subscription awsSubscription) {

        if (gotError || gotCancel) {
            awsSubscription.cancel();
            return;
        }

        this.awsSubscription = awsSubscription;

        if (requestBuffer > 0) {
            awsSubscription.request(requestBuffer);
            requestBuffer = 0;
        }
    }

    private void _onNext(ByteBuffer byteBuffer) {

        if (gotError || gotCancel)
            return;

        if (!byteBuffer.hasRemaining()) {
            subscriber.onNext(allocator.getEmpty());
            return;
        }

        var sent = false;

        while (byteBuffer.remaining() > 0) {

            if (currentChunk == null)
                currentChunk = allocator.buffer(chunkSize);

            var nBytes = (int) Math.min(byteBuffer.remaining(), currentChunk.writableBytes());
            var newPosition = byteBuffer.position() + nBytes;

            currentChunk.setBytes(currentChunk.writerIndex(), byteBuffer, byteBuffer.position(), nBytes);
            currentChunk.writerIndex(currentChunk.writerIndex() + nBytes);

            byteBuffer.position(newPosition);

            if (currentChunk.writableBytes() == 0) {
                subscriber.onNext(currentChunk);
                currentChunk = null;
                sent = true;
            }
        }

        if (!sent)
            awsSubscription.request(1);

        // TODO: Assumption that buf.capacity() > size of byteBuffer
        // Otherwise we might emit more items that requested - there needs to be a queue
    }

    private void _onComplete() {

        if (gotError || gotCancel)
            return;

        if (currentChunk != null) {
            subscriber.onNext(currentChunk);
            currentChunk = null;
        }

        subscriber.onComplete();
    }

    private void _onError(Throwable error) {

        if (currentChunk != null) {
            subscriber.onNext(currentChunk);
            currentChunk = null;
        }

        var tracError = errors.handleException(READ_OPERATION, storagePath, error);

        if (gotError) {
            log.warn("{} {} [{}]: Read operation already failed, then another error occurred",
                    READ_OPERATION, storageKey, storagePath, tracError);
        }
        else if (gotCancel) {
            log.warn("{} {} [{}]: Read operation was cancelled, then an error occurred",
                    READ_OPERATION, storageKey, storagePath, tracError);
        }
        else {
            log.error("{} {} [{}]: {}",
                    READ_OPERATION, storageKey, storagePath, tracError.getMessage(), tracError);

            gotError = true;
            subscriber.onError(tracError);
        }
    }

    private class ClientSubscription implements Flow.Subscription {

        @Override
        public void request(long n) {

            if (awsSubscription != null)
                awsSubscription.request(n);
            else
                requestBuffer += n;
        }

        @Override
        public void cancel() {

            if (currentChunk != null) {
                currentChunk.close();
                currentChunk = null;
            }

            if (!gotCancel) {
                log.info("CANCEL {} {} [{}]", READ_OPERATION, storageKey, storagePath);
            }

            gotCancel = true;

            if (awsSubscription != null)
                awsSubscription.cancel();
            else {
                log.warn("{} {} [{}]: Read operation cancelled before connection was established",
                        READ_OPERATION, storageKey, storagePath);
            }
        }
    }

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
            executor.submit(() -> _onError(error));
        }
    }

    private class ResponseStream implements Subscriber<ByteBuffer> {

        @Override
        public void onSubscribe(Subscription s) {
            executor.submit(() -> _onSubscribe(s));
        }

        @Override
        public void onNext(ByteBuffer byteBuffer) {
            executor.submit(() -> _onNext(byteBuffer));
        }

        @Override
        public void onError(Throwable error) {
            executor.submit(() -> _onError(error));
        }

        @Override
        public void onComplete() {
            executor.submit(S3ObjectReader.this::_onComplete);
        }
    }

}
