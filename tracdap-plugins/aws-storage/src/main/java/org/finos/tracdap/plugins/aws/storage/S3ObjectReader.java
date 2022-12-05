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

import io.netty.buffer.Unpooled;
import org.finos.tracdap.common.storage.StorageErrors;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.OrderedEventExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.finos.tracdap.common.storage.IFileStorage.READ_OPERATION;
import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.DUPLICATE_SUBSCRIPTION;


public class S3ObjectReader implements Flow.Publisher<ByteBuf> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storageKey;
    private final String storagePath;
    private final String bucket;
    private final String absolutePath;

    private final S3AsyncClient client;
    private final OrderedEventExecutor executor;
    private final StorageErrors errors;

    private final AtomicBoolean subscriberSet;
    private Flow.Subscriber<? super ByteBuf> subscriber;
    private Flow.Subscription subscription;
    private Subscription awsSubscription;
    private ResponseHandler handler;

    private int requestBuffer;

    public S3ObjectReader(
            String storageKey, String storagePath, String bucket, String absolutePath,
            S3AsyncClient client, OrderedEventExecutor executor,
            StorageErrors errors) {

        this.storageKey = storageKey;
        this.storagePath = storagePath;
        this.bucket = bucket;
        this.absolutePath = absolutePath;

        this.client = client;
        this.executor = executor;
        this.errors = errors;

        this.subscriberSet = new AtomicBoolean();
        this.subscriber = null;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuf> subscriber) {

        var subscribeOk = subscriberSet.compareAndSet(false, true);

        if (!subscribeOk) {

            // According to Java API docs, errors in subscribe() should be reported as IllegalStateException
            // https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/Flow.Publisher.html#subscribe(java.util.concurrent.Flow.Subscriber)

            var eStorage = errors.explicitError(DUPLICATE_SUBSCRIPTION, storagePath, READ_OPERATION);
            var eFlowState = new IllegalStateException(eStorage.getMessage(), eStorage);
            subscriber.onError(eFlowState);
            return;
        }

        this.subscriber = subscriber;
        this.subscription = new ClientSubscription();

        // Make sure the doStart action goes into the event loop before calling subscriber.onSubscribe()
        // This makes sure that doStart is called before any requests from the subscription get processed

        executor.submit(this::doStart);

        // Now activate the subscription, before doStart gets executed
        // This approach allows errors to be reported normally during onStart (e.g. file not found)
        // Otherwise, if the subscription is not yet active, errors should be reported with IllegalStateException
        // File not found is an expected error, reporting it with EStorage makes for cleaner error handling

        subscriber.onSubscribe(subscription);
    }

    private void doStart() {

        var request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(absolutePath)
                .build();

        var handler = new ResponseHandler();

        var clientCall = client.getObject(request, handler);

        clientCall.handleAsync((stream, error) -> {

            log.info("Call future handler");



            return null;

        }, executor);


        // client.getObject();
    }

    private void _onPrepare(CompletableFuture<Void> signal) {
        log.info("Got prepare");
        signal.complete(null);
    }

    private void _onResponse(GetObjectResponse response) {
        log.info("Got response: " + response.contentLength());
    }

    private void _onStream(SdkPublisher<ByteBuffer> publisher) {
        log.info("Got stream");
        publisher.subscribe(new ResponseStream());
    }

    private void _onErrorDuringSetup(Throwable error) {
        log.info("Got exception: " + error.getMessage(), error);
        var tracError = errors.handleException(error, storagePath, READ_OPERATION);
        subscriber.onError(tracError);
    }

    private void _onSubscribe(Subscription s) {

        awsSubscription = s;

        if (requestBuffer > 0) {
            awsSubscription.request(requestBuffer);
            requestBuffer = 0;
        }
    }

    private void _onNext(ByteBuffer byteBuffer) {
        subscriber.onNext(Unpooled.wrappedBuffer(byteBuffer));
    }

    private void _onComplete() {
        log.info("Stream completed");
        subscriber.onComplete();
    }

    private void _onError(Throwable error) {
        subscriber.onError(error);
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

            if (awsSubscription != null)
                awsSubscription.cancel();
            else {

                // todo
                log.warn("Cancel before subscribe");
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
            executor.submit(() -> _onErrorDuringSetup(error));
        }
    }

    private class ResponseStream implements Subscriber<ByteBuffer> {

        @Override
        public void onSubscribe(Subscription s) {
            executor.submit(() -> _onSubscribe(s));
        }

        @Override
        public void onNext(ByteBuffer byteBuffer) {
            var x = 1;
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
