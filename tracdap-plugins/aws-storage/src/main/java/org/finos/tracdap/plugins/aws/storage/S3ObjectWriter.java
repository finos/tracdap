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
import org.finos.tracdap.common.storage.StorageErrors;
import org.finos.tracdap.common.util.LoggingHelpers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.OrderedEventExecutor;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.finos.tracdap.common.storage.CommonFileStorage.WRITE_OPERATION;
import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.DUPLICATE_SUBSCRIPTION;


public class S3ObjectWriter implements Flow.Subscriber<ByteBuf> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storageKey;
    private final String storagePath;
    private final String bucket;
    private final String objectKey;

    private final S3AsyncClient client;
    private final CompletableFuture<Long> signal;
    private final IDataContext dataContext;
    private final OrderedEventExecutor executor;
    private final StorageErrors errors;

    private final AtomicBoolean subscriptionSet;
    private Flow.Subscription subscription;

    private final CompositeByteBuf buffer = Unpooled.compositeBuffer();

    public S3ObjectWriter(
            String storageKey, String storagePath,
            String bucket, String objectKey,
            S3AsyncClient client,
            CompletableFuture<Long> signal,
            IDataContext dataContext,
            StorageErrors errors) {

        this.storageKey = storageKey;
        this.storagePath = storagePath;
        this.bucket = bucket;
        this.objectKey = objectKey;

        this.client = client;
        this.signal = signal;
        this.dataContext = dataContext;
        this.executor = dataContext.eventLoopExecutor();
        this.errors = errors;

        this.subscriptionSet = new AtomicBoolean();
        this.subscription = null;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        var subscribeOk = subscriptionSet.compareAndSet(false, true);

        if (!subscribeOk) {

            var eStorage = errors.explicitError(WRITE_OPERATION, storagePath, DUPLICATE_SUBSCRIPTION);
            throw new IllegalStateException(eStorage.getMessage(), eStorage);
        }

        this.subscription = subscription;

        executor.submit(() -> this.subscription.request(128));
    }

    @Override
    public void onNext(ByteBuf item) {

        buffer.addComponent(true, item);
        this.subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {

        try {
            var tracError = errors.handleException(WRITE_OPERATION, storagePath, throwable);

            log.error("{} {} [{}]: {}", WRITE_OPERATION, storageKey, storagePath, tracError.getMessage(), tracError);

            signal.completeExceptionally(throwable);
        }
        finally {
            buffer.release();
        }
    }

    @Override
    public void onComplete() {

        var content = AsyncRequestBody.fromByteBuffer(buffer.nioBuffer());
        var contentLength = (long) buffer.readableBytes();

        var request = PutObjectRequest.builder()
                .bucket(this.bucket)
                .key(objectKey)
                .contentLength(contentLength)
                .build();

        var response = dataContext.toContext(client.putObject(request, content));

        response.handle(this::onCompleteHandler);
    }

    private CompletionStage<Void> onCompleteHandler(PutObjectResponse result, Throwable error) {

        try {

            if (error != null) {

                var tracError = errors.handleException(WRITE_OPERATION, storagePath, error);

                log.error("{} {} [{}]: {}", WRITE_OPERATION, storageKey, storagePath, tracError.getMessage(), tracError);

                var mappedError = errors.handleException(WRITE_OPERATION, storagePath, error);
                signal.completeExceptionally(mappedError);
            }
            else {

                var contentLength = (long) buffer.readableBytes();

                log.info("{} {} [{}]: Write operation complete, object size is {}",
                        WRITE_OPERATION, storageKey, storagePath, LoggingHelpers.formatFileSize(contentLength));

                signal.complete(contentLength);
            }

            return CompletableFuture.completedFuture(null);
        }
        finally {
            buffer.release();
        }
    }
}
