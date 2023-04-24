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

import org.finos.tracdap.common.storage.StorageErrors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.OrderedEventExecutor;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.finos.tracdap.common.storage.CommonFileStorage.WRITE_OPERATION;
import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.DUPLICATE_SUBSCRIPTION;


public class S3ObjectWriter implements Flow.Subscriber<ByteBuf> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storageKey;
    private final String storagePath;
    private final String bucket;
    private final String absolutePath;

    private final S3AsyncClient client;
    private final CompletableFuture<Long> signal;
    private final OrderedEventExecutor executor;
    private final StorageErrors errors;

    private final AtomicBoolean subscriptionSet;
    private Flow.Subscription subscription;

    private CompositeByteBuf buffer = Unpooled.compositeBuffer();

    public S3ObjectWriter(
            String storageKey, String storagePath, String bucket, String absolutePath,
            S3AsyncClient client, CompletableFuture<Long> signal, OrderedEventExecutor executor,
            StorageErrors errors) {

        this.storageKey = storageKey;
        this.storagePath = storagePath;
        this.bucket = bucket;
        this.absolutePath = absolutePath;

        this.client = client;
        this.signal = signal;
        this.executor = executor;
        this.errors = errors;

        this.subscriptionSet = new AtomicBoolean();
        this.subscription = null;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        var subscribeOk = subscriptionSet.compareAndSet(false, true);

        if (!subscribeOk) {

            var eStorage = errors.explicitError(DUPLICATE_SUBSCRIPTION, storagePath, WRITE_OPERATION);
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

        buffer.release();
        signal.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {

        var content = AsyncRequestBody.fromByteBuffer(buffer.nioBuffer());
        var contentLength = (long) buffer.readableBytes();

        var request = PutObjectRequest.builder()
                .bucket(this.bucket)
                .key(absolutePath)
                .contentLength(contentLength)
                .build();

        client.putObject(request, content).handleAsync((response, error) -> {

            try {

                if (error != null) {

                    log.error("Write operation failed: {} [{}]", error.getMessage(), absolutePath, error);

                    var mappedError = errors.handleException(error, storagePath, WRITE_OPERATION);
                    signal.completeExceptionally(mappedError);
                }
                else {

                    log.info("Write operation complete: {} bytes written [{}]", contentLength, absolutePath);

                    signal.complete(contentLength);
                }

                return null;
            }
            finally {
                buffer.release();
            }

        }, executor);
    }
}
