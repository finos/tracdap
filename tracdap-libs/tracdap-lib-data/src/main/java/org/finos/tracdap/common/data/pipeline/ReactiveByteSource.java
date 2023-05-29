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

package org.finos.tracdap.common.data.pipeline;

import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.exception.ETracPublic;

import org.apache.arrow.memory.ArrowBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Flow;


public class ReactiveByteSource
    extends
        BaseDataProducer<DataPipeline.StreamApi>
    implements
        DataPipeline.SourceStage,
        Flow.Subscriber<ArrowBuf> {

    private static final int BACKPRESSURE_HEADROOM = 256;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataPipelineImpl pipeline;
    private final Flow.Publisher<ArrowBuf> publisher;

    private Flow.Subscription subscription;
    private long nRequested;
    private long nReceived;
    private boolean cancelled = false;

    public ReactiveByteSource(DataPipelineImpl pipeline, Flow.Publisher<ArrowBuf> publisher) {
        super(DataPipeline.StreamApi.class);
        this.pipeline = pipeline;
        this.publisher = publisher;
    }

    @Override
    public void connect() {

        if (subscription != null) {
            log.warn("Data stream started twice");
            return;
        }

        publisher.subscribe(this);
    }

    @Override
    public void pump() {

        // data stream not active, do not pump
        if (subscription == null) {

            if (nRequested > 0 || cancelled)
                log.warn("Data stream has already closed");

            return;
        }

        if (consumerReady()) {
            var headroom = BACKPRESSURE_HEADROOM - (nRequested - nReceived);
            if (headroom > 0) {
                nRequested += headroom;
                subscription.request(headroom);
            }
        }
    }

    @Override
    public boolean isReady() {
        return consumerReady() && subscription != null;
    }

    @Override
    public void cancel() {

        cancelled = true;
        markAsDone();

        close();
    }

    @Override
    public void close() {

        if (subscription != null) {
            subscription.cancel();
            subscription = null;
        }
    }

    // The flow consumer API is called from outside the data pipeline framework
    // Any errors still unhandled at this level need to be reported to the pipeline

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        reportUnhandledErrors(() -> {

            if (log.isTraceEnabled())
                log.trace("onSubscribe()");

            this.subscription = subscription;
            consumer().onStart();

            if (consumerReady()) {
                nRequested += BACKPRESSURE_HEADROOM;
                subscription.request(BACKPRESSURE_HEADROOM);
            }

            return null;
        });
    }

    @Override
    public void onNext(ArrowBuf chunk) {

        if (cancelled) {
            chunk.close();
            return;
        }

        reportUnhandledErrors(() -> {

            if (log.isTraceEnabled())
                log.trace("onNext(), size = {}", chunk.readableBytes());

            nReceived += 1;
            consumer().onNext(chunk);

            // subscription may be null if the stage was closed early due to an error

            if (consumerReady() && subscription != null) {
                var headroom = BACKPRESSURE_HEADROOM - (nRequested - nReceived);
                if (headroom > 0) {
                    nRequested += headroom;
                    subscription.request(headroom);
                }
            }

            return null;
        });
    }

    @Override
    public void onComplete() {

        if (cancelled) {
            return;
        }

        reportUnhandledErrors(() -> {

            if (log.isTraceEnabled())
                log.trace("onComplete()");

            markAsDone();
            consumer().onComplete();

            return null;
        });
    }

    @Override
    public void onError(Throwable error) {

        if (cancelled)
            return;

        reportUnhandledErrors(() -> {

            if (log.isTraceEnabled())
                log.trace("onError()");

            markAsDone();
            consumer().onError(error);

            return null;
        });
    }

    private void reportUnhandledErrors(Callable<?> lambda) {

        try {
            lambda.call();
        }
        catch (ETracPublic regularError) {

            markAsDone();

            pipeline.reportRegularError(regularError);
            close();
        }
        catch (Throwable error) {

            log.error("An unhandled error has reached the top level: " + error.getMessage(), error);

            markAsDone();

            pipeline.reportUnhandledError(error);
            close();
        }
    }
}
