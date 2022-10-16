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

import io.netty.buffer.ByteBuf;

import java.util.concurrent.Flow;


public class ReactiveByteSource
        extends BaseByteProducer
        implements DataPipeline.SourceStage,
        Flow.Subscriber<ByteBuf> {

    private final DataPipelineImpl pipeline;
    private final Flow.Publisher<? extends ByteBuf> publisher;

    private Flow.Subscription subscription;
    private boolean started = false;
    private long nRequested;
    private long nReceived;

    public ReactiveByteSource(DataPipelineImpl pipeline, Flow.Publisher<? extends ByteBuf> publisher) {
        this.pipeline = pipeline;
        this.publisher = publisher;
    }

    @Override
    public void pump() {

        if (!started) {
            this.started = true;
            emitStart();
        }

        if (subscription != null) {

            if (nReceived == nRequested) {
                nRequested += 1;
                subscription.request(1);
            }
        }
        else {
            // todo log
        }
    }

    @Override
    public void cancel() {

        if (subscription != null) {
            subscription.cancel();
            subscription = null;
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        this.subscription = subscription;
    }

    @Override
    public void onNext(ByteBuf chunk) {

        try {

            nReceived += 1;

            emitChunk(chunk);
        }
        catch (Throwable e) {
            pipeline.markAsFailed(e);
        }
    }

    @Override
    public void onComplete() {

        try {
            emitEnd();
        }
        catch (Throwable e) {
            pipeline.markAsFailed(e);
        }
    }

    @Override
    public void onError(Throwable error) {

        try {
            emitFailed(error);
        }
        finally {
            pipeline.markAsFailed(error);
        }
    }

    @Override
    public void close() {
        cancel();
    }
}
