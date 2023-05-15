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
import org.finos.tracdap.common.exception.ETracInternal;

import org.apache.arrow.memory.ArrowBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Flow;


public class ReactiveByteSink
    extends
        BaseDataSink<DataPipeline.StreamApi>
    implements
        DataPipeline.StreamApi {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Flow.Subscriber<ArrowBuf> sink;
    private Flow.Subscription subscription;
    private int chunksRequested;
    private int chunksDelivered;

    ReactiveByteSink(DataPipelineImpl pipeline, Flow.Subscriber<ArrowBuf> sink) {

        super(pipeline);

        this.sink = sink;
        this.chunksRequested = 0;
        this.chunksDelivered = 0;
    }

    @Override
    public DataPipeline.StreamApi dataInterface() {
        return this;
    }

    @Override
    public void connect() {

        if (subscription != null) {
            log.warn("Data stream started twice");
            return;
        }

        subscription = new Subscription();
        sink.onSubscribe(subscription);
    }

    private void doRequest(long n) {

        chunksRequested += n;
        pipeline.pumpData();
    }

    public void doCancel() {

        pipeline.requestCancel();
        close();
    }

    @Override
    public boolean isReady() {
        return chunksRequested > chunksDelivered && subscription != null;
    }

    @Override
    public void pump() {

        if (isReady())
            pipeline.pumpData();
    }

    @Override
    public void terminate(Throwable error) {

        if (isDone()) {
            log.warn("Requested termination, but stage is already down");
            return;
        }

        markAsDone();
        sink.onError(error);
    }

    @Override
    public void close() {

        // no-op
    }

    @Override
    public void onStart() {

        // No-op, already subscribed
    }

    @Override
    public void onNext(ArrowBuf chunk) {

        if (chunksRequested <= chunksDelivered) {
            var err = new ETracInternal("Data stream is out of sync");
            log.error(err.getMessage(), err);
            throw err;
        }

        chunksDelivered += 1;
        sink.onNext(chunk);
    }

    @Override
    public void onComplete() {

        markAsDone();
        sink.onComplete();

        reportComplete();
    }

    @Override
    public void onError(Throwable error) {

        markAsDone();
        sink.onError(error);

        reportRegularError(error);
    }

    private class Subscription implements Flow.Subscription {

        @Override
        public void request(long n) {
            doRequest(n);
        }

        @Override
        public void cancel() {
            doCancel();
        }
    }
}
