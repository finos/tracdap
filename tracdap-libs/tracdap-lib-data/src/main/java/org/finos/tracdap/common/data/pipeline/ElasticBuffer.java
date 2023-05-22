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

import java.util.ArrayDeque;
import java.util.Queue;


public class ElasticBuffer
    extends
        BaseDataProducer<DataPipeline.StreamApi>
    implements
        DataPipeline.DataConsumer<DataPipeline.StreamApi>,
        DataPipeline.StreamApi {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final int QUEUE_LIMIT = 1024;
    private static final int QUEUE_SAFETY_LIMIT = 512;

    private final Queue<ArrowBuf> queue;
    private boolean eos;

    public ElasticBuffer() {
        super(DataPipeline.StreamApi.class);
        this.queue = new ArrayDeque<>(QUEUE_LIMIT);
    }

    @Override
    public DataPipeline.StreamApi dataInterface() {
        return this;
    }

    @Override
    public void pump() {

        while (consumerReady() && !queue.isEmpty()) {
            var chunk = queue.remove();
            consumer().onNext(chunk);
        }

        if (consumerReady() && queue.isEmpty() && eos) {
            eos = false;
            doComplete();
        }
    }

    @Override
    public boolean isReady() {
        return queue.size() < (QUEUE_LIMIT - QUEUE_SAFETY_LIMIT) && !isDone();
    }

    @Override
    public void onStart() {

        consumer().onStart();
    }

    @Override
    public void onNext(ArrowBuf chunk) {

        if (isDone()) {
            log.warn("Data stage is already done, incoming data will be dropped");
            chunk.close();
            return;
        }

        var queued = queue.offer(chunk);

        if (!queued) {
            log.warn("Data buffer has overflowed, this data pipeline will fail");
            chunk.close();
            throw new ETracInternal("Data buffer has overflowed");
        }

        pump();
    }

    @Override
    public void onComplete() {

        if (isDone()) {
            log.warn("Data stage is already done, incoming EOS will be dropped");
            return;
        }

        eos = true;

        pump();
    }

    @Override
    public void onError(Throwable error) {

        try {
            markAsDone();
            consumer().onError(error);
        }
        finally {
            close();
        }
    }

    private void doComplete() {

        try {
            markAsDone();
            consumer().onComplete();
        }
        finally {
            close();
        }
    }

    @Override
    public void close() {

        while (!queue.isEmpty()) {
            var chunk = queue.remove();
            chunk.close();
        }
    }
}
