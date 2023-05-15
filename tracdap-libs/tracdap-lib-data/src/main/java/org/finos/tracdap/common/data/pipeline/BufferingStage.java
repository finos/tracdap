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
import org.finos.tracdap.common.exception.EUnexpected;

import org.apache.arrow.memory.ArrowBuf;

import java.util.ArrayList;
import java.util.List;


public class BufferingStage
    extends
        BaseDataProducer<DataPipeline.BufferApi>
    implements
        DataPipeline.DataConsumer<DataPipeline.StreamApi>,
        DataPipeline.StreamApi {

    private List<ArrowBuf> buffer;

    public BufferingStage() {
        super(DataPipeline.BufferApi.class);
        buffer = null;
    }

    @Override
    public DataPipeline.StreamApi dataInterface() {
        return this;
    }

    @Override
    public void pump() {
        // no-op
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void onStart() {

        if (buffer != null)
            throw new EUnexpected();

        buffer = new ArrayList<>();
    }

    @Override
    public void onNext(ArrowBuf chunk) {

        buffer.add(chunk);
    }

    @Override
    public void onComplete() {

        try {

            var relinquishBuffer = this.buffer;
            this.buffer = null;

            markAsDone();
            consumer().onBuffer(relinquishBuffer);
        }
        finally {
            close();
        }
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

    @Override
    public void close() {

        if (buffer != null) {
            buffer.forEach(ArrowBuf::close);
            buffer = null;
        }
    }
}
