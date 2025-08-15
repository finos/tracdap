/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
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

import org.finos.tracdap.common.data.ArrowVsrContext;
import org.finos.tracdap.common.data.DataPipeline;


public class CounterStage
        extends BaseDataProducer<DataPipeline.ArrowApi>
        implements
        DataPipeline.ArrowApi,
        DataPipeline.DataConsumer<DataPipeline.ArrowApi>,
        DataPipeline.DataProducer<DataPipeline.ArrowApi> {

    private ArrowVsrContext batch;
    private long batchCount;
    private long rowCount;

    public CounterStage() {
        super(DataPipeline.ArrowApi.class);
    }

    @Override
    public void onStart(ArrowVsrContext batch) {
        this.batch = batch;
        consumer().onStart(batch);
    }

    public long getBatchCount() {
        return batchCount;
    }

    public long getRowCount() {
        return rowCount;
    }

    @Override
    public void onBatch() {
        batchCount++;
        rowCount += batch.getVsr().getRowCount();
        consumer().onBatch();
    }

    @Override
    public void onComplete() {
        consumer().onComplete();
    }

    @Override
    public void onError(Throwable error) {
        consumer().onError(error);
    }

    @Override
    public DataPipeline.ArrowApi dataInterface() {
        return this;
    }

    @Override
    public boolean isReady() {
        return consumerReady();
    }

    @Override
    public void pump() {
        // No-op
    }

    @Override
    public void close() throws Exception {
        // No-op
    }
}
