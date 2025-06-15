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

package org.finos.tracdap.test.data;

import org.finos.tracdap.common.data.ArrowVsrContext;
import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.data.pipeline.BaseDataProducer;


public class SingleBatchDataSource
        extends BaseDataProducer<DataPipeline.ArrowApi>
        implements DataPipeline.SourceStage {

    private final ArrowVsrContext root;

    public SingleBatchDataSource(ArrowVsrContext root) {
        super(DataPipeline.ArrowApi.class);
        this.root = root;
    }

    @Override
    public void connect() {
        // no-op
    }

    @Override
    public void pump() {

        consumer().onStart(root);
        consumer().onBatch();
        markAsDone();
        consumer().onComplete();
    }

    @Override public boolean isReady() {
        return true;
    }

    @Override
    public void cancel() {
        markAsDone();
    }

    @Override
    public void close() {

    }
}
