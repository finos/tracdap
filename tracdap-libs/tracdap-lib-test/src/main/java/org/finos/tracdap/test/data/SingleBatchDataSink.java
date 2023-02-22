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

package org.finos.tracdap.test.data;

import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.data.pipeline.BaseDataSink;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;


public class SingleBatchDataSink
        extends BaseDataSink <DataPipeline.ArrowApi>
        implements DataPipeline.ArrowApi {

    private VectorSchemaRoot root;

    private Schema schema;
    private long rowCount;

    public SingleBatchDataSink(DataPipeline pipeline) {

        super(pipeline);
    }

    @Override
    public DataPipeline.ArrowApi dataInterface() {
        return this;
    }

    public Schema getSchema() {
        return schema;
    }

    public long getRowCount() { return rowCount; }

    @Override
    public void connect() {
        // no-op
    }

    @Override
    public void pump() { /* no-op, immediate stage */ }

    @Override
    public boolean isReady() { return true; }

    @Override
    public void terminate(Throwable error) {

    }

    @Override
    public void close() {

    }

    @Override
    public void onStart(VectorSchemaRoot root) {
        this.root = root;
        this.schema = root.getSchema();
    }

    @Override
    public void onBatch() {
        this.rowCount += root.getRowCount();
    }

    @Override
    public void onComplete() {

        reportComplete();
    }

    @Override
    public void onError(Throwable error) {

        reportRegularError(error);
    }
}
