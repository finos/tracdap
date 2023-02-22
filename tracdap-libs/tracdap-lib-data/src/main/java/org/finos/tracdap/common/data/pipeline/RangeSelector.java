/*
 * Copyright 2023 Accenture Global Solutions Limited
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

import org.apache.arrow.vector.VectorSchemaRoot;
import org.finos.tracdap.common.data.DataPipeline;

import java.util.ArrayList;
import java.util.List;


public class RangeSelector
        extends
        BaseDataProducer<DataPipeline.ArrowApi>
        implements
        DataPipeline.ArrowApi,
        DataPipeline.DataConsumer<DataPipeline.ArrowApi>,
        DataPipeline.DataProducer<DataPipeline.ArrowApi> {

    private final long offset;
    private final long limit;
    private long currentRow;

    private VectorSchemaRoot root;

    public RangeSelector(long offset, long limit) {
        super(DataPipeline.ArrowApi.class);

        this.offset = offset;
        this.limit = limit;
        this.currentRow = 0;

        this.root = new VectorSchemaRoot(List.of());
    }

    @Override
    public boolean isReady() {
        return consumerReady();
    }

    @Override
    public void pump() {

    }

    @Override
    public DataPipeline.ArrowApi dataInterface() {
        return this;
    }

    @Override
    public void close() {

    }

    @Override
    public void onStart(VectorSchemaRoot root) {

        var vectors = new ArrayList<>();

        for (var vector : root.getFieldVectors()) {

            vector.
        }

        this.root = new VectorSchemaRoot()

        consumer().onStart(root);
    }

    @Override
    public void onBatch() {

        var batchSize = root.getRowCount();
        var batchStartRow = currentRow;
        var batchEndRow = currentRow + batchSize;

        if (batchStartRow >= offset && (batchEndRow < offset + limit || limit == 0)) {
            consumer().onBatch();
        }
        else if (batchEndRow >= offset && (batchStartRow < offset + limit || limit == 0)) {

            var sliceStart = (int) (offset - batchStartRow);
            var sliceEnd = (int) Math.min(offset + limit - batchStartRow, root.getRowCount());
            var slice = root.slice(sliceStart, sliceEnd);

            root.getVector(0).

            consumer().onBatch();
        }

        currentRow += batchSize;

        // TODO: if (batchStartRow >= offset + limit && limit != 0) {
        //      pipeline.cancel()
        // }
    }

    @Override
    public void onComplete() {
        consumer().onComplete();
    }

    @Override
    public void onError(Throwable error) {
        consumer().onError(error);
    }
}
