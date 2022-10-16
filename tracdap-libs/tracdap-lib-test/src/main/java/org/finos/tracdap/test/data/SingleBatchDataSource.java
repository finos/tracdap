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

import org.apache.arrow.vector.VectorSchemaRoot;
import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.data.pipeline.BaseDataProducer;


public class SingleBatchDataSource extends BaseDataProducer implements DataPipeline.SourceStage {

    private final VectorSchemaRoot root;

    public SingleBatchDataSource(VectorSchemaRoot root) {
        this.root = root;
    }

    @Override
    public void pump() {

        emitRoot(root);
        emitBatch();
        emitEnd();
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() {

    }
}
