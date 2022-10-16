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


public abstract class BaseSinkStage implements DataPipeline.SinkStage {

    private final DataPipelineImpl pipeline;

    protected BaseSinkStage(DataPipeline pipeline) {

        if (!(pipeline instanceof DataPipelineImpl))
            throw new EUnexpected();

        this.pipeline = (DataPipelineImpl) pipeline;
    }

    @Override
    public void start() {
        pipeline.feedData();
    }

    @Override
    public boolean poll() {
        return true;
    }

    @Override
    public void emitFailed(Throwable e) {
        pipeline.markAsFailed(e);
    }

    @Override
    public void emitComplete() {
        pipeline.markComplete();
    }
}
