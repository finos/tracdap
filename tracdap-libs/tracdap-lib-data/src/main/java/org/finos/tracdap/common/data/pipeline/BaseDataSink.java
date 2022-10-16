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


public abstract class BaseDataSink <API_T extends DataPipeline.DataInterface<API_T>>
    extends
        BaseDataStage
    implements
        DataPipeline.DataConsumer<API_T>,
        DataPipeline.SinkStage {

    protected final DataPipelineImpl pipeline;

    protected BaseDataSink(DataPipeline pipeline) {

        if (!(pipeline instanceof DataPipelineImpl))
            throw new EUnexpected();

        this.pipeline = (DataPipelineImpl) pipeline;
    }

    protected final void reportComplete() {
        pipeline.reportComplete();
    }

    protected final void reportRegularError(Throwable error) {
        pipeline.reportRegularError(error);
    }

    protected final void reportUnhandledError(Throwable error) {
        pipeline.reportUnhandledError(error);
    }
}
