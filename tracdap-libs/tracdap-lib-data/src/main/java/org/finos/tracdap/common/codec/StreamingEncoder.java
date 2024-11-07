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

package org.finos.tracdap.common.codec;

import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.data.pipeline.BaseDataProducer;

public abstract class StreamingEncoder
    extends
        BaseDataProducer<DataPipeline.StreamApi>
    implements
        ICodec.Encoder<DataPipeline.StreamApi>,
        DataPipeline.ArrowApi {

    protected StreamingEncoder() {
        super(DataPipeline.StreamApi.class);
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
        /* no-op, immediate stage */
    }
}
