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

public abstract class BufferDecoder
    extends
        BaseDataProducer<DataPipeline.ArrowApi>
    implements
        ICodec.Decoder<DataPipeline.BufferApi>,
        DataPipeline.BufferApi {

    protected BufferDecoder() {
        super(DataPipeline.ArrowApi.class);
    }

    @Override
    public DataPipeline.BufferApi dataInterface() {
        return this;
    }

    @Override
    public boolean isReady() {
        // Buffer decoders will only ever receive on chunk, so they are always ready
        return true;
    }

    // Implementations must supply pump(), because BufferDecoder is not immediate
}
