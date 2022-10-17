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


public abstract class BaseDataProducer
        <API_T extends DataPipeline.DataInterface<API_T>>
        extends BaseDataStage
        implements DataPipeline.DataProducer<API_T> {

    private final Class<API_T> consumerType;
    private DataPipeline.DataConsumer<API_T> consumer;

    protected BaseDataProducer(Class<API_T> consumerType) {
        this.consumerType = consumerType;
    }

    @SuppressWarnings("unchecked")
    final void bind(DataPipeline.DataConsumer<?> consumer) {

        if (!consumerType.isAssignableFrom(consumer.dataInterface().getClass()))
            throw new EUnexpected();

        this.consumer = (DataPipeline.DataConsumer<API_T>) consumer;
    }

    @Override
    public Class<API_T> consumerType() {
        return consumerType;
    }

    @Override
    public final boolean consumerReady() {
        return consumer.isReady();
    }

    @Override
    public final API_T consumer() {
        return consumer.dataInterface();
    }
}
