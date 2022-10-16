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

import org.apache.arrow.vector.VectorSchemaRoot;


public abstract class BaseDataProducer implements DataPipeline.DataStreamProducer {

    private DataPipeline.DataStreamConsumer consumer;

    final void bind(DataPipeline.DataStreamConsumer consumer) {

        if (this.consumer != null)
            throw new EUnexpected();  // todo err

        this.consumer = consumer;
    }

    public final void emitRoot(VectorSchemaRoot root) {

        //log.info("emitRoot");

        if (this.consumer == null)
            throw new EUnexpected();

        consumer.onStart(root);
    }

    public final void emitBatch() {

        //log.info("emitBatch");

        if (this.consumer == null)
            throw new EUnexpected();

        consumer.onNext();
    }

    public final void emitEnd() {

        //log.info("emitEnd");

        if (this.consumer == null)
            throw new EUnexpected();

        consumer.onComplete();
    }

    public final void emitFailed(Throwable error) {

        //log.info("emitFailed");

        if (this.consumer == null)
            throw new EUnexpected();

        consumer.onError(error);
    }
}
