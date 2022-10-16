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

package org.finos.tracdap.common.data;


import org.finos.tracdap.common.data.pipeline.DataPipelineImpl;

import org.apache.arrow.vector.VectorSchemaRoot;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


public interface DataPipeline {


    // -----------------------------------------------------------------------------------------------------------------
    // BUILD PIPELINES
    // -----------------------------------------------------------------------------------------------------------------

    static DataPipeline forSource(SourceStage source, IDataContext ctx) {
        return DataPipelineImpl.forSource(source, ctx);
    }

    static DataPipeline forSource(Flow.Publisher<? extends ByteBuf> source, IDataContext ctx) {
        return DataPipelineImpl.forSource(source, ctx);
    }

    DataPipeline addStage(DataStage stage);
    DataPipeline addSink(SinkStage sink);
    DataPipeline addSink(Flow.Subscriber<ByteBuf> sink);


    // -----------------------------------------------------------------------------------------------------------------
    // RUN / MONITOR PIPELINES
    // -----------------------------------------------------------------------------------------------------------------

    CompletionStage<Void> execute();


    // -----------------------------------------------------------------------------------------------------------------
    // PIPELINE STAGE INTERFACES
    // -----------------------------------------------------------------------------------------------------------------

    interface PipelineStage extends AutoCloseable {}

    interface ActionStage extends PipelineStage { }
    interface DataStage extends PipelineStage {}

    interface DataProducer extends DataStage {}
    interface DataConsumer extends DataStage {}
    interface ByteProducer extends DataStage {}
    interface ByteConsumer extends DataStage {}

    interface SourceStage extends DataStage {

        void pump();
        void cancel();
    }

    interface SinkStage extends DataStage {

        void start();
        boolean poll();

        void emitComplete();
        void emitFailed(Throwable error);
    }


    interface DataStreamConsumer extends DataConsumer {

        void onStart(VectorSchemaRoot root);
        void onNext();
        void onComplete();
        void onError(Throwable error);
    }

    interface ByteStreamConsumer extends ByteConsumer {

        void onStart();
        void onNext(ByteBuf chunk);
        void onComplete();
        void onError(Throwable error);
    }

    interface ByteBufferConsumer extends ByteConsumer {

        void consumeBuffer(ByteBuf buffer);
    }

    interface DataStreamProducer extends DataProducer {

        void emitRoot(VectorSchemaRoot root);
        void emitBatch();
        void emitEnd();
        void emitFailed(Throwable error);
    }

    interface ByteStreamProducer extends ByteProducer {

         void emitStart();
         void emitChunk(ByteBuf chunk);
         void emitEnd();
         void emitFailed(Throwable error);
    }

    interface ByteBufferProducer extends ByteProducer {

        void emitBuffer(ByteBuf buffer);
    }
}
