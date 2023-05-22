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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


/**
 * <p>The previous data framework used Java Flow publisher / subscriber classes to stream data.
 * However, this approach had limitations:</p>
 *
 * <ul>
 *
 * <li>In Arrow / Java, VSR is.a single container that data flows through, not a stream of objects.
 * Data that is loaded must be unloaded before it can be re-used,
 * which is not guaranteed in the loosely coupled Flow interface.</li>
 *
 * <li>Sources and sinks have several different interfaces and mapping them to publishers / subscribers
 * is not always beneficial. Channels, blocking streams and memory buffers are all needed to support
 * different client libraries. While any of these can be converted to Flow interfaces, the mapping
 * is not always natural, which restricts flow control and increases data copying.</li>
 *
 * </ul>
 *
 * <p>The data pipeline framework is an attempt to address these issues.
 * It allows stages to be strung together in a pipeline and connected to different sources / sinks.
 * The pipeline will join stages together and choose an execution context for the pipeline.
 * For example, a pipeline where all event-driven stages can execute on the event loop,
 * while a pipeline that reads from a blocking input stream would be assigned a worker thread.
 * Sometimes the pipeline can plug together stages with different patterns (e.g. block input to streaming output),
 * where this is not possible errors will be raised when the pipeline is built.</p>
 */
public interface DataPipeline {



    // -----------------------------------------------------------------------------------------------------------------
    // BUILD PIPELINES
    // -----------------------------------------------------------------------------------------------------------------


    static DataPipeline forSource(SourceStage source, IDataContext ctx) {
        return DataPipelineImpl.forSource(source, ctx);
    }

    static DataPipeline forSource(Flow.Publisher<ArrowBuf> source, IDataContext ctx) {
        return DataPipelineImpl.forSource(source, ctx);
    }

    DataPipeline addStage(DataStage stage);
    DataPipeline addSink(SinkStage sink);
    DataPipeline addSink(Flow.Subscriber<ArrowBuf> sink);



    // -----------------------------------------------------------------------------------------------------------------
    // RUN / MONITOR PIPELINES
    // -----------------------------------------------------------------------------------------------------------------


    CompletionStage<Void> execute();



    // -----------------------------------------------------------------------------------------------------------------
    // PIPELINE STAGE INTERFACES
    // -----------------------------------------------------------------------------------------------------------------


    interface DataInterface <API_T>  {

        API_T dataInterface();
    }

    interface ArrowApi extends DataInterface<ArrowApi> {

        void onStart(VectorSchemaRoot root);
        void onBatch();
        void onComplete();
        void onError(Throwable error);
    }

    interface StreamApi extends DataInterface<StreamApi> {

        void onStart();
        void onNext(ArrowBuf chunk);
        void onComplete();
        void onError(Throwable error);
    }

    interface BufferApi extends DataInterface<BufferApi> {

        void onBuffer(List<ArrowBuf> buffer);
        void onError(Throwable error);
    }


    interface DataStage extends AutoCloseable {

        boolean isDone();
        boolean isReady();
        void pump();
    }

    interface SourceStage extends DataStage {

        void connect();
        void cancel();
    }

    interface SinkStage extends DataStage {

        void connect();
        void terminate(Throwable error);
    }

    interface DataConsumer <API_T> extends DataStage, DataInterface<API_T> {

        API_T dataInterface();
    }

    interface DataProducer<API_T extends DataInterface<API_T>> extends DataStage {

        Class<API_T> consumerType();
        boolean consumerReady();
        API_T consumer();
    }
}
