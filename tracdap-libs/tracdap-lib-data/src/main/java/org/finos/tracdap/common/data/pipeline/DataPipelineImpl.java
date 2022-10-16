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
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


public class DataPipelineImpl implements DataPipeline {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IDataContext ctx;
    private final List<DataStage> stages;
    private final CompletableFuture<Void> completion;

    private SourceStage sourceStage;
    private SinkStage sinkStage;
    private boolean started;

    public DataPipelineImpl(IDataContext ctx) {

        this.ctx = ctx;
        this.stages = new ArrayList<>();
        this.completion = new CompletableFuture<>();
        this.started = false;
    }

    public CompletionStage<Void> execute() {

        if (sourceStage == null || sinkStage == null)
            throw new ETracInternal("Data pipeline must have one source and one sink");

        if (started)
            throw new ETracInternal("Data pipeline is already started");

        log.info("Executing data pipeline");

        started = true;
        ctx.eventLoopExecutor().execute(this::start);

        return completion;
    }

    void start() {

        sinkStage.start();
        feedData();
    }

    void feedData() {

        if (completion.isDone())
            return;

        try {
            if (sinkStage.poll())
                sourceStage.pump();
        }
        finally {
            ctx.eventLoopExecutor().execute(this::feedData);
        }
    }

    void cancel() {

        if (sourceStage != null)
            sourceStage.cancel();
    }

    void markComplete() {

        log.info("Data pipeline complete");

        completion.complete(null);

        close();
    }

    void markAsFailed(Throwable error) {

        try {

            log.info("Data pipeline failed", error);

            var newStatus = completion.completeExceptionally(error);

            if (newStatus) {
                sourceStage.cancel();
                sinkStage.emitFailed(error);
            }
        }
        finally {

            close();
        }
    }

    void close() {

        for (var stage : stages) {
            try {
                stage.close();
            }
            catch (Throwable e) {
                log.warn("There was an error while the data pipeline was shutting down: {}", e.getMessage());
            }
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // PIPELINE ASSEMBLY
    // -----------------------------------------------------------------------------------------------------------------


    public static DataPipeline forSource(SourceStage source, IDataContext ctx) {

        var pipeline = new DataPipelineImpl(ctx);

        pipeline.stages.add(source);
        pipeline.sourceStage = source;

        return pipeline;
    }

    public static DataPipeline forSource(Flow.Publisher<? extends ByteBuf> source, IDataContext ctx) {

        var pipeline = new DataPipelineImpl(ctx);

        var rbs = new ReactiveByteSource(pipeline, source);
        source.subscribe(rbs);

        pipeline.stages.add(rbs);
        pipeline.sourceStage = rbs;

        return pipeline;
    }

    public DataPipeline addStage(DataStage stage) {

        if (sourceStage == null || sinkStage != null)
            throw new ETracInternal("Data pipeline must have one source and one sink");

        if (started)
            throw new ETracInternal("Data pipeline is already started");

        if (stage instanceof DataStreamConsumer) {
            addDataConsumer((DataStreamConsumer) stage);
            return this;
        }

        if (stage instanceof ByteStreamConsumer) {
            addByteConsumer((ByteStreamConsumer) stage);
            return this;
        }

        if (stage instanceof ByteBufferConsumer) {
            addByteConsumer((ByteBufferConsumer) stage);
            return this;
        }

        throw new ETracInternal("Data pipeline cannot add unsupported stage type " + stage.getClass().getName());
    }

    public DataPipeline addSink(SinkStage sink) {

        if (sourceStage == null || sinkStage != null)
            throw new ETracInternal("Data pipeline must have one source and one sink");

        if (started)
            throw new ETracInternal("Data pipeline is already started");

        addStage(sink);
        sinkStage = sink;

        return this;

    }

    public DataPipeline addSink(Flow.Subscriber<ByteBuf> sink) {

        var reactiveSink = new ReactiveByteSink(this, sink);
        return addSink(reactiveSink);
    }

    private void addDataConsumer(DataStreamConsumer stage) {

        var priorStage = stages.get(stages.size() - 1);

        if (priorStage instanceof BaseDataProducer) {

            ((BaseDataProducer) priorStage).bind(stage);
            stages.add(stage);
        }
        else {

            throw new EUnexpected();
        }
    }

    private void addByteConsumer(ByteStreamConsumer stage) {

        var priorStage = stages.get(stages.size() - 1);

        if (priorStage instanceof BaseByteProducer) {

            ((BaseByteProducer) priorStage).bind(stage);
            stages.add(stage);
        }
        else if (priorStage instanceof BaseBufferProducer) {

            // todo
            throw new EUnexpected();
        }
        else {

            throw new EUnexpected();
        }
    }

    private void addByteConsumer(ByteBufferConsumer stage) {

        var priorStage = stages.get(stages.size() - 1);

        if (priorStage instanceof BaseBufferProducer) {

            ((BaseBufferProducer) priorStage).bind(stage);
        }
        else if (priorStage instanceof BaseByteProducer) {

            var buffering = new BufferingStage();
            ((BaseByteProducer) priorStage).bind(buffering);
            buffering.bind(stage);

            stages.add(buffering);
            stages.add(stage);
        }
        else {

            throw new EUnexpected();
        }
    }
}
