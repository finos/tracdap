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
import org.finos.tracdap.common.exception.ETracPublic;
import org.finos.tracdap.common.exception.EUnexpected;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;


public class DataPipelineImpl implements DataPipeline {

    private static final Logger log = LoggerFactory.getLogger(DataPipeline.class);

    private final IDataContext ctx;
    private final List<DataStage> stages;
    private final AtomicBoolean pumpScheduled;
    private final CompletableFuture<Void> completion;

    private SourceStage sourceStage;
    private SinkStage sinkStage;
    private boolean started;

    public DataPipelineImpl(IDataContext ctx) {

        this.ctx = ctx;
        this.stages = new ArrayList<>();
        this.pumpScheduled = new AtomicBoolean(false);
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
        sourceStage.connect();
        sinkStage.connect();

        // Schedule running the data pump on the pipeline's event loop
        pumpScheduled.set(true);
        ctx.eventLoopExecutor().execute(this::runDataPump);

        return completion;
    }

    void pumpData() {

        // Schedule running the data pump on the pipeline's event loop

        var notAlreadyScheduled = pumpScheduled.compareAndSet(false, true);

        if (notAlreadyScheduled)
            ctx.eventLoopExecutor().execute(this::runDataPump);
    }

    private void runDataPump() {

        // The data pump makes one pass over all the stages in the pipeline, starting at the back
        // Stages are pumped if their consumer is ready to accept data

        // The feedback is that stages can request a pump if they have entered a ready state
        // This happens by a stage calling pumpData(), to schedule another pump on the event loop

        // Source and sink stages can be triggered externally, depending on their transport mechanism
        // These triggers must come in on the same event loop as the data pump is not thread safe
        // Currently all processors for a single request run on the same event loop, which keeps things simple
        // When we look at data back-ends with blocking APIs, we may need to allocate a worker thread to a pipeline

        // Any errors that propagate are picked up using reportUnhandledError (the same as the source/sink stages)
        // Since this is a top level call on teh event loop, it is important to make sure no errors get lost

        try {

            pumpScheduled.set(false);

            if (completion.isDone())
                return;

            var consumerReady = true;

            for (var i = stages.size() - 1; i >= 0; i--) {

                var stage = stages.get(i);

                // If a stage is complete, all preceding stages must also be complete
                if (stage.isDone())
                    return;

                if (consumerReady)
                    stage.pump();

                consumerReady = stage.isReady();
            }
        }
        catch (Throwable error) {
            reportUnhandledError(error);
        }
    }

    void requestCancel() {

        log.warn("Request to cancel the data operation");
        var error = new ETracPublic("Request to cancel the data operation");

        completion.completeExceptionally(error);

        if (sourceStage.isDone()) {
            sourceStage.cancel();
        }

        if (!sinkStage.isDone()) {
            sinkStage.terminate(error);
        }

        closeAllStages();
    }

    void reportComplete() {

        // Expect all the stages have gone down cleanly

        log.info("Data pipeline complete");
        completion.complete(null);

        // This is a failsafe check, normally source and sink should be already stopped when close() is called
        // But just in case, double check and shut them down with a warning if they are still running

        if (!sourceStage.isDone()) {
            log.warn("Source stage still running after data pipeline completed");
            sourceStage.cancel();
        }

        if (!sinkStage.isDone()) {
            log.warn("Sink stage still running after data pipeline completed");
            var error = new ETracInternal("Sink stage still running after data pipeline completed\"");
            sinkStage.terminate(error);
        }

        closeAllStages();
    }

    void reportRegularError(Throwable error) {

        // Expect all the stages have gone down cleanly

        log.error("Data pipeline failed: {}", error.getMessage(), error);
        completion.completeExceptionally(error);

        sourceStage.cancel();
        sinkStage.terminate(error);

        closeAllStages();
    }

    void reportUnhandledError(Throwable error) {

        // Expect the stages are in an inconsistent state
        // Force shutdown for the source and sink

        var unhandled = new ETracInternal("Data pipeline failed with an unhandled error: " + error.getMessage(), error);
        log.error(unhandled.getMessage(), unhandled);
        completion.completeExceptionally(unhandled);

        sourceStage.cancel();
        sinkStage.terminate(unhandled);

        closeAllStages();
    }

    private void closeAllStages() {

        // This method is internal, to trigger a shutdown externally use requestCancel(), or report compete / error.

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

        log.info("DATA PIPELINE: New for source type [{}]", source.getClass().getSimpleName());

        var pipeline = new DataPipelineImpl(ctx);

        pipeline.stages.add(source);
        pipeline.sourceStage = source;

        return pipeline;
    }

    public static DataPipeline forSource(Flow.Publisher<? extends ByteBuf> source, IDataContext ctx) {

        log.info("DATA PIPELINE: New for stream type [{}]", source.getClass().getSimpleName());

        var pipeline = new DataPipelineImpl(ctx);

        var rbs = new ReactiveByteSource(pipeline, source);

        pipeline.stages.add(rbs);
        pipeline.sourceStage = rbs;

        return pipeline;
    }

    public DataPipeline addStage(DataStage stage) {

        if (sourceStage == null || sinkStage != null)
            throw new ETracInternal("Data pipeline must have one source and one sink");

        if (started)
            throw new ETracInternal("Data pipeline is already started");

        if (stage instanceof DataConsumer<?>) {
            doAddStage((DataConsumer<?>) stage);
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

        var sinkBuffer = new ElasticBuffer();
        addStage(sinkBuffer);

        var reactiveSink = new ReactiveByteSink(this, sink);
        return addSink(reactiveSink);
    }

    private void doAddStage(DataConsumer<?> stage) {

        var priorStage = (DataProducer<?>) stages.get(stages.size() - 1);

        if (priorStage.consumerType().isAssignableFrom(stage.dataInterface().getClass())) {
            connectStage(priorStage, stage);
        }

        else if (StreamApi.class.isAssignableFrom(priorStage.consumerType()) &&
                 BufferApi.class.isAssignableFrom(stage.dataInterface().getClass())) {

            var buffering = new BufferingStage();
            connectStage(priorStage, buffering);
            connectStage(buffering, stage);
        }

        else {

            throw new EUnexpected();
        }
    }

    private void connectStage(DataProducer<?> producer, DataConsumer<?> consumer) {

        log.info("DATA PIPELINE: Add stage {}", consumer.getClass().getSimpleName());

        var concreteProducer = (BaseDataProducer<?>) producer;

        if (concreteProducer == null)
            throw new EUnexpected();

        concreteProducer.bind(consumer);

        stages.add(consumer);
    }
}
