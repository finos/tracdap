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

package org.finos.tracdap.svc.orch.service;

import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.test.meta.SampleMetadata;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Snapshot reads in checkJob against a real gRPC call, so the read deadline applies (Mockito stubs ignore it)
 */
public class JobProgressDeadlineTest {

    private static final String PROGRESS_PATH = "path/to/trac_job_progress.json";

    private StallingStorage storage;
    private ExecutorService serverExecutor;
    private Server server;
    private ManagedChannel channel;

    private JobProcessor processor;
    private TestClock clock;
    private TagHeader jobId;

    @BeforeEach
    void setup() throws Exception {

        var serverName = InProcessServerBuilder.generateName();

        storage = new StallingStorage();
        serverExecutor = Executors.newCachedThreadPool();
        server = InProcessServerBuilder.forName(serverName).executor(serverExecutor).addService(storage).build().start();
        channel = InProcessChannelBuilder.forName(serverName).build();

        var configManager = Mockito.mock(ConfigManager.class);
        Mockito.when(configManager.loadRootConfigObject(PlatformConfig.class)).thenReturn(PlatformConfig.getDefaultInstance());

        var registry = new PluginRegistry();
        registry.addSingleton(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub.class, Mockito.mock(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub.class));
        registry.addSingleton(TracStorageApiGrpc.TracStorageApiBlockingStub.class, TracStorageApiGrpc.newBlockingStub(channel));
        registry.addSingleton(JobExecutor.class, Mockito.mock(JobExecutor.class));
        registry.addSingleton(ConfigManager.class, configManager);

        processor = new JobProcessor(null, Mockito.mock(GrpcConcern.class, Answers.CALLS_REAL_METHODS), registry);
        clock = new TestClock();
        processor.setClock(clock);

        jobId = SampleMetadata.newHeader(ObjectType.JOB);
    }

    @AfterEach
    void teardown() throws Exception {

        storage.release();
        channel.shutdownNow();
        server.shutdownNow();
        serverExecutor.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void stalledExistsTimesOut() {

        storage.existsDelay = Duration.ofSeconds(10);

        var started = System.nanoTime();
        var status = checkJob();
        var elapsed = Duration.ofNanos(System.nanoTime() - started);

        Assertions.assertEquals(JobStatusCode.RUNNING, status.getStatusCode());
        Assertions.assertFalse(status.hasProgress());
        assertWithinDeadline(elapsed);
    }

    @Test
    void stalledReadTimesOut() {

        storage.readDelay = Duration.ofSeconds(10);

        var started = System.nanoTime();
        var status = checkJob();
        var elapsed = Duration.ofNanos(System.nanoTime() - started);

        Assertions.assertFalse(status.hasProgress());
        assertWithinDeadline(elapsed);
    }

    @Test
    void existsAndReadShareOneDeadline() {

        // Each call alone fits in the deadline, together they do not
        storage.existsDelay = Duration.ofMillis(1500);
        storage.readDelay = Duration.ofMillis(1500);

        var started = System.nanoTime();
        var status = checkJob();
        var elapsed = Duration.ofNanos(System.nanoTime() - started);

        Assertions.assertFalse(status.hasProgress());
        assertWithinDeadline(elapsed);
    }

    @Test
    void laterReadGetsItsOwnDeadline() {

        storage.existsDelay = Duration.ofSeconds(10);
        Assertions.assertFalse(checkJob().hasProgress());

        // The first deadline has passed, the next read after the reuse time has a full deadline of its own
        storage.existsDelay = Duration.ZERO;
        storage.readDelay = Duration.ofMillis(1500);
        clock.advance(JobProcessor.PROGRESS_REUSE_TIME.plusMillis(1));

        var status = checkJob();

        Assertions.assertTrue(status.hasProgress());
        Assertions.assertEquals("aggregate", status.getProgress().getSteps(0).getStepName());
    }

    private void assertWithinDeadline(Duration elapsed) {

        var deadline = JobProcessor.PROGRESS_READ_DEADLINE;

        Assertions.assertTrue(elapsed.compareTo(deadline.minusMillis(100)) >= 0, "Returned before the deadline: " + elapsed);
        Assertions.assertTrue(elapsed.compareTo(deadline.plusSeconds(1)) < 0, "Returned well after the deadline: " + elapsed);
    }

    private JobStatus checkJob() {

        var jobState = new JobState();
        jobState.tenant = SampleMetadata.TEST_TENANT;
        jobState.jobId = jobId;
        jobState.jobKey = MetadataUtil.objectKey(jobId);
        jobState.jobType = JobType.RUN_MODEL;
        jobState.tracStatus = JobStatusCode.RUNNING;
        jobState.cacheStatus = CacheStatus.RUNNING_IN_EXECUTOR;
        jobState.jobConfig = JobConfig.newBuilder()
                .putProperties(ConfigKeys.RESULT_STORAGE_LOCATION, "RESULT_STORAGE")
                .putProperties(ConfigKeys.RESULT_PROGRESS_PATH, PROGRESS_PATH)
                .build();

        var request = JobStatusRequest.newBuilder()
                .setTenant(SampleMetadata.TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(jobId))
                .build();

        var jobSelector = TagSelector.newBuilder()
                .setObjectType(ObjectType.JOB).setObjectId(jobId.getObjectId())
                .setObjectVersion(1).setLatestTag(true)
                .build();

        return processor.checkJob(request, jobSelector, Optional.of(jobState));
    }

    private static class StallingStorage extends TracStorageApiGrpc.TracStorageApiImplBase {

        private final Object lock = new Object();
        private boolean released = false;

        volatile Duration existsDelay = Duration.ZERO;
        volatile Duration readDelay = Duration.ZERO;

        void release() {
            synchronized (lock) {
                released = true;
                lock.notifyAll();
            }
        }

        private void stall(Duration delay) {

            var until = System.nanoTime() + delay.toNanos();

            synchronized (lock) {
                while (!released && System.nanoTime() < until) {
                    try {
                        lock.wait(Math.max(1, (until - System.nanoTime()) / 1_000_000));
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        @Override
        public void exists(StorageRequest request, StreamObserver<StorageExistsResponse> response) {

            stall(existsDelay);
            response.onNext(StorageExistsResponse.newBuilder().setExists(true).build());
            response.onCompleted();
        }

        @Override
        public void readSmallFile(StorageReadRequest request, StreamObserver<StorageReadResponse> response) {

            stall(readDelay);

            try {
                var progress = JobProgress.newBuilder()
                        .addSteps(JobStepStatus.newBuilder()
                                .setStepType(JobStepType.STEP_MODEL)
                                .setStepName("aggregate")
                                .setStatusCode(JobStepStatusCode.STEP_RUNNING))
                        .build();

                response.onNext(StorageReadResponse.newBuilder()
                        .setContent(ByteString.copyFromUtf8(JsonFormat.printer().print(progress)))
                        .build());
                response.onCompleted();
            }
            catch (Exception e) {
                response.onError(e);
            }
        }
    }

    private static final class TestClock extends Clock {

        private volatile Instant now = Instant.parse("2026-11-22T12:00:00Z");

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }
}
