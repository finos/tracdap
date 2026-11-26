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

import org.finos.tracdap.api.JobStatusRequest;
import org.finos.tracdap.api.MetadataReadRequest;
import org.finos.tracdap.api.MetadataWriteBatchRequest;
import org.finos.tracdap.api.MetadataWriteBatchResponse;
import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.api.StorageExistsResponse;
import org.finos.tracdap.api.StorageReadRequest;
import org.finos.tracdap.api.StorageReadResponse;
import org.finos.tracdap.api.StorageRequest;
import org.finos.tracdap.api.TracStorageApiGrpc;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.api.internal.RuntimeJobStatus;
import org.finos.tracdap.common.config.ConfigFormat;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.middleware.GrpcClientConfig;
import org.finos.tracdap.common.middleware.GrpcClientState;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.test.meta.SampleMetadata;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class JobProcessorTest {

    private InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient;
    private TracStorageApiGrpc.TracStorageApiBlockingStub storageClient;
    private GrpcConcern commonConcerns;
    private JobProcessor processor;

    private TagHeader jobId;
    private TagHeader resultId;
    private TestClock clock;

    @BeforeEach
    void setup() {

        metaClient = Mockito.mock(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub.class);
        storageClient = Mockito.mock(TracStorageApiGrpc.TracStorageApiBlockingStub.class);

        var configManager = Mockito.mock(ConfigManager.class);
        Mockito.when(configManager.loadRootConfigObject(PlatformConfig.class))
                .thenReturn(PlatformConfig.getDefaultInstance());

        var registry = new PluginRegistry();
        registry.addSingleton(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub.class, metaClient);
        registry.addSingleton(TracStorageApiGrpc.TracStorageApiBlockingStub.class, storageClient);
        registry.addSingleton(JobExecutor.class, Mockito.mock(JobExecutor.class));
        registry.addSingleton(ConfigManager.class, configManager);

        commonConcerns = Mockito.mock(GrpcConcern.class, Answers.CALLS_REAL_METHODS);

        processor = new JobProcessor(null, commonConcerns, registry);

        jobId = SampleMetadata.newHeader(ObjectType.JOB);
        resultId = SampleMetadata.newHeader(ObjectType.RESULT);

        clock = new TestClock();
        processor.setClock(clock);

        // Mockito stubs ignore deadlines, the deadline tests use an in-process server
        Mockito.when(storageClient.withDeadline(ArgumentMatchers.any())).thenReturn(storageClient);
    }

    @Test
    void fetchJobResult_executorSucceeded() throws Exception {

        storageReturns(runtimeResult(JobStatusCode.SUCCEEDED, ""));

        var jobState = jobState(JobStatusCode.SUCCEEDED, null);
        var newState = processor.fetchJobResult(jobState);

        Assertions.assertEquals(CacheStatus.RESULTS_RECEIVED, newState.cacheStatus);
        Assertions.assertEquals(JobStatusCode.FINISHING, newState.tracStatus);
        Assertions.assertEquals(JobStatusCode.SUCCEEDED, newState.runtimeResult.getResult().getStatusCode());
    }

    @Test
    void fetchJobResult_executorFailed() throws Exception {

        storageReturns(runtimeResult(JobStatusCode.FAILED, "Model failed"));

        var jobState = jobState(JobStatusCode.FAILED, "Executor message");
        var newState = processor.fetchJobResult(jobState);

        Assertions.assertEquals(CacheStatus.RESULTS_RECEIVED, newState.cacheStatus);
        Assertions.assertEquals(JobStatusCode.FAILED, newState.tracStatus);
        Assertions.assertEquals("Model failed", newState.runtimeResult.getResult().getStatusMessage());
    }

    @Test
    void fetchJobResult_executorFailedNoResultFile() {

        storageThrows(Status.Code.INTERNAL);

        var jobState = jobState(JobStatusCode.FAILED, "Executor message");
        var newState = processor.fetchJobResult(jobState);

        Assertions.assertEquals(CacheStatus.RESULTS_INVALID, newState.cacheStatus);
        Assertions.assertEquals(JobStatusCode.FAILED, newState.tracStatus);
        Assertions.assertEquals("Executor message", newState.statusMessage);
        Assertions.assertNull(newState.runtimeResult);
    }

    @Test
    void fetchJobResult_executorFailedNoResultFileNoMessage() {

        storageThrows(Status.Code.INTERNAL);

        var jobState = jobState(JobStatusCode.FAILED, null);
        var newState = processor.fetchJobResult(jobState);

        Assertions.assertEquals(CacheStatus.RESULTS_INVALID, newState.cacheStatus);
        Assertions.assertEquals(JobStatusCode.FAILED, newState.tracStatus);
        Assertions.assertEquals("No details available", newState.statusMessage);
    }

    @Test
    void fetchJobResult_executorStatusUnknownNoResultFile() {

        storageThrows(Status.Code.INTERNAL);

        var jobState = jobState(JobStatusCode.JOB_STATUS_CODE_NOT_SET, "Job status could not be determined");
        var newState = processor.fetchJobResult(jobState);

        Assertions.assertEquals(CacheStatus.RESULTS_INVALID, newState.cacheStatus);
        Assertions.assertEquals(JobStatusCode.FAILED, newState.tracStatus);
        Assertions.assertEquals("Job status could not be determined", newState.statusMessage);
    }

    @Test
    void fetchJobResult_executorSucceededNoResultFile() {

        storageThrows(Status.Code.INTERNAL);

        var jobState = jobState(JobStatusCode.SUCCEEDED, null);
        var newState = processor.fetchJobResult(jobState);

        Assertions.assertEquals(CacheStatus.RESULTS_INVALID, newState.cacheStatus);
        Assertions.assertEquals(JobStatusCode.FAILED, newState.tracStatus);
        Assertions.assertEquals("No details available", newState.statusMessage);
    }

    @Test
    void fetchJobResult_executorFailedResultUnreadable() {

        Mockito.when(storageClient.readSmallFile(ArgumentMatchers.any(StorageReadRequest.class)))
                .thenReturn(StorageReadResponse.newBuilder()
                .setContent(ByteString.copyFromUtf8("not a job result"))
                .build());

        var jobState = jobState(JobStatusCode.FAILED, "Executor message");
        var newState = processor.fetchJobResult(jobState);

        Assertions.assertEquals(CacheStatus.RESULTS_INVALID, newState.cacheStatus);
        Assertions.assertEquals(JobStatusCode.FAILED, newState.tracStatus);
        Assertions.assertEquals("Executor message", newState.statusMessage);
    }

    @ParameterizedTest
    @EnumSource(value = Status.Code.class, names = {"UNAVAILABLE", "DEADLINE_EXCEEDED"})
    void fetchJobResult_retryableError(Status.Code code) {

        storageThrows(code);

        var jobState = jobState(JobStatusCode.FAILED, "Executor message");

        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> processor.fetchJobResult(jobState));
        Assertions.assertEquals(code, error.getStatus().getCode());
    }

    @Test
    void scheduleLaunch_staleJobTagRetried() {

        var jobState = jobState(JobStatusCode.SUCCEEDED, null);
        jobState.clientConfig = passThroughClient();

        var v2 = nextTagVersion(jobId);
        var v3 = nextTagVersion(v2);

        Mockito.when(metaClient.updateTag(ArgumentMatchers.any()))
                .thenThrow(Status.ALREADY_EXISTS.asRuntimeException())
                .thenReturn(v3);

        Mockito.when(metaClient.readObject(ArgumentMatchers.any()))
                .thenReturn(Tag.newBuilder().setHeader(v2).build());

        var newState = processor.scheduleLaunch(jobState);

        Assertions.assertEquals(v3, newState.jobId);
        Assertions.assertEquals(CacheStatus.LAUNCH_SCHEDULED, newState.cacheStatus);

        var writes = ArgumentCaptor.forClass(MetadataWriteRequest.class);
        Mockito.verify(metaClient, Mockito.times(2)).updateTag(writes.capture());

        var retry = writes.getAllValues().get(1);
        Assertions.assertEquals(MetadataUtil.selectorFor(v2), retry.getPriorVersion());
        Assertions.assertEquals(MetadataConstants.TRAC_JOB_STATUS_ATTR, retry.getTagUpdates(0).getAttrName());
        Assertions.assertEquals(JobStatusCode.PREPARING.name(), MetadataCodec.decodeStringValue(retry.getTagUpdates(0).getValue()));
    }

    @Test
    void scheduleLaunch_otherErrorNotRetried() {

        var jobState = jobState(JobStatusCode.SUCCEEDED, null);
        jobState.clientConfig = passThroughClient();

        Mockito.when(metaClient.updateTag(ArgumentMatchers.any()))
                .thenThrow(Status.UNAVAILABLE.asRuntimeException());

        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> processor.scheduleLaunch(jobState));
        Assertions.assertEquals(Status.Code.UNAVAILABLE, error.getStatus().getCode());

        Mockito.verify(metaClient, Mockito.times(1)).updateTag(ArgumentMatchers.any());
        Mockito.verify(metaClient, Mockito.never()).readObject(ArgumentMatchers.any());
    }

    @Test
    void handleProcessingFailed_staleJobTagRetried() {

        var jobState = jobState(JobStatusCode.FAILED, "Executor message");
        jobState.clientConfig = passThroughClient();

        var latestJobId = nextTagVersion(jobId);

        Mockito.when(metaClient.writeBatch(ArgumentMatchers.any()))
                .thenThrow(Status.ALREADY_EXISTS.asRuntimeException())
                .thenReturn(MetadataWriteBatchResponse.getDefaultInstance());

        Mockito.when(metaClient.readObject(ArgumentMatchers.any()))
                .thenReturn(Tag.newBuilder().setHeader(latestJobId).build());

        var newState = processor.handleProcessingFailed(jobState, "Result could not be saved");

        Assertions.assertEquals(CacheStatus.READY_TO_REMOVE, newState.cacheStatus);
        Assertions.assertEquals(JobStatusCode.FAILED, newState.tracStatus);

        var batch = ArgumentCaptor.forClass(MetadataWriteBatchRequest.class);
        Mockito.verify(metaClient, Mockito.times(2)).writeBatch(batch.capture());
        Assertions.assertEquals(MetadataUtil.selectorFor(latestJobId), batch.getAllValues().get(1).getUpdateTags(0).getPriorVersion());
    }

    @Test
    void checkJob_cachedJobSameTenant() {

        var jobState = jobState(JobStatusCode.SUCCEEDED, null);
        jobState.tracStatus = JobStatusCode.RUNNING;
        jobState.cacheStatus = CacheStatus.RUNNING_IN_EXECUTOR;

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));

        Assertions.assertEquals(jobId, status.getJobId());
        Assertions.assertEquals(JobStatusCode.RUNNING, status.getStatusCode());
        Mockito.verify(metaClient, Mockito.never()).readObject(ArgumentMatchers.any());
    }

    @Test
    void checkJob_cachedJobOtherTenant() {

        var jobState = jobState(JobStatusCode.SUCCEEDED, null);
        jobState.tracStatus = JobStatusCode.RUNNING;

        Mockito.when(metaClient.readObject(ArgumentMatchers.any()))
                .thenThrow(Status.NOT_FOUND.withDescription("Metadata not found for JOB [x]").asRuntimeException());

        var request = statusRequest("OTHER_TENANT");
        var error = Assertions.assertThrows(StatusRuntimeException.class,
                () -> processor.checkJob(request, jobSelector(), Optional.of(jobState)));

        Assertions.assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        var read = ArgumentCaptor.forClass(MetadataReadRequest.class);
        Mockito.verify(metaClient).readObject(read.capture());
        Assertions.assertEquals("OTHER_TENANT", read.getValue().getTenant());
    }

    @ParameterizedTest
    @EnumSource(value = JobStatusCode.class, names = {"SUCCEEDED", "FAILED", "CANCELLED"})
    void checkJob_notCachedReadsMetadata(JobStatusCode statusCode) {

        var errorMessage = statusCode == JobStatusCode.SUCCEEDED ? null : "Model failed";
        var latestJobId = nextTagVersion(jobId);

        Mockito.when(metaClient.readObject(ArgumentMatchers.any()))
                .thenReturn(jobTag(latestJobId, statusCode.name(), errorMessage));

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.empty());

        Assertions.assertEquals(latestJobId, status.getJobId());
        Assertions.assertEquals(statusCode, status.getStatusCode());
        Assertions.assertEquals(errorMessage == null ? "" : errorMessage, status.getStatusMessage());

        var read = ArgumentCaptor.forClass(MetadataReadRequest.class);
        Mockito.verify(metaClient).readObject(read.capture());
        Assertions.assertEquals(SampleMetadata.TEST_TENANT, read.getValue().getTenant());
        Assertions.assertEquals(jobSelector(), read.getValue().getSelector());
    }

    @Test
    void checkJob_notCachedRunningJob() {

        Mockito.when(metaClient.readObject(ArgumentMatchers.any()))
                .thenReturn(jobTag(jobId, JobStatusCode.RUNNING.name(), null));

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.empty());

        Assertions.assertEquals(jobId, status.getJobId());
        Assertions.assertEquals(JobStatusCode.RUNNING, status.getStatusCode());
        Assertions.assertEquals("", status.getStatusMessage());
    }

    @Test
    void checkJob_notCachedUnrecognisedStatus() {

        Mockito.when(metaClient.readObject(ArgumentMatchers.any()))
                .thenReturn(jobTag(jobId, "NOT_A_STATUS", null));

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.empty());

        Assertions.assertEquals(jobId, status.getJobId());
        Assertions.assertEquals(JobStatusCode.JOB_STATUS_CODE_NOT_SET, status.getStatusCode());
    }

    @Test
    void checkJob_notFoundPassesThrough() {

        var notFound = Status.NOT_FOUND.withDescription("Metadata not found for JOB [x]").asRuntimeException();

        Mockito.when(metaClient.readObject(ArgumentMatchers.any())).thenThrow(notFound);

        var request = statusRequest(SampleMetadata.TEST_TENANT);
        var error = Assertions.assertThrows(StatusRuntimeException.class,
                () -> processor.checkJob(request, jobSelector(), Optional.empty()));

        Assertions.assertSame(notFound, error);
    }

    @Test
    void checkJob_metadataReadUsesCallerContext() {

        var callerClient = Mockito.mock(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub.class);

        Mockito.when(callerClient.readObject(ArgumentMatchers.any()))
                .thenReturn(jobTag(jobId, JobStatusCode.SUCCEEDED.name(), null));

        Mockito.doReturn(clientStateFor(callerClient))
                .when(commonConcerns).prepareClientCall(ArgumentMatchers.any());

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.empty());

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, status.getStatusCode());
        Mockito.verify(commonConcerns).prepareClientCall(io.grpc.Context.current());
        Mockito.verify(callerClient).readObject(ArgumentMatchers.any());
        Mockito.verify(metaClient, Mockito.never()).readObject(ArgumentMatchers.any());
    }

    @Test
    void checkJob_progressMissingSnapshotNotRead() {

        snapshotExists(false);

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(runningJob(true)));

        Assertions.assertEquals(JobStatusCode.RUNNING, status.getStatusCode());
        Assertions.assertFalse(status.hasProgress());
        Mockito.verify(storageClient).exists(ArgumentMatchers.any(StorageRequest.class));
        Mockito.verify(storageClient, Mockito.never()).readSmallFile(ArgumentMatchers.any());
    }

    @Test
    void checkJob_progressSeenSkipsExists() throws Exception {

        snapshotExists(true);
        snapshotReturns(progressJson());

        var jobState = runningJob(true);
        var first = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));

        clock.advance(PROGRESS_REUSE);
        var second = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));

        Assertions.assertEquals(sampleProgress(), first.getProgress());
        Assertions.assertEquals(sampleProgress(), second.getProgress());
        Mockito.verify(storageClient, Mockito.times(1)).exists(ArgumentMatchers.any(StorageRequest.class));
        Mockito.verify(storageClient, Mockito.times(2)).readSmallFile(ArgumentMatchers.any());
    }

    @Test
    void checkJob_progressReadsJobLocation() throws Exception {

        snapshotExists(true);
        snapshotReturns(progressJson());

        processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(runningJob(true)));

        var existsRequest = ArgumentCaptor.forClass(StorageRequest.class);
        var readRequest = ArgumentCaptor.forClass(StorageReadRequest.class);
        Mockito.verify(storageClient).exists(existsRequest.capture());
        Mockito.verify(storageClient).readSmallFile(readRequest.capture());

        for (var request : java.util.List.of(
                java.util.List.of(existsRequest.getValue().getTenant(), existsRequest.getValue().getStorageKey(), existsRequest.getValue().getStoragePath()),
                java.util.List.of(readRequest.getValue().getTenant(), readRequest.getValue().getStorageKey(), readRequest.getValue().getStoragePath())))
            Assertions.assertEquals(java.util.List.of(SampleMetadata.TEST_TENANT, "RESULT_STORAGE", PROGRESS_PATH), request);
    }

    @Test
    void checkJob_progressUnreadable() {

        snapshotExists(true);
        Mockito.when(storageClient.readSmallFile(ArgumentMatchers.any(StorageReadRequest.class)))
                .thenThrow(Status.INTERNAL.asRuntimeException());

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(runningJob(true)));

        Assertions.assertEquals(JobStatusCode.RUNNING, status.getStatusCode());
        Assertions.assertFalse(status.hasProgress());
    }

    @Test
    void checkJob_progressCorrupt() {

        snapshotExists(true);
        snapshotReturns("{ \"steps\": [ { \"stepType\": ");

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(runningJob(true)));

        Assertions.assertEquals(JobStatusCode.RUNNING, status.getStatusCode());
        Assertions.assertFalse(status.hasProgress());
    }

    @Test
    void checkJob_progressKeptWhenLaterReadFails() throws Exception {

        snapshotExists(true);
        snapshotReturns(progressJson());

        var jobState = runningJob(true);
        processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));

        snapshotReturns("{ \"steps\": [ { \"stepType\": ");
        clock.advance(PROGRESS_REUSE);
        var partial = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));

        Mockito.when(storageClient.readSmallFile(ArgumentMatchers.any(StorageReadRequest.class)))
                .thenThrow(Status.UNAVAILABLE.asRuntimeException());
        clock.advance(PROGRESS_REUSE);
        var unavailable = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));

        Assertions.assertEquals(sampleProgress(), partial.getProgress());
        Assertions.assertEquals(sampleProgress(), unavailable.getProgress());
        Mockito.verify(storageClient, Mockito.times(3)).readSmallFile(ArgumentMatchers.any());
    }

    @Test
    void checkJob_progressUnknownField() throws Exception {

        snapshotExists(true);
        snapshotReturns(progressJson().replaceFirst("\\{", "{ \"stepGroups\": [ { \"groupName\": \"later\" } ],"));

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(runningJob(true)));

        Assertions.assertEquals(sampleProgress(), status.getProgress());
    }

    @Test
    void checkJob_progressNoPath() {

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(runningJob(false)));

        Assertions.assertEquals(JobStatusCode.RUNNING, status.getStatusCode());
        Assertions.assertFalse(status.hasProgress());
        Mockito.verifyNoInteractions(storageClient);
    }

    @ParameterizedTest
    @EnumSource(value = JobStatusCode.class, names = {"QUEUED", "SUCCEEDED", "FAILED", "CANCELLED"})
    void checkJob_progressNotReadForStatus(JobStatusCode statusCode) {

        var jobState = runningJob(true);
        jobState.tracStatus = statusCode;
        jobState.cacheStatus = statusCode == JobStatusCode.QUEUED ? CacheStatus.QUEUED_IN_TRAC : CacheStatus.READY_TO_REMOVE;

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));

        Assertions.assertEquals(statusCode, status.getStatusCode());
        Assertions.assertFalse(status.hasProgress());
        Mockito.verifyNoInteractions(storageClient);
    }

    @ParameterizedTest
    @EnumSource(value = JobStatusCode.class, names = {"SUBMITTED", "RUNNING", "FINISHING"})
    void checkJob_progressReadForStatus(JobStatusCode statusCode) throws Exception {

        snapshotExists(true);
        snapshotReturns(progressJson());

        var jobState = runningJob(true);
        jobState.tracStatus = statusCode;

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));

        Assertions.assertEquals(statusCode, status.getStatusCode());
        Assertions.assertEquals(sampleProgress(), status.getProgress());
    }

    @Test
    void checkJob_progressReadWhenFinishingComputed() throws Exception {

        snapshotExists(true);
        snapshotReturns(progressJson());

        // The job has succeeded in the executor, but its results are not saved yet
        var jobState = runningJob(true);
        jobState.tracStatus = JobStatusCode.SUCCEEDED;
        jobState.cacheStatus = CacheStatus.EXECUTOR_SUCCEEDED;

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));

        Assertions.assertEquals(JobStatusCode.FINISHING, status.getStatusCode());
        Assertions.assertEquals(sampleProgress(), status.getProgress());
    }

    @Test
    void checkJob_progressNotReadFromMetadata() {

        Mockito.when(metaClient.readObject(ArgumentMatchers.any())).thenReturn(jobTag(jobId, JobStatusCode.RUNNING.name(), null));

        var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.empty());

        Assertions.assertEquals(JobStatusCode.RUNNING, status.getStatusCode());
        Assertions.assertFalse(status.hasProgress());
        Mockito.verifyNoInteractions(storageClient);
    }

    @Test
    void checkJob_progressNotReadForOtherTenant() {

        Mockito.when(metaClient.readObject(ArgumentMatchers.any()))
                .thenThrow(Status.NOT_FOUND.withDescription("Metadata not found").asRuntimeException());

        Assertions.assertThrows(StatusRuntimeException.class, () ->
                processor.checkJob(statusRequest("OTHER_TENANT"), jobSelector(), Optional.of(runningJob(true))));

        Mockito.verifyNoInteractions(storageClient);
    }

    @Test
    void getStatus_neverReadsProgress() {

        var status = processor.getStatus(runningJob(true));

        Assertions.assertFalse(status.hasProgress());
        Mockito.verifyNoInteractions(storageClient);
    }

    @Test
    void checkJob_progressReusedWithinWindow() throws Exception {

        snapshotExists(true);
        snapshotReturns(progressJson());

        var jobState = runningJob(true);

        for (var i = 0; i < 5; i++) {
            var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));
            Assertions.assertEquals(sampleProgress(), status.getProgress());
            clock.advance(Duration.ofMillis(500));
        }

        Mockito.verify(storageClient, Mockito.times(1)).exists(ArgumentMatchers.any(StorageRequest.class));
        Mockito.verify(storageClient, Mockito.times(1)).readSmallFile(ArgumentMatchers.any());
    }

    @Test
    void checkJob_failedReadReusedWithinWindow() {

        Mockito.when(storageClient.exists(ArgumentMatchers.any(StorageRequest.class)))
                .thenThrow(Status.UNAVAILABLE.asRuntimeException());

        var jobState = runningJob(true);

        for (var i = 0; i < 5; i++) {
            var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));
            Assertions.assertFalse(status.hasProgress());
            clock.advance(Duration.ofMillis(500));
        }

        Mockito.verify(storageClient, Mockito.times(1)).exists(ArgumentMatchers.any(StorageRequest.class));

        // After the window the read is tried again
        clock.advance(PROGRESS_REUSE);
        processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobState));

        Mockito.verify(storageClient, Mockito.times(2)).exists(ArgumentMatchers.any(StorageRequest.class));
    }

    @Test
    void checkJob_progressEntriesExpire() throws Exception {

        snapshotExists(true);
        snapshotReturns(progressJson());

        var firstJob = runningJob(true);
        processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(firstJob));

        Assertions.assertEquals(java.util.Set.of(firstJob.jobKey), processor.progressReadsHeld());

        // The first job is no longer polled, a check for any job drops its entry once it is past retention
        clock.advance(JobProcessor.PROGRESS_RETENTION_TIME.plusSeconds(1));
        processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(runningJob(false)));

        Assertions.assertEquals(java.util.Set.of(), processor.progressReadsHeld());

        // A job polled again after expiry checks the snapshot exists again
        processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(firstJob));
        Mockito.verify(storageClient, Mockito.times(2)).exists(ArgumentMatchers.any(StorageRequest.class));
    }

    @Test
    void checkJob_progressManyThreads() throws Exception {

        var jobs = new ArrayList<JobState>();

        for (var i = 0; i < 8; i++) {
            var job = runningJob(true);
            job.jobKey = "JOB-progress-" + i;
            job.jobConfig = job.jobConfig.toBuilder().putProperties(ConfigKeys.RESULT_PROGRESS_PATH, "progress/" + i).build();
            jobs.add(job);
        }

        Mockito.when(storageClient.exists(ArgumentMatchers.any(StorageRequest.class)))
                .thenReturn(StorageExistsResponse.newBuilder().setExists(true).build());
        Mockito.when(storageClient.readSmallFile(ArgumentMatchers.any(StorageReadRequest.class))).thenAnswer(call -> {
            var path = call.getArgument(0, StorageReadRequest.class).getStoragePath();
            var progress = sampleProgress().toBuilder().setTotalLogEntries(Long.parseLong(path.substring(path.indexOf('/') + 1)));
            return StorageReadResponse.newBuilder().setContent(ByteString.copyFromUtf8(JsonFormat.printer().print(progress))).build();
        });

        var pool = Executors.newFixedThreadPool(8);
        var tasks = new ArrayList<Callable<Boolean>>();

        for (var thread = 0; thread < 8; thread++) {
            tasks.add(() -> {
                for (var poll = 0; poll < 200; poll++) {
                    var index = poll % jobs.size();
                    var status = processor.checkJob(statusRequest(SampleMetadata.TEST_TENANT), jobSelector(), Optional.of(jobs.get(index)));
                    if (status.getProgress().getTotalLogEntries() != index)
                        return false;
                    clock.advance(Duration.ofMillis(10));
                }
                return true;
            });
        }

        try {
            for (var result : pool.invokeAll(tasks, 30, TimeUnit.SECONDS))
                Assertions.assertTrue(result.get());
        }
        finally {
            pool.shutdownNow();
        }
    }

    private JobStatusRequest statusRequest(String tenant) {

        return JobStatusRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(MetadataUtil.selectorFor(jobId))
                .build();
    }

    private TagSelector jobSelector() {

        return TagSelector.newBuilder()
                .setObjectType(ObjectType.JOB)
                .setObjectId(jobId.getObjectId())
                .setObjectVersion(1)
                .setLatestTag(true)
                .build();
    }

    private static Tag jobTag(TagHeader header, String statusName, String errorMessage) {

        var tag = Tag.newBuilder()
                .setHeader(header)
                .putAttrs(MetadataConstants.TRAC_JOB_STATUS_ATTR, MetadataCodec.encodeValue(statusName));

        if (errorMessage != null)
            tag.putAttrs(MetadataConstants.TRAC_JOB_ERROR_MESSAGE_ATTR, MetadataCodec.encodeValue(errorMessage));

        return tag.build();
    }

    private static GrpcClientState clientStateFor(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub callerClient) {

        return new GrpcClientState() {

            @Override
            @SuppressWarnings("unchecked")
            public <TStub extends AbstractStub<TStub>> TStub configureClient(TStub clientStub) {
                return (TStub) callerClient;
            }

            @Override
            public GrpcClientState restore(GrpcConcern grpcConcern) {
                return this;
            }
        };
    }

    private static GrpcClientConfig passThroughClient() {

        return new GrpcClientConfig() {
            @Override
            public <TStub extends AbstractStub<TStub>> TStub configureClient(TStub clientStub) {
                return clientStub;
            }
        };
    }

    private static TagHeader nextTagVersion(TagHeader header) {

        return header.toBuilder()
                .setTagVersion(header.getTagVersion() + 1)
                .build();
    }

    private static final String PROGRESS_PATH = "path/to/trac_job_progress.json";
    private static final Duration PROGRESS_REUSE = JobProcessor.PROGRESS_REUSE_TIME.plusMillis(1);

    private JobState runningJob(boolean progressPath) {

        var jobState = jobState(JobStatusCode.SUCCEEDED, null);
        jobState.tracStatus = JobStatusCode.RUNNING;
        jobState.cacheStatus = CacheStatus.RUNNING_IN_EXECUTOR;

        if (progressPath)
            jobState.jobConfig = jobState.jobConfig.toBuilder().putProperties(ConfigKeys.RESULT_PROGRESS_PATH, PROGRESS_PATH).build();

        return jobState;
    }

    private static JobProgress sampleProgress() {

        return JobProgress.newBuilder()
                .addSteps(JobStepStatus.newBuilder()
                        .setStepType(JobStepType.STEP_MODEL)
                        .setStepName("aggregate")
                        .setStatusCode(JobStepStatusCode.STEP_RUNNING)
                        .setStartTime(MetadataCodec.encodeDatetime(Instant.parse("2026-11-22T12:00:00Z").atOffset(ZoneOffset.UTC))))
                .addLogEntries(JobLogEntry.newBuilder()
                        .setTimestamp(MetadataCodec.encodeDatetime(Instant.parse("2026-11-22T12:00:01Z").atOffset(ZoneOffset.UTC)))
                        .setLevel("INFO")
                        .setSource(JobLogSource.LOG_SOURCE_MODEL)
                        .setStepType(JobStepType.STEP_MODEL)
                        .setStepName("aggregate")
                        .setMessage("Aggregating 12 regions"))
                .setTotalLogEntries(1)
                .build();
    }

    private static String progressJson() throws Exception {
        return JsonFormat.printer().print(sampleProgress());
    }

    private void snapshotExists(boolean exists) {

        Mockito.when(storageClient.exists(ArgumentMatchers.any(StorageRequest.class)))
                .thenReturn(StorageExistsResponse.newBuilder().setExists(exists).build());
    }

    private void snapshotReturns(String content) {

        Mockito.when(storageClient.readSmallFile(ArgumentMatchers.any(StorageReadRequest.class)))
                .thenReturn(StorageReadResponse.newBuilder()
                .setContent(ByteString.copyFrom(content, StandardCharsets.UTF_8))
                .build());
    }

    private static final class TestClock extends Clock {

        private volatile Instant now = Instant.parse("2026-11-22T12:00:00Z");

        synchronized void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    private void storageReturns(JobResult jobResult) throws Exception {

        var content = JsonFormat.printer().print(jobResult);

        Mockito.when(storageClient.readSmallFile(ArgumentMatchers.any(StorageReadRequest.class)))
                .thenReturn(StorageReadResponse.newBuilder()
                .setContent(ByteString.copyFrom(content, StandardCharsets.UTF_8))
                .build());
    }

    private void storageThrows(Status.Code code) {

        Mockito.when(storageClient.readSmallFile(ArgumentMatchers.any(StorageReadRequest.class)))
                .thenThrow(Status.fromCode(code).asRuntimeException());
    }

    private JobState jobState(JobStatusCode executorStatus, String statusMessage) {

        var executorFailed = executorStatus != JobStatusCode.SUCCEEDED;

        var jobState = new JobState();
        jobState.tenant = SampleMetadata.TEST_TENANT;
        jobState.jobId = jobId;
        jobState.jobKey = MetadataUtil.objectKey(jobId);
        jobState.jobType = JobType.RUN_MODEL;
        jobState.resultId = resultId;
        jobState.tracStatus = executorFailed ? JobStatusCode.FAILED : JobStatusCode.FINISHING;
        jobState.cacheStatus = executorFailed ? CacheStatus.EXECUTOR_FAILED : CacheStatus.EXECUTOR_SUCCEEDED;
        jobState.statusMessage = statusMessage;
        jobState.runtimeStatus = RuntimeJobStatus.newBuilder().setStatusCode(executorStatus).build();

        jobState.jobConfig = JobConfig.newBuilder()
                .putProperties(ConfigKeys.RESULT_STORAGE_LOCATION, "RESULT_STORAGE")
                .putProperties(ConfigKeys.RESULT_STORAGE_PATH, "path/to/trac_job_result.json")
                .putProperties(ConfigKeys.RESULT_FORMAT, ConfigFormat.JSON.toString())
                .build();

        return jobState;
    }

    private JobResult runtimeResult(JobStatusCode statusCode, String statusMessage) {

        return JobResult.newBuilder()
                .setJobId(jobId)
                .setResultId(resultId)
                .setResult(ResultDefinition.newBuilder()
                .setJobId(MetadataUtil.selectorFor(jobId))
                .setStatusCode(statusCode)
                .setStatusMessage(statusMessage))
                .build();
    }
}
