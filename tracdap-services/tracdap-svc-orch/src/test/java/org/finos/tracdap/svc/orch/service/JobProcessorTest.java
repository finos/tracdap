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

import org.finos.tracdap.api.StorageReadRequest;
import org.finos.tracdap.api.StorageReadResponse;
import org.finos.tracdap.api.TracStorageApiGrpc;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.api.internal.RuntimeJobStatus;
import org.finos.tracdap.common.config.ConfigFormat;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.metadata.MetadataUtil;
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;


public class JobProcessorTest {

    private TracStorageApiGrpc.TracStorageApiBlockingStub storageClient;
    private JobProcessor processor;

    private TagHeader jobId;
    private TagHeader resultId;

    @BeforeEach
    void setup() {

        storageClient = Mockito.mock(TracStorageApiGrpc.TracStorageApiBlockingStub.class);

        var configManager = Mockito.mock(ConfigManager.class);
        Mockito.when(configManager.loadRootConfigObject(PlatformConfig.class))
                .thenReturn(PlatformConfig.getDefaultInstance());

        var registry = new PluginRegistry();
        registry.addSingleton(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub.class,
                Mockito.mock(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub.class));
        registry.addSingleton(TracStorageApiGrpc.TracStorageApiBlockingStub.class, storageClient);
        registry.addSingleton(JobExecutor.class, Mockito.mock(JobExecutor.class));
        registry.addSingleton(ConfigManager.class, configManager);

        var commonConcerns = Mockito.mock(GrpcConcern.class, Answers.CALLS_REAL_METHODS);

        processor = new JobProcessor(null, commonConcerns, registry);

        jobId = SampleMetadata.newHeader(ObjectType.JOB);
        resultId = SampleMetadata.newHeader(ObjectType.RESULT);
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
