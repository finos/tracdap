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

package org.finos.tracdap.svc.orch.api;

import org.finos.tracdap.api.JobRequest;
import org.finos.tracdap.api.JobStatus;
import org.finos.tracdap.api.JobStatusRequest;
import org.finos.tracdap.common.exception.ECacheCorruption;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.orch.service.JobManager;
import org.finos.tracdap.svc.orch.service.JobProcessor;
import org.finos.tracdap.svc.orch.service.JobState;
import org.finos.tracdap.test.meta.SampleMetadata;

import io.grpc.stub.StreamObserver;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class TracOrchestratorApiTest {

    private JobManager jobManager;
    private JobProcessor jobProcessor;
    private TracOrchestratorApi api;

    private TagHeader jobId;
    private JobStatus cachedStatus;

    @BeforeEach
    void setup() {

        jobManager = Mockito.mock(JobManager.class);
        jobProcessor = Mockito.mock(JobProcessor.class);

        var registry = new PluginRegistry();
        registry.addSingleton(JobManager.class, jobManager);
        registry.addSingleton(JobProcessor.class, jobProcessor);

        var commonConcerns = Mockito.mock(GrpcConcern.class, Answers.CALLS_REAL_METHODS);

        api = new TracOrchestratorApi(registry, commonConcerns);

        jobId = SampleMetadata.newHeader(ObjectType.JOB);
        cachedStatus = JobStatus.newBuilder().setJobId(jobId).setStatusCode(JobStatusCode.RUNNING).build();

        Mockito.when(jobProcessor.checkJob(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(cachedStatus);
    }

    @Test
    void checkJob_explicitTagVersion() {

        checkJobFindsVersionOne(MetadataUtil.selectorFor(jobId));
    }

    @Test
    void checkJob_latestTag() {

        checkJobFindsVersionOne(MetadataUtil.selectorFor(jobId, false, true));
    }

    @Test
    void checkJob_latestObject() {

        checkJobFindsVersionOne(MetadataUtil.selectorForLatest(jobId));
    }

    @Test
    void checkJob_asOf() {

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.JOB)
                .setObjectId(jobId.getObjectId())
                .setObjectAsOf(DatetimeValue.newBuilder().setIsoDatetime("2026-11-22T12:00:00Z"))
                .setTagAsOf(DatetimeValue.newBuilder().setIsoDatetime("2026-11-22T12:00:00Z"))
                .build();

        checkJobFindsVersionOne(selector);
    }

    private void checkJobFindsVersionOne(TagSelector selector) {

        var cachedJob = Optional.of(new JobState());
        var expectedKey = "JOB-" + jobId.getObjectId() + "-v1";

        Mockito.when(jobManager.queryJob(expectedKey)).thenReturn(cachedJob);

        var response = new Response<JobStatus>();
        api.checkJob(statusRequest(selector), response);

        Assertions.assertNull(response.error);
        Assertions.assertEquals(List.of(cachedStatus), response.values);

        var jobSelector = ArgumentCaptor.forClass(TagSelector.class);
        Mockito.verify(jobProcessor).checkJob(ArgumentMatchers.eq(statusRequest(selector)), jobSelector.capture(), ArgumentMatchers.same(cachedJob));

        Assertions.assertEquals(ObjectType.JOB, jobSelector.getValue().getObjectType());
        Assertions.assertEquals(jobId.getObjectId(), jobSelector.getValue().getObjectId());
        Assertions.assertEquals(1, jobSelector.getValue().getObjectVersion());
        Assertions.assertTrue(jobSelector.getValue().getLatestTag());
    }

    @Test
    void checkJob_objectVersionTwoNotFound() {

        var selector = MetadataUtil.selectorFor(jobId).toBuilder().setObjectVersion(2).build();

        var response = new Response<JobStatus>();
        api.checkJob(statusRequest(selector), response);

        Assertions.assertInstanceOf(EMetadataNotFound.class, response.error);
        Assertions.assertTrue(response.error.getMessage().startsWith("Metadata not found for JOB [" + jobId.getObjectId() + "]"));

        Mockito.verifyNoInteractions(jobManager);
        Mockito.verify(jobProcessor, Mockito.never()).checkJob(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    @Test
    void checkJob_corruptCacheEntryIsMiss() {

        Mockito.when(jobManager.queryJob(ArgumentMatchers.anyString()))
                .thenThrow(new ECacheCorruption("Cache entry could not be decoded"));

        var response = new Response<JobStatus>();
        api.checkJob(statusRequest(MetadataUtil.selectorFor(jobId)), response);

        Assertions.assertNull(response.error);
        Mockito.verify(jobProcessor).checkJob(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.eq(Optional.empty()));
    }

    @Test
    void submitJob_replyUnchanged() {

        var request = JobRequest.getDefaultInstance();
        var newJob = new JobState();
        var assembled = new JobState();
        var cached = new JobState();
        var submitStatus = JobStatus.newBuilder().setJobId(jobId).setStatusCode(JobStatusCode.QUEUED).build();

        Mockito.when(jobProcessor.newJob(ArgumentMatchers.eq(request), ArgumentMatchers.any())).thenReturn(newJob);
        Mockito.when(jobProcessor.assembleAndValidate(newJob)).thenReturn(assembled);
        Mockito.when(jobManager.addNewJob(assembled)).thenReturn(cached);
        Mockito.when(jobProcessor.getStatus(cached)).thenReturn(submitStatus);

        var response = new Response<JobStatus>();
        api.submitJob(request, response);

        Assertions.assertNull(response.error);
        Assertions.assertEquals(List.of(submitStatus), response.values);
        Mockito.verify(jobProcessor, Mockito.never()).checkJob(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    @Test
    void validateJob_replyUnchanged() {

        var request = JobRequest.getDefaultInstance();
        var newJob = new JobState();
        var assembled = new JobState();
        var validateStatus = JobStatus.newBuilder().setStatusCode(JobStatusCode.VALIDATED).build();

        Mockito.when(jobProcessor.newJob(ArgumentMatchers.eq(request), ArgumentMatchers.any())).thenReturn(newJob);
        Mockito.when(jobProcessor.assembleAndValidate(newJob)).thenReturn(assembled);
        Mockito.when(jobProcessor.getStatus(assembled)).thenReturn(validateStatus);

        var response = new Response<JobStatus>();
        api.validateJob(request, response);

        Assertions.assertNull(response.error);
        Assertions.assertEquals(List.of(validateStatus), response.values);
        Mockito.verifyNoInteractions(jobManager);
    }

    private static JobStatusRequest statusRequest(TagSelector selector) {

        return JobStatusRequest.newBuilder()
                .setTenant(SampleMetadata.TEST_TENANT)
                .setSelector(selector)
                .build();
    }

    private static class Response<T> implements StreamObserver<T> {

        final List<T> values = new ArrayList<>();
        Throwable error;

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable error) {
            this.error = error;
        }

        @Override
        public void onCompleted() {
        }
    }
}
