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

package com.accenture.trac.svc.orch.service;

import org.finos.tracdap.api.*;
import org.finos.tracdap.metadata.JobStatusCode;
import com.accenture.trac.common.exception.EMetadataNotFound;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataUtil;

import com.accenture.trac.svc.orch.cache.IJobCache;
import com.accenture.trac.svc.orch.cache.JobState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class JobApiService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JobLifecycle jobLifecycle;
    private final IJobCache jobCache;

    public JobApiService(
            JobLifecycle jobLifecycle,
            IJobCache jobCache) {

        this.jobLifecycle = jobLifecycle;
        this.jobCache = jobCache;
    }

    public CompletableFuture<JobStatus> validateJob(JobRequest request) {

        var jobState = newJob(request);

        return CompletableFuture.completedFuture(jobState)

                .thenApply(s -> jobStatus(s, JobStatusCode.PREPARING))

                .thenCompose(jobLifecycle::assembleAndValidate)

                .thenApply(s -> jobStatus(s, JobStatusCode.VALIDATED))

                .thenApply(this::reportStatus);
    }

    public CompletableFuture<JobStatus> submitJob(JobRequest request) {

        var jobState = newJob(request);

        return CompletableFuture.completedFuture(jobState)

                .thenApply(s -> jobStatus(s, JobStatusCode.PREPARING))

                .thenCompose(jobLifecycle::assembleAndValidate)

                .thenApply(s -> jobStatus(s, JobStatusCode.VALIDATED))
                .thenApply(s -> jobStatus(s, JobStatusCode.PENDING))

                .thenCompose(jobLifecycle::saveInitialMetadata)

                .thenCompose(this::submitForExecution)

                .thenApply(this::reportStatus);
    }

    public CompletionStage<JobStatus> checkJob(JobStatusRequest request) {

        // TODO: Keys for other selector types
        if (!request.getSelector().hasObjectVersion())
            throw new EUnexpected();

        var jobKey = MetadataUtil.objectKey(request.getSelector());
        var jobState = jobCache.readJob(jobKey);

        // TODO: Should there be a different error for jobs not found in the cache? EJobNotLive?
        if (jobState == null) {
            var message = String.format("Job not found (it may have completed): [%s]", jobKey);
            log.error(message);
            throw new EMetadataNotFound(message);
        }

        var jobStatus = reportStatus(jobState);

        return CompletableFuture.completedFuture(jobStatus);
    }

    private JobState newJob(JobRequest request) {

        var jobState = new JobState();
        jobState.tenant = request.getTenant();
        jobState.jobType = request.getJob().getJobType();
        jobState.definition = request.getJob();
        jobState.jobRequest = request;

        return jobState;
    }

    private JobState jobStatus(JobState jobState, JobStatusCode statusCode) {

        jobState.statusCode = statusCode;
        return jobState;
    }

    private CompletionStage<JobState> submitForExecution(JobState jobState) {

        var jobKey = MetadataUtil.objectKey(jobState.jobId);

        try (var ctx = jobCache.useTicket(jobKey)) {

            // Should not happen for a new job ID
            // However if it does, we definitely want to report an error!
            if (ctx.superseded())
                throw new EUnexpected();

            jobState.jobKey = jobKey;
            jobState.statusCode = JobStatusCode.QUEUED;

            jobCache.createJob(jobKey, jobState, ctx.ticket());

            return CompletableFuture.completedFuture(jobState);
        }
    }

    private JobStatus reportStatus(JobState jobState) {

        var status = JobStatus.newBuilder();

        if (jobState.jobId != null)
            status.setJobId(jobState.jobId);

        status.setStatusCode(jobState.statusCode);

        if (jobState.statusMessage != null)
            status.setStatusMessage(jobState.statusMessage);

        return status.build();
    }
}
