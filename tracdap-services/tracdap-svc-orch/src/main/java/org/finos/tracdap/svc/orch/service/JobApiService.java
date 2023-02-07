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

package org.finos.tracdap.svc.orch.service;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.auth.internal.AuthHelpers;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.JobStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobApiService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JobManager jobManager;
    private final JobProcessor jobProcessor;

    public JobApiService(JobManager jobManager, JobProcessor jobProcessor) {

        this.jobManager = jobManager;
        this.jobProcessor = jobProcessor;
    }

    public JobStatus validateJob(JobRequest request) {

        var jobState = newJob(request);

        jobState = jobProcessor.assembleAndValidate(jobState);

        return reportStatus(jobState);
    }

    public JobStatus submitJob(JobRequest request) {

        var jobState = newJob(request);

        jobState = jobProcessor.assembleAndValidate(jobState);
        jobState = jobManager.addNewJob(jobState);

        return reportStatus(jobState);
    }

    public JobStatus checkJob(JobStatusRequest request) {

        // TODO: Keys for other selector types
        if (!request.getSelector().hasObjectVersion())
            throw new EUnexpected();

        var jobKey = MetadataUtil.objectKey(request.getSelector());
        var jobState = jobManager.queryJob(jobKey);

        // TODO: Should there be a different error for jobs not found in the cache? EJobNotLive?
        if (jobState == null) {
            var message = String.format("Job not found (it may have completed): [%s]", jobKey);
            log.error(message);
            throw new EMetadataNotFound(message);
        }

        return reportStatus(jobState);
    }

    private JobState newJob(JobRequest request) {

        var jobState = new JobState();
        jobState.tenant = request.getTenant();
        jobState.owner = AuthHelpers.currentUser();

        jobState.jobType = request.getJob().getJobType();
        jobState.definition = request.getJob();
        jobState.jobRequest = request;

        jobState.tracStatus = JobStatusCode.PREPARING;

        return jobState;
    }

    private JobStatus reportStatus(JobState jobState) {

        var status = JobStatus.newBuilder();

        if (jobState.jobId != null)
            status.setJobId(jobState.jobId);

        status.setStatusCode(jobState.tracStatus);

        if (jobState.statusMessage != null)
            status.setStatusMessage(jobState.statusMessage);

        // This is a work-around because the new orchestration logic sets tracStatus before the results are saved
        // We need to merge the remaining logic in JobLifecycle into JobProcessor
        // Then tracStatus can be set on a new state object prior to saving and before updating the cache

        if (jobState.tracStatus == JobStatusCode.SUCCEEDED || jobState.tracStatus == JobStatusCode.FAILED) {
            if (jobState.cacheStatus.startsWith("EXECUTOR_") || jobState.cacheStatus.startsWith("RESULTS_")) {
                status.setStatusCode(JobStatusCode.FINISHING);
                status.clearStatusMessage();
            }
        }

        return status.build();
    }
}
