/*
 * Copyright 2023 Accenture Global Solutions Limited
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

import org.finos.tracdap.api.JobRequest;
import org.finos.tracdap.api.JobStatus;
import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.api.internal.RuntimeJobStatus;
import org.finos.tracdap.api.internal.TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub;
import org.finos.tracdap.common.auth.internal.AuthHelpers;
import org.finos.tracdap.common.auth.internal.InternalAuthProvider;
import org.finos.tracdap.common.cache.CacheEntry;
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.exec.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.metadata.JobStatusCode;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.TagUpdate;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.finos.tracdap.common.metadata.MetadataConstants.TRAC_JOB_STATUS_ATTR;


public class JobProcessor {

    // Timeout for delegate sessions used to record metadata updates
    // We regenerate these for each operation, e.g. reporting failures after a job hang we don't want the token to expire
    // As a result the delegate token can be short-lived

    private static final Duration DELEGATE_SESSION_TIMEOUT = Duration.of(5, ChronoUnit.MINUTES);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PlatformConfig platformConfig;
    private final TrustedMetadataApiBlockingStub metaClient;
    private final InternalAuthProvider internalAuth;
    private final IJobExecutor<?> executor;
    private final Validator validator = new Validator();

    // TODO: Refactor into this class
    private final JobProcessorHelpers lifecycle;


    public JobProcessor(
            PlatformConfig platformConfig,
            TrustedMetadataApiBlockingStub metaClient,
            InternalAuthProvider internalAuth,
            IJobExecutor<?> jobExecutor) {

        this.platformConfig = platformConfig;
        this.metaClient = metaClient;
        this.internalAuth = internalAuth;
        this.executor = jobExecutor;

        this.lifecycle = new JobProcessorHelpers(platformConfig, metaClient);
    }

    public JobState newJob(JobRequest request) {

        var jobState = new JobState();
        jobState.tenant = request.getTenant();
        jobState.owner = AuthHelpers.currentUser();

        jobState.jobType = request.getJob().getJobType();
        jobState.definition = request.getJob();
        jobState.jobRequest = request;

        jobState.tracStatus = JobStatusCode.PREPARING;

        return jobState;
    }

    public JobStatus getStatus(JobState jobState) {

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

    public JobState assembleAndValidate(JobState jobState) {

        var newState = jobState.clone();

        try {

            // Credentials are not serialized in the cache, they need to be regenerated
            newState.credentials = internalAuth.createDelegateSession(jobState.owner, DELEGATE_SESSION_TIMEOUT);

            // Load in all the resources referenced by the job
            newState = lifecycle.loadResources(newState);

            // Semantic validation (job consistency)
            var metadata = new MetadataBundle(newState.resources, newState.resourceMapping);
            validator.validateConsistency(newState.definition, metadata, platformConfig);

            newState.tracStatus = JobStatusCode.VALIDATED;

            return newState;
        }
        catch (StatusRuntimeException e) {

            // Special handling for NOT_FOUND errors while querying the metadata service
            // Treat these as consistency validation errors
            // A better solution would be to allow partial response for batch load and pass to the validator
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND)
                throw new EConsistencyValidation("One or more items used in this job could not be found");

            throw e;
        }
    }

    public JobState saveInitialMetadata(JobState jobState) {

        var newState = jobState.clone();

        // Credentials are not serialized in the cache, they need to be regenerated
        newState.credentials = internalAuth.createDelegateSession(jobState.owner, DELEGATE_SESSION_TIMEOUT);

        // Apply any transformations specific to the job type
        newState = lifecycle.applyTransform(newState);

        // Result IDs are needed in order to generate the job instruction
        // They are also updated in the job definition that is being created
        newState = lifecycle.allocateResultIds(newState);

        // Create the job instruction - this is what will go to the executor
        newState = lifecycle.buildJobConfig(newState);

        // The job definition is ready - write it to the metadata store
        newState = lifecycle.saveInitialMetadata(newState);

        newState.tracStatus = JobStatusCode.QUEUED;
        newState.cacheStatus = CacheStatus.QUEUED_IN_TRAC;

        return newState;
    }

    public JobState scheduleLaunch(JobState jobState) {

        var newState = jobState.clone();
        newState.tracStatus = JobStatusCode.PREPARING;
        newState.cacheStatus = CacheStatus.LAUNCH_SCHEDULED;

        return updateMetadata(newState);
    }

    private JobState updateMetadata(JobState jobState) {

        var newState = jobState.clone();

        // Credentials are not serialized in the cache, they need to be regenerated
        newState.credentials = internalAuth.createDelegateSession(jobState.owner, DELEGATE_SESSION_TIMEOUT);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(jobState.tenant)
                .setObjectType(ObjectType.JOB)
                .setPriorVersion(MetadataUtil.selectorFor(jobState.jobId))  // prior version selector
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName(TRAC_JOB_STATUS_ATTR)
                        .setValue(MetadataCodec.encodeValue(jobState.tracStatus.name())))
                .build();

        var userAuth = metaClient.withCallCredentials(newState.credentials);
        newState.jobId = userAuth.updateTag(writeRequest);

        return newState;
    }

    public JobState launchJob(JobState jobState) {

        // TODO: Use a submission ID to avoid clash on repeat

        var jobExecutor = stronglyTypedExecutor();

        // All jobs are submitted as one-shot for now
        var executorState = jobExecutor.submitOneshotJob(
                jobState.jobId,
                jobState.jobConfig,
                jobState.sysConfig);

        log.info("Job has been sent to the executor: [{}]", jobState.jobKey);

        var newState = jobState.clone();
        newState.tracStatus = JobStatusCode.SUBMITTED;
        newState.cacheStatus = CacheStatus.SENT_TO_EXECUTOR;
        newState.executorState = executorState;
        newState.executorStatus = null;
        newState.executorResult = null;

        return newState;
    }

    public List<RuntimeJobStatus> pollExecutorJobs(List<CacheEntry<JobState>> jobs) {

        // TODO: Handle errors decoding the executor state, in stronglyTypeState()
        // This could happen if e.g. the platform configuration is changed with a new executor
        // Probably the job should be failed and removed from the cache

        // TODO: Use listJobs() to avoid polling jobs individually
        // Requires support from all executors, or use a feature flag

        var jobStatusList = new ArrayList<RuntimeJobStatus>(jobs.size());

        var jobExecutor = stronglyTypedExecutor();

        for (var job : jobs) {

            var jobState = job.value();

            if (jobState.executorState != null) {

                // TODO: Errors can relate to the executor or individual jobs
                // For individual job errors, jobs should be aborted after a number of retries

                var executorState = stronglyTypedState(jobExecutor, jobState.executorState);
                var jobStatus = jobExecutor.getJobStatus(executorState);

                jobStatusList.add(jobStatus);
            }
            else {

                var unknownStatus = RuntimeJobStatus.newBuilder()
                        .setStatusCode(JobStatusCode.JOB_STATUS_CODE_NOT_SET)
                        .build();

                jobStatusList.add(unknownStatus);
            }
        }

        return jobStatusList;
    }

    public JobState recordJobStatus(JobState jobState, RuntimeJobStatus executorStatus) {

        var newState = jobState.clone();
        newState.executorStatus = executorStatus;

        log.info("Job status received from executor: [{}]", newState.jobKey);

        switch (executorStatus.getStatusCode()) {

            // Initial state, executor has not updated

            case PENDING:
                return jobState;

            // Change to SUBMITTED / RUNNING state is significant, send update to metadata service

            case SUBMITTED:
            case VALIDATED:
            case QUEUED:

                newState.tracStatus = JobStatusCode.SUBMITTED;
                newState.cacheStatus = CacheStatus.QUEUED_IN_EXECUTOR;
                return updateMetadata(newState);

            case PREPARING:
            case RUNNING:
                newState.tracStatus = JobStatusCode.RUNNING;
                newState.cacheStatus = CacheStatus.RUNNING_IN_EXECUTOR;
                return updateMetadata(newState);

            // Completed states will be reported when the job results are fully assembled
            // Do not send metadata updates here for finishing states

            case FINISHING:
                newState.tracStatus = JobStatusCode.FINISHING;
                newState.cacheStatus = CacheStatus.EXECUTOR_COMPLETE;
                return newState;

            case SUCCEEDED:
                newState.tracStatus = JobStatusCode.FINISHING;
                newState.cacheStatus = CacheStatus.EXECUTOR_SUCCEEDED;
                return newState;

            case FAILED:
                newState.tracStatus = JobStatusCode.FAILED;
                newState.cacheStatus = CacheStatus.EXECUTOR_FAILED;
                newState.statusMessage = executorStatus.getStatusMessage();
                newState.errorDetail = executorStatus.getErrorDetail();

                log.error("Execution failed for [{}]: {}", newState.jobKey, executorStatus.getStatusMessage());
                log.error("Error detail for [{}]\n{}", newState.jobKey, executorStatus.getErrorDetail());

                return newState;

            // TODO: Handling for CANCELLED
            // Cancellation is not supported yet, but will need to be handled here when it is

            // If executor status could not be determined, treat this as a failure
            // This will cause the job to be reported as failed and trigger job cleanup

            // TODO: Can we allow STATUS_UNKNOWN to happen a few times before recording a failure?
            // E.g. to handle intermittent errors talking to the executor

            case JOB_STATUS_CODE_NOT_SET:
            case UNRECOGNIZED:
            default:

                newState.tracStatus = JobStatusCode.FAILED;
                newState.cacheStatus = CacheStatus.EXECUTOR_FAILED;
                newState.statusMessage = "Job status could not be determined";

                return newState;
        }
    }

    public JobState fetchJobResult(JobState jobState) {

        // TODO: Handle errors decoding the executor state, in stronglyTypeState()
        // This could happen if e.g. the platform configuration is changed with a new executor
        // Probably the job should be failed and removed from the cache

        var jobExecutor = stronglyTypedExecutor();
        var executorState = stronglyTypedState(jobExecutor, jobState.executorState);

        if (jobState.executorState == null) {

            log.info("Cannot fetch job result: [{}] Executor state is not available", jobState.jobKey);

            var newState = jobState.clone();
            newState.tracStatus = JobStatusCode.FAILED;
            newState.cacheStatus = CacheStatus.RESULTS_INVALID;
            newState.statusMessage = "executor state is not available";

            return newState;
        }

        try {

            var executorResult = jobExecutor.getJobResult(executorState);

            // If the validator is extended to cover the config interface,
            // The top level job result could be validated directly

            for (var result : executorResult.getResultsMap().entrySet()) {

                log.info("Validating job result: [{}] item [{}]", jobState.jobKey, result.getKey());
                validator.validateFixedObject(result.getValue());
            }

            var newState = jobState.clone();
            newState.executorResult = executorResult;
            newState.tracStatus = JobStatusCode.SUCCEEDED;
            newState.cacheStatus = CacheStatus.RESULTS_RECEIVED;

            return newState;
        }
        catch (EValidation e) {

            // Parsing and validation failures mean the job has definitely failed
            // Handle these as part of the result processing
            // These are not executor / communication errors and should not be retried

            var errorMessage = e.getMessage();
            var shortMessage = errorMessage.lines().findFirst().orElse("No details available");

            var newState = jobState.clone();
            newState.tracStatus = JobStatusCode.FAILED;
            newState.cacheStatus = CacheStatus.RESULTS_INVALID;
            newState.statusMessage = shortMessage;

            log.error("Failed to decode job result: [{}] {}", jobState.jobKey, shortMessage, e);

            return newState;
        }
    }

    public JobState saveResultMetadata(JobState jobState) {

        var newState = jobState.clone();

        // Credentials are not serialized in the cache, they need to be regenerated
        newState.credentials = internalAuth.createDelegateSession(jobState.owner, DELEGATE_SESSION_TIMEOUT);

        // TRAC job status must already be set before calling lifecycle

        lifecycle.processJobResult(newState);

        newState.cacheStatus = CacheStatus.RESULTS_SAVED;

        return newState;
    }

    public JobState cleanUpJob(JobState jobState) {

        // TODO: Handle errors decoding the executor state, in stronglyTypeState()
        // This could happen if e.g. the platform configuration is changed with a new executor
        // Probably the job should be failed and removed from the cache

        if (jobState.executorState != null) {

            var jobExecutor = stronglyTypedExecutor();
            var executorState = stronglyTypedState(jobExecutor, jobState.executorState);

            jobExecutor.deleteJob(executorState);
        }
        else {

            log.warn("Job could not be cleaned up: [{}] Executor state is not available", jobState.jobKey);
            log.warn("There may be an orphaned task in the executor");
        }

        var newState = jobState.clone();
        newState.cacheStatus = CacheStatus.READY_TO_REMOVE;
        newState.executorState = null;
        newState.executorStatus = null;
        newState.executorResult = null;

        return newState;
    }

    public JobState scheduleRemoval(JobState jobState) {

        var newState = jobState.clone();
        newState.cacheStatus = CacheStatus.REMOVAL_SCHEDULED;

        return newState;
    }

    public JobState handleProcessingFailed(JobState jobState, String errorMessage) {

        var newState = jobState.clone();
        newState.tracStatus = JobStatusCode.FAILED;
        newState.statusMessage = errorMessage;

        // Credentials are not serialized in the cache, they need to be regenerated
        newState.credentials = internalAuth.createDelegateSession(jobState.owner, DELEGATE_SESSION_TIMEOUT);

        lifecycle.processJobResult(newState);

        newState.cacheStatus = CacheStatus.READY_TO_REMOVE;

        return newState;
    }

    @SuppressWarnings("unchecked")
    private <TState extends Serializable>
    IJobExecutor<TState> stronglyTypedExecutor() {
        return (IJobExecutor<TState>) executor;
    }

    @SuppressWarnings("unchecked")
    private <TState extends Serializable>
    TState stronglyTypedState(IJobExecutor<TState> executor, Object executorState) {

        var stateClass = executor.stateClass();

        if (executorState == null)
            throw new ETracInternal("Invalid job state: null");

        if (!stateClass.isInstance(executorState)) {

            String message = String.format(
                    "Invalid job state: Expected [%s], got [%s]",
                    stateClass.getName(), executorState.getClass().getName());

            throw new ETracInternal(message);
        }

        return (TState) executorState;
    }
}
