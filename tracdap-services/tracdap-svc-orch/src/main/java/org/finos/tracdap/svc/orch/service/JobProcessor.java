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
import org.finos.tracdap.api.TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub;
import org.finos.tracdap.common.auth.internal.AuthHelpers;
import org.finos.tracdap.common.auth.internal.InternalAuthProvider;
import org.finos.tracdap.common.config.ConfigFormat;
import org.finos.tracdap.common.config.ConfigParser;
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.exec.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.metadata.JobStatusCode;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.TagUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.finos.tracdap.common.metadata.MetadataConstants.TRAC_JOB_STATUS_ATTR;


public class JobProcessor {

    // These timeouts are to avoid granting unbounded delegate sessions
    // When job processing timeouts are added in JobManager, the session timeouts should reflect those

    private static final Duration JOB_SETUP_SESSION_TIMEOUT = Duration.of(1, ChronoUnit.MINUTES);
    private static final Duration JOB_EXECUTION_SESSION_TIMEOUT = Duration.of(24, ChronoUnit.HOURS);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final TrustedMetadataApiBlockingStub metaClient;
    private final InternalAuthProvider internalAuth;
    private final IBatchExecutor<?> executor;
    private final Validator validator = new Validator();

    // TODO: Refactor into this class
    private final JobProcessorHelpers lifecycle;


    public JobProcessor(
            TrustedMetadataApiBlockingStub metaClient,
            InternalAuthProvider internalAuth,
            IBatchExecutor<?> executor,
            JobProcessorHelpers lifecycle) {

        this.metaClient = metaClient;
        this.internalAuth = internalAuth;
        this.executor = executor;
        this.lifecycle = lifecycle;
    }

    public JobState newJob(JobRequest request) {

        var jobState = new JobState();
        jobState.tenant = request.getTenant();
        jobState.owner = AuthHelpers.currentUser();

        // Create a short-lived delegate auth session to set up the job
        jobState.credentials = internalAuth.createDelegateSession(jobState.owner, JOB_SETUP_SESSION_TIMEOUT);

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

        // Credentials token processor must be reset after deserializing jobState from the cache
        internalAuth.setTokenProcessor(newState.credentials);

        newState = lifecycle.applyTransform(newState);
        newState = lifecycle.loadResources(newState);
        newState = lifecycle.allocateResultIds(newState);
        newState = lifecycle.buildJobConfig(newState);

        // static validate
        // semantic validate

        newState.tracStatus = JobStatusCode.VALIDATED;

        return newState;
    }

    public JobState saveInitialMetadata(JobState jobState) {

        var newState = jobState.clone();

        // Credentials token processor must be reset after deserializing jobState from the cache
        internalAuth.setTokenProcessor(newState.credentials);

        newState = lifecycle.saveInitialMetadata(newState);

        newState.tracStatus = JobStatusCode.QUEUED;
        newState.cacheStatus = CacheStatus.QUEUED_IN_TRAC;

        return newState;
    }

    public JobState scheduleLaunch(JobState jobState) {

        var newState = jobState.clone();
        newState.tracStatus = JobStatusCode.PENDING;
        newState.cacheStatus = CacheStatus.LAUNCH_SCHEDULED;

        // Create a new, long-lived delegate auth session for job execution
        jobState.credentials = internalAuth.createDelegateSession(jobState.owner, JOB_EXECUTION_SESSION_TIMEOUT);

        return updateMetadata(newState);
    }

    private JobState updateMetadata(JobState jobState) {

        // Credentials token processor must be reset after deserializing jobState from the cache
        internalAuth.setTokenProcessor(jobState.credentials);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(jobState.tenant)
                .setObjectType(ObjectType.JOB)
                .setPriorVersion(MetadataUtil.selectorFor(jobState.jobId))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName(TRAC_JOB_STATUS_ATTR)
                        .setValue(MetadataCodec.encodeValue(jobState.tracStatus.name())))
                .build();

        var userAuth = metaClient.withCallCredentials(jobState.credentials);
        var newId = userAuth.updateTag(writeRequest);

        var newState = jobState.clone();
        newState.jobId = newId;

        return newState;
    }

    public JobState launchJob(JobState jobState) {

        // TODO: Use a submission ID to avoid clash on repeat

        var jobKey =  jobState.jobKey;

        var batchExecutor = stronglyTypedExecutor();
        var batchState = batchExecutor.createBatch(jobKey);

        batchState = batchExecutor.createVolume(jobKey, batchState, "config", ExecutorVolumeType.CONFIG_DIR);
        batchState = batchExecutor.createVolume(jobKey, batchState, "result", ExecutorVolumeType.RESULT_DIR);
        batchState = batchExecutor.createVolume(jobKey, batchState, "log", ExecutorVolumeType.RESULT_DIR);
        batchState = batchExecutor.createVolume(jobKey, batchState, "scratch", ExecutorVolumeType.SCRATCH_DIR);

        // No specialisation is needed to build the job config
        // This may change in the future, in which case add IJobLogic.buildJobConfig()

        var jobConfigJson = ConfigParser.quoteConfig(jobState.jobConfig, ConfigFormat.JSON);
        var sysConfigJson = ConfigParser.quoteConfig(jobState.sysConfig, ConfigFormat.JSON);
        batchState = batchExecutor.writeFile(jobKey, batchState, "config", "job_config.json", jobConfigJson);
        batchState = batchExecutor.writeFile(jobKey, batchState, "config", "sys_config.json", sysConfigJson);

        var launchCmd = LaunchCmd.trac();

        var launchArgs = List.of(
                LaunchArg.string("--sys-config"), LaunchArg.path("config", "sys_config.json"),
                LaunchArg.string("--job-config"), LaunchArg.path("config", "job_config.json"),
                LaunchArg.string("--job-result-dir"), LaunchArg.path("result", "."),
                LaunchArg.string("--job-result-format"), LaunchArg.string("json"),
                LaunchArg.string("--scratch-dir"), LaunchArg.path("scratch", "."));

        batchState = batchExecutor.startBatch(jobKey, batchState, launchCmd, launchArgs);

        log.info("Job has been sent to the executor: [{}]", jobKey);

        var newState = jobState.clone();
        newState.tracStatus = JobStatusCode.SUBMITTED;
        newState.cacheStatus = CacheStatus.SENT_TO_EXECUTOR;
        newState.batchStatus = ExecutorJobStatus.STATUS_UNKNOWN;
        newState.batchState = batchState.toByteArray();

        return newState;
    }

    public List<ExecutorJobInfo> pollExecutorJobs(List<Map.Entry<String, JobState>> jobs) {

        var executor = stronglyTypedExecutor();

        var jobState = jobs.stream()
                // Only poll jobs that have an executor state
                .filter(j -> j.getValue().batchState != null)
                .map(j -> Map.entry(j.getKey(), stronglyTypedState(executor, j.getValue().batchState)))
                .collect(Collectors.toList());

        return executor.pollBatches(jobState);
    }

    public JobState recordJobStatus(JobState jobState, ExecutorJobInfo batchInfo) {

        var newState = jobState.clone();
        newState.batchStatus = batchInfo.getStatus();

        log.info("Job status received from executor: [{}] {}", newState.jobKey, newState.batchStatus);

        switch (batchInfo.getStatus()) {

            // Change to SUBMITTED / RUNNING state is significant, send update to metadata service

            case QUEUED:
                newState.tracStatus = JobStatusCode.SUBMITTED;
                newState.cacheStatus = CacheStatus.QUEUED_IN_EXECUTOR;
                return updateMetadata(newState);

            case RUNNING:
                newState.tracStatus = JobStatusCode.RUNNING;
                newState.cacheStatus = CacheStatus.RUNNING_IN_EXECUTOR;
                return updateMetadata(newState);

            // Completed states will be reported when the job results are fully assembled
            // Do not send metadata updates here for finishing states

            case COMPLETE:
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
                newState.statusMessage = batchInfo.getStatusMessage();
                newState.errorDetail = batchInfo.getErrorDetail();

                log.error("Execution failed for [{}]: {}", newState.jobKey, batchInfo.getStatusMessage());
                log.error("Error detail for [{}]\n{}", newState.jobKey, batchInfo.getErrorDetail());

                return newState;

            // TODO: Handling for CANCELLED
            // Cancellation is not supported yet, but will need to be handled here when it is

            // If executor status could not be determined, treat this as a failure
            // This will cause the job to be reported as failed and trigger job cleanup

            // TODO: Can we allow STATUS_UNKNOWN to happen a few times before recording a failure?
            // E.g. to handle intermittent errors talking to the executor

            case STATUS_UNKNOWN:
            default:

                newState.tracStatus = JobStatusCode.FAILED;
                newState.cacheStatus = CacheStatus.EXECUTOR_FAILED;
                newState.statusMessage = "Job status could not be determined";

                return newState;
        }
    }

    public JobState fetchJobResult(JobState jobState) {

        var batchExecutor = stronglyTypedExecutor();
        var batchState = stronglyTypedState(batchExecutor, jobState.batchState);

        if (jobState.batchState == null) {

            log.info("Cannot fetch job result: [{}] Executor state is not available", jobState.jobKey);

            var newState = jobState.clone();
            newState.tracStatus = JobStatusCode.FAILED;
            newState.cacheStatus = CacheStatus.RESULTS_INVALID;
            newState.statusMessage = "executor state is not available";

            return newState;
        }

        try {

            var resultFile = String.format("job_result_%s.json", jobState.jobKey);
            var resultBytes = batchExecutor.readFile(jobState.jobKey, batchState, "result", resultFile);

            var results = ConfigParser.parseConfig(resultBytes, ConfigFormat.JSON, JobResult.class);

            // If the validator is extended to cover the config interface,
            // The top level job result could be validated directly

            for (var result : results.getResultsMap().entrySet()) {

                log.info("Validating job result: [{}] item [{}]", jobState.jobKey, result.getKey());
                validator.validateFixedObject(result.getValue());
            }

            var newState = jobState.clone();
            newState.jobResult = results;
            newState.tracStatus = JobStatusCode.SUCCEEDED;
            newState.cacheStatus = CacheStatus.RESULTS_RECEIVED;

            return newState;
        }
        catch (EConfigParse | EValidation e) {

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

        // Credentials token processor must be reset after deserializing jobState from the cache
        internalAuth.setTokenProcessor(jobState.credentials);

        // TRAC job status must already be set before calling lifecycle

        lifecycle.processJobResult(jobState);

        var newState = jobState.clone();
        newState.cacheStatus = CacheStatus.RESULTS_SAVED;

        return newState;
    }

    public JobState cleanUpJob(JobState jobState) {

        if (jobState.batchState != null) {

            var batchExecutor = stronglyTypedExecutor();
            var batchState = stronglyTypedState(batchExecutor, jobState.batchState);

            batchExecutor.destroyBatch(jobState.jobKey, batchState);

            var newState = jobState.clone();
            newState.cacheStatus = CacheStatus.READY_TO_REMOVE;
            newState.batchStatus = ExecutorJobStatus.STATUS_UNKNOWN;
            newState.batchState = null;

            return newState;
        }
        else {

            var newState = jobState.clone();
            newState.cacheStatus = CacheStatus.READY_TO_REMOVE;
            newState.batchStatus = ExecutorJobStatus.STATUS_UNKNOWN;

            log.warn("Job could not be cleaned up: [{}] Executor state is not available", jobState.jobKey);
            log.warn("There may be an orphaned task in the executor");

            return newState;
        }
    }

    public JobState scheduleRemoval(JobState jobState) {

        var newState = jobState.clone();
        newState.cacheStatus = CacheStatus.REMOVAL_SCHEDULED;

        return newState;
    }

    public JobState handleProcessingFailed(JobState jobState, String errorMessage, Exception exception) {

        var newState = jobState.clone();
        newState.tracStatus = JobStatusCode.FAILED;
        newState.statusMessage = errorMessage;
        newState.exception = exception;

        internalAuth.setTokenProcessor(newState.credentials);
        lifecycle.processJobResult(newState);

        newState.cacheStatus = CacheStatus.READY_TO_REMOVE;

        return newState;
    }

    @SuppressWarnings("unchecked")
    private <TState extends Message>
    IBatchExecutor<TState> stronglyTypedExecutor() {
        return (IBatchExecutor<TState>) executor;
    }

    private <TState extends Message>
    TState stronglyTypedState(IBatchExecutor<TState> batchExecutor, byte[] stateBytes) {
        try {
            return batchExecutor.stateDecoder().parseFrom(stateBytes);
        }
        catch (InvalidProtocolBufferException e) {
            throw new ETracInternal("Invalid job state: " + e.getMessage(), e);
        }
    }
}
