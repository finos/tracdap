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

import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.api.TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub;
import org.finos.tracdap.common.auth.GrpcClientAuth;
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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.finos.tracdap.common.metadata.MetadataConstants.TRAC_JOB_STATUS_ATTR;


public class JobProcessor {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final TrustedMetadataApiBlockingStub metaClient;
    private final IBatchExecutor<?> executor;
    private final Validator validator = new Validator();

    // TODO: Refactor into this class
    private final JobLifecycle lifecycle;


    public JobProcessor(TrustedMetadataApiBlockingStub metaClient, IBatchExecutor<?> executor, JobLifecycle lifecycle) {

        this.metaClient = metaClient;
        this.executor = executor;
        this.lifecycle = lifecycle;
    }


    // Metadata actions

    public JobState assembleAndValidate(JobState jobState) {

        var newState = jobState.clone();

        newState = lifecycle.assembleAndValidate(newState);

        newState.tracStatus = JobStatusCode.VALIDATED;

        return newState;
    }

    public JobState saveInitialMetadata(JobState jobState) {

        var newState = jobState.clone();

        newState = lifecycle.saveInitialMetadata(newState);

        newState.tracStatus = JobStatusCode.QUEUED;
        newState.cacheStatus = CacheStatus.QUEUED_IN_TRAC;

        return newState;
    }

    public JobState updateMetadata(JobState jobState) {

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(jobState.tenant)
                .setObjectType(ObjectType.JOB)
                .setPriorVersion(MetadataUtil.selectorFor(jobState.jobId))
                .addTagUpdates(TagUpdate.newBuilder()
                .setAttrName(TRAC_JOB_STATUS_ATTR)
                .setValue(MetadataCodec.encodeValue(jobState.tracStatus.name())))
                .build();

        var userAuth = GrpcClientAuth.applyIfAvailable(metaClient, jobState.ownerToken);
        var newId = userAuth.updateTag(writeRequest);

        var newState = jobState.clone();
        newState.jobId = newId;

        return newState;
    }

    public JobState saveResultMetadata(JobState jobState) {

        // TRAC job status must already be set before calling lifecycle

        lifecycle.processJobResult(jobState);

        var newState = jobState.clone();
        newState.cacheStatus = CacheStatus.RESULTS_SAVED;

        return newState;
    }


    // Executor actions

    public JobState launchJob(JobState jobState) {

        // TODO: Use a submission ID to avoid clash on repeat

        var jobKey =  jobState.jobKey;

        log.info("LAUNCH JOB: [{}]", jobKey);

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

        var newState = jobState.clone();
        newState.tracStatus = JobStatusCode.SUBMITTED;
        newState.cacheStatus = CacheStatus.SENT_TO_EXECUTOR;
        newState.batchStatus = ExecutorJobStatus.STATUS_UNKNOWN;
        newState.batchState = batchState.toByteArray();

        log.info("LAUNCH JOB SUCCEEDED: [{}]", jobKey);

        return newState;
    }

    public JobState recordPollStatus(JobState jobState, ExecutorJobInfo batchInfo) {

        var newState = jobState.clone();
        newState.batchStatus = batchInfo.getStatus();

        log.info("EXECUTOR STATE UPDATE: [{}] {}", newState.jobKey, batchInfo.getStatus());

        switch (batchInfo.getStatus()) {


            // Change to SUBMITTED / RUNNING state is significant, send update to metadata service

            case QUEUED:
                newState.tracStatus = JobStatusCode.PENDING;  // QUEUED_IN_EXECUTOR todo
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

        var jobKey = jobState.jobKey;

        log.info("FETCH JOB RESULT: [{}]", jobKey);

        var batchExecutor = stronglyTypedExecutor();
        var batchState = stronglyTypedState(batchExecutor, jobState.batchState);

        try {

            var resultFile = String.format("job_result_%s.json", jobKey);
            var resultBytes = batchExecutor.readFile(jobKey, batchState, "result", resultFile);

            var results = ConfigParser.parseConfig(resultBytes, ConfigFormat.JSON, JobResult.class);

            // If the validator is extended to cover the config interface,
            // The top level job result could be validated directly

            for (var result : results.getResultsMap().entrySet()) {

                log.info("Validating job result [{}]", result.getKey());
                validator.validateFixedObject(result.getValue());
            }

            var newState = jobState.clone();
            newState.jobResult = results;
            newState.tracStatus = JobStatusCode.SUCCEEDED;
            newState.cacheStatus = CacheStatus.RESULTS_RECEIVED;

            log.info("FETCH JOB RESULT SUCCEEDED: [{}]", jobKey);

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

            log.error("FETCH JOB RESULT FAILED: [{}] {}", jobKey, shortMessage, e);

            return newState;
        }
    }

    public JobState cleanUpJob(JobState jobState) {

        log.info("CLEAN UP JOB: [{}]", jobState.jobKey);

        var batchExecutor = stronglyTypedExecutor();
        var batchState = stronglyTypedState(batchExecutor, jobState.batchState);

        batchExecutor.destroyBatch(jobState.jobKey, batchState);

        var newState = jobState.clone();
        newState.cacheStatus = CacheStatus.READY_TO_REMOVE;
        newState.batchStatus = ExecutorJobStatus.STATUS_UNKNOWN;
        newState.batchState = null;

        log.info("CLEAN UP JOB SUCCEEDED: [{}]", jobState.jobKey);

        return newState;
    }

    // Executor polling

    public List<ExecutorJobInfo> pollExecutorJobs(List<Map.Entry<String, JobState>> jobs) {

        var executor = stronglyTypedExecutor();

        var jobState = jobs.stream()
                .map(j -> Map.entry(j.getKey(), stronglyTypedState(executor, j.getValue().batchState)))
                .collect(Collectors.toList());

        return executor.pollBatches(jobState);
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
