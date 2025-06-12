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
import org.finos.tracdap.api.internal.RuntimeJobStatus;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc.InternalMetadataApiBlockingStub;
import org.finos.tracdap.common.cache.CacheEntry;
import org.finos.tracdap.common.config.ConfigFormat;
import org.finos.tracdap.common.config.ConfigParser;
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.middleware.GrpcClientState;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.common.service.TenantConfigManager;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.metadata.JobStatusCode;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.TagUpdate;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.finos.tracdap.common.metadata.MetadataConstants.TRAC_JOB_STATUS_ATTR;


public class JobProcessor {

    public static final String TRAC_RESULTS = "trac_results";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final TenantConfigManager tenantState;
    private final GrpcConcern commonConcerns;

    private final InternalMetadataApiBlockingStub metaClient;
    private final TracStorageApiGrpc.TracStorageApiBlockingStub storageClient;
    private final IJobExecutor<?> executor;

    private final ConfigParser configParser = new ConfigParser();
    private final Validator validator = new Validator();

    // TODO: Refactor into this class
    private final JobProcessorHelpers lifecycle;


    public JobProcessor(
            TenantConfigManager tenantState,
            GrpcConcern commonConcerns,
            PluginRegistry registry) {

        this.tenantState = tenantState;
        this.commonConcerns = commonConcerns;

        this.metaClient = registry.getSingleton(InternalMetadataApiBlockingStub.class);
        this.storageClient = registry.getSingleton(TracStorageApiGrpc.TracStorageApiBlockingStub.class);
        this.executor = registry.getSingleton(JobExecutor.class);

        this.lifecycle = new JobProcessorHelpers(tenantState, commonConcerns, registry);
    }

    public JobState newJob(JobRequest request, GrpcClientState clientState) {

        var jobState = new JobState();

        jobState.tenant = request.getTenant();
        jobState.jobRequest = request;
        jobState.jobType = request.getJob().getJobType();
        jobState.definition = request.getJob();

        jobState.clientState = clientState;
        jobState.requestMetadata = RequestMetadata.get(Context.current());
        jobState.userMetadata = UserMetadata.get(Context.current());

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

            var tenantConfig = tenantState.getTenantConfig(jobState.tenant);

            // Load in all the resources referenced by the job
            newState = lifecycle.loadMetadata(newState);

            // Semantic validation (job consistency)
            var metadata = new MetadataBundle(newState.objectMapping, newState.objects, newState.tags);
            validator.validateConsistency(newState.definition, metadata, tenantConfig);

            // Apply any transformations specific to the job type
            // Including this step during validation will catch more errors before jobs are submitted
            newState = lifecycle.applyTransform(newState);

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

        // The job definition was prepared in assembleAndValidate() - write it to the metadata store
        newState = lifecycle.saveJobDefinition(newState);

        // Preallocate IDs that will be used by the runtime for output objects
        newState = lifecycle.preallocateObjectIds(newState);

        // Create the job instruction - this is what will go to the executor
        newState = lifecycle.buildJobConfig(newState);

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

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(jobState.tenant)
                .setObjectType(ObjectType.JOB)
                .setPriorVersion(MetadataUtil.selectorFor(jobState.jobId))  // prior version selector
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName(TRAC_JOB_STATUS_ATTR)
                        .setValue(MetadataCodec.encodeValue(jobState.tracStatus.name())))
                .build();

        var userAuth = lifecycle.configureClient(metaClient, jobState);
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
        newState.runtimeStatus = null;
        newState.runtimeResult = null;

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

    public JobState recordJobStatus(JobState jobState, RuntimeJobStatus runtimeStatus) {

        var newState = jobState.clone();
        newState.runtimeStatus = runtimeStatus;

        log.info("Job status received from executor: [{}]", newState.jobKey);

        switch (runtimeStatus.getStatusCode()) {

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
                newState.statusMessage = runtimeStatus.getStatusMessage();
                newState.errorDetail = runtimeStatus.getErrorDetail();

                log.error("Execution failed for [{}]: {}", newState.jobKey, runtimeStatus.getStatusMessage());
                log.error("Error detail for [{}]\n{}", newState.jobKey, runtimeStatus.getErrorDetail());

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

        try {

            var clientState = commonConcerns.prepareClientCall(Context.ROOT);
            var client = clientState.configureClient(storageClient);

            var request = StorageReadRequest.newBuilder()
                    .setTenant(jobState.tenant)
                    .setStorageKey(TRAC_RESULTS)
                    .setStoragePath(jobState.jobResultPath)
                    .build();

            var jobResultFile = client.readSmallFile(request);
            var jobResult = configParser.parseConfig(jobResultFile.getContent().toByteArray(), ConfigFormat.JSON, JobResult.class);

            validator.validateFixedObject(jobResult);

            var newState = jobState.clone();
            newState.runtimeResult = jobResult;
            newState.tracStatus = JobStatusCode.FINISHING;
            newState.cacheStatus = CacheStatus.RESULTS_RECEIVED;

            return newState;
        }
        catch (ETrac e) {

            // Failed to retrieve or decode the result from the runtime
            // Store the error state and handle as part of the result processing

            // TODO: It may be possible to retry for some error conditions

            var shortMessage = e instanceof ETracPublic
                    ? e.getMessage().lines().findFirst().orElse("No details available")
                    : "No details available";

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

        lifecycle.processJobResult(newState);
        lifecycle.saveJobResult(newState);

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
        newState.runtimeStatus = null;
        newState.runtimeResult = null;

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

        lifecycle.processJobResult(newState);
        lifecycle.saveJobResult(newState);

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
