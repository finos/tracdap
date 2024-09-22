/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.exec;

import org.finos.tracdap.api.internal.*;
import org.finos.tracdap.common.config.ConfigFormat;
import org.finos.tracdap.common.config.ConfigParser;
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.grpc.CompressionClientInterceptor;
import org.finos.tracdap.common.grpc.GrpcChannelFactory;
import org.finos.tracdap.common.grpc.LoggingClientInterceptor;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.RuntimeConfig;
import org.finos.tracdap.config.ServiceConfig;
import org.finos.tracdap.metadata.JobStatusCode;
import org.finos.tracdap.metadata.TagHeader;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.regex.Pattern;


public class BatchJobExecutor<TBatchState extends Serializable> implements IJobExecutor<BatchJobState<TBatchState>> {

    private static final short DEFAULT_RUNTIME_API_PORT = 9000;

    private static final Pattern TRAC_ERROR_LINE = Pattern.compile("tracdap.rt.exceptions.(E\\w+): (.+)");

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final IBatchExecutor<TBatchState> batchExecutor;

    private GrpcChannelFactory channelFactory;

    public BatchJobExecutor(IBatchExecutor<TBatchState> batchExecutor) {
        this.batchExecutor = batchExecutor;
    }

    @Override
    public void start(GrpcChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
        batchExecutor.start();
    }

    @Override
    public void stop() {
        batchExecutor.stop();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<BatchJobState<TBatchState>> stateClass() {
        return (Class<BatchJobState<TBatchState>>) (Object) BatchJobState.class;
    }

    @Override
    public BatchJobState<TBatchState> submitJob() {
        throw new ETracInternal("Not implemented yet");
    }

    @Override
    public BatchJobState<TBatchState> submitOneshotJob(TagHeader jobId, JobConfig jobConfig, RuntimeConfig sysConfig) {

        // If an error occurs before the batch state is created, then there is nothing to clean up

        var batchKey = MetadataUtil.objectKey(jobId);
        var batchState = batchExecutor.createBatch(batchKey);

        try {

            var batchSysConfig = sysConfig.toBuilder();

            var runtimeApiEnabled = batchExecutor.hasFeature(IBatchExecutor.Feature.EXPOSE_PORT);
            var storageMappingEnabled = batchExecutor.hasFeature(IBatchExecutor.Feature.STORAGE_MAPPING);
            var resultVolumeEnabled = batchExecutor.hasFeature(IBatchExecutor.Feature.OUTPUT_VOLUMES);
            var logVolumeEnabled = batchExecutor.hasFeature(IBatchExecutor.Feature.OUTPUT_VOLUMES);

            if (runtimeApiEnabled) {

                // TODO: Make runtime API port a config property
                var runtimeApiConfig = ServiceConfig.newBuilder()
                        .setEnabled(true)
                        .setPort(DEFAULT_RUNTIME_API_PORT)
                        .clearAlias()
                        .build();

                batchSysConfig.setRuntimeApi(runtimeApiConfig);
            }

            if (storageMappingEnabled) {

                batchState = batchExecutor.configureBatchStorage(
                        batchKey, batchState, sysConfig.getStorage(),
                        batchSysConfig::mergeStorage);
            }

            var jobConfigJson = ConfigParser.quoteConfig(jobConfig, ConfigFormat.JSON);
            var sysConfigJson = ConfigParser.quoteConfig(batchSysConfig.build(), ConfigFormat.JSON);

            batchState = batchExecutor.addVolume(batchKey, batchState, "config", BatchVolumeType.CONFIG_VOLUME);
            batchState = batchExecutor.addFile(batchKey, batchState, "config", "job_config.json", jobConfigJson);
            batchState = batchExecutor.addFile(batchKey, batchState, "config", "sys_config.json", sysConfigJson);

            batchState = batchExecutor.addVolume(batchKey, batchState, "scratch", BatchVolumeType.SCRATCH_VOLUME);

            var batchConfig = BatchConfig.forCommand(
                    LaunchCmd.trac(), List.of(
                            LaunchArg.string("--sys-config"), LaunchArg.path("config", "sys_config.json"),
                            LaunchArg.string("--job-config"), LaunchArg.path("config", "job_config.json"),
                            LaunchArg.string("--scratch-dir"), LaunchArg.path("scratch", ".")));

            if (resultVolumeEnabled) {
                batchState = batchExecutor.addVolume(batchKey, batchState, "result", BatchVolumeType.OUTPUT_VOLUME);
                batchConfig.addExtraArgs(List.of(
                        LaunchArg.string("--job-result-dir"), LaunchArg.path("result", "."),
                        LaunchArg.string("--job-result-format"), LaunchArg.string("json")));
            }

            if (logVolumeEnabled) {
                batchState = batchExecutor.addVolume(batchKey, batchState, "log", BatchVolumeType.OUTPUT_VOLUME);
                batchConfig.addLoggingRedirect(
                        LaunchArg.path("log", "trac_rt_stdout.log"),
                        LaunchArg.path("log", "trac_rt_stderr.log"));
            }

            batchState = batchExecutor.submitBatch(batchKey, batchState, batchConfig);

            var jobState = new BatchJobState<TBatchState>();
            jobState.batchKey = batchKey;
            jobState.batchState = batchState;
            jobState.runtimeApiEnabled = runtimeApiEnabled;
            jobState.resultVolumeEnabled = resultVolumeEnabled;
            jobState.logVolumeEnabled = logVolumeEnabled;

            return jobState;
        }
        catch (Exception submitError) {

            // If submit fails, an error is thrown and the orchestrator never gets the batch state
            // So, clean up an resources that were created on the way out
            // The batch executor should handle cleaning up partially created batches

            log.error("There was an error submitting the batch, attempting to clean up resources...");

            try {
                batchExecutor.deleteBatch(batchKey, batchState);
                log.info("Clean up was successful");
            }
            catch (Exception deleteError) {
                log.error("There was an error cleaning up the batch, some resources may not have been removed");
                log.error(deleteError.getMessage(), deleteError);
            }

            throw submitError;
        }
    }

    @Override
    public BatchJobState<TBatchState> submitExternalJob() {
        throw new ETracInternal("Not implemented yet");
    }

    @Override
    public BatchJobState<TBatchState> cancelJob(BatchJobState<TBatchState> jobState) {
        jobState.batchState = batchExecutor.cancelBatch(jobState.batchKey, jobState.batchState);
        return jobState;
    }

    @Override
    public void deleteJob(BatchJobState<TBatchState> jobState) {
        batchExecutor.deleteBatch(jobState.batchKey, jobState.batchState);
    }

    @Override
    public List<RuntimeJobStatus> listJobs() {
        throw new ETracInternal("Not implemented yet");
    }

    @Override
    public RuntimeJobStatus getJobStatus(BatchJobState<TBatchState> jobState) {

        var batchStatus = batchExecutor.getBatchStatus(jobState.batchKey, jobState.batchState);

        RuntimeJobStatus jobStatus;

        // Prefer using the API server to get the job status, if it is available
        if (jobState.runtimeApiEnabled && batchStatus.getStatusCode() == BatchStatusCode.RUNNING) {

            jobStatus = getStatusFromApi(jobState);
        }
        else {

            // Otherwise build job status based on the batch status
            var jobStatusBuilder = RuntimeJobStatus.newBuilder()
                    .setStatusCode(mapStatusCode(batchStatus.getStatusCode()))
                    .setStatusMessage(batchStatus.getStatusMessage());

            if (jobState.logVolumeEnabled && batchStatus.getStatusCode() == BatchStatusCode.FAILED)
                updateStatusFromLogs(jobState, jobStatusBuilder);

            jobStatus = jobStatusBuilder.build();
        }

        if (jobStatus.getStatusCode() == JobStatusCode.SUCCEEDED ||
            jobStatus.getStatusCode() == JobStatusCode.FINISHING) {

            log.info("Reporting job status for [{}]: {}", jobState.batchKey, jobStatus.getStatusCode());
        }
        else if (jobStatus.getStatusCode() == JobStatusCode.CANCELLED) {

            log.warn("Reporting job status for [{}]: {}", jobState.batchKey, jobStatus.getStatusCode());
        }
        else if (jobStatus.getStatusCode() == JobStatusCode.FAILED) {

            log.error("Reporting job status for [{}]: {}", jobState.batchKey, jobStatus.getStatusCode());
            log.error(jobStatus.getStatusMessage());

            if (!jobStatus.getErrorDetail().isBlank())
                log.error(jobStatus.getErrorDetail());
        }

        return jobStatus;
    }

    private RuntimeJobStatus getStatusFromApi(BatchJobState<TBatchState> jobState) {

        var runtimeApiAddress = getRuntimeApiAddress(jobState);
        var runtimeChannel = (ManagedChannel) null;

        try {

            runtimeChannel = channelFactory.createChannel(runtimeApiAddress);
            var runtimeApi = getRuntimeApi(runtimeChannel);

            var runtimeRequest = RuntimeJobInfoRequest.newBuilder()
                    .setJobKey(jobState.batchKey)
                    .build();

            return runtimeApi.getJobStatus(runtimeRequest);
        }
        catch (StatusRuntimeException grpcError) {

            log.error("Error getting job status for [{}]: {}", jobState.batchKey, grpcError.getMessage());
            throw mapRuntimeApiError(grpcError);
        }
        finally {
            if (runtimeChannel != null)
                runtimeChannel.shutdown();
        }
    }

    private void updateStatusFromLogs(BatchJobState<TBatchState> jobState, RuntimeJobStatus.Builder batchJobStatus) {

        // If a log file is available, fetch it and look for a TRAC exception message
        // If the error log is not available, fall back on the basic batch status
        // Typically this is a lot less useful (e.g. "Batch failed, exit code 5")!

        var logsAvailable = batchExecutor.hasOutputFile(
                jobState.batchKey, jobState.batchState,
                "log", "trac_rt_stderr.log");

        if (!logsAvailable) {
            log.warn("Runtime error log is not available");
            return;
        }

        var stdErrBytes = batchExecutor.getOutputFile(
                jobState.batchKey, jobState.batchState,
                "log", "trac_rt_stderr.log");

        var stdErrText = new String(stdErrBytes, StandardCharsets.UTF_8);
        var statusMessage = extractErrorFromLogs(stdErrText);

        if (statusMessage != null)
            batchJobStatus.setStatusMessage(statusMessage);

        batchJobStatus.setErrorDetail(stdErrText);
    }

    private String extractErrorFromLogs(String stdErrText) {

        var lastLineIndex = stdErrText.stripTrailing().lastIndexOf("\n");
        var lastLine = stdErrText.substring(lastLineIndex + 1).stripTrailing();

        var tracError = TRAC_ERROR_LINE.matcher(lastLine);

        if (tracError.matches()) {

            var exception = tracError.group(1);
            var message = tracError.group(2);

            // Errors are reported further up the stack, they can come from runtime API or other places
            log.debug("TRAC exception in runtime error log: [{}] {}", exception, message);

            return message;
        }
        else if (lastLine.isEmpty()) {

            log.warn("Runtime error log is empty");
            return null;
        }
        else {

            log.error("Runtime error log is not in the expected format");
            log.error(lastLine);
            return null;
        }
    }

    @Override
    public Flow.Publisher<RuntimeJobStatus> followJobStatus(BatchJobState<TBatchState> jobState) {
        throw new ETracInternal("Not implemented yet");
    }

    @Override
    public RuntimeJobResult getJobResult(BatchJobState<TBatchState> jobState) {

        var batchStatus = batchExecutor.getBatchStatus(jobState.batchKey, jobState.batchState);
        var batchCode = batchStatus.getStatusCode();

        if (jobState.runtimeApiEnabled && batchCode == BatchStatusCode.RUNNING)
            return getResultFromApi(jobState);

        if (jobState.resultVolumeEnabled && (batchCode == BatchStatusCode.COMPLETE || batchCode== BatchStatusCode.SUCCEEDED))
            return getResultFromResultFile(jobState);

        // Currently results are not expected to be available if the job has failed
        // In future we might give a job result for failed jobs using the error status info

        var message = String.format("Job result is not available for [%s] (this is a bug)", jobState.batchKey);
        log.error(message);
        throw new ETracInternal(message);
    }

    private RuntimeJobResult getResultFromApi(BatchJobState<TBatchState> jobState) {

        var runtimeApiAddress = getRuntimeApiAddress(jobState);
        var runtimeChannel = (ManagedChannel) null;

        try {

            runtimeChannel = channelFactory.createChannel(runtimeApiAddress);
            var runtimeApi = getRuntimeApi(runtimeChannel);

            var runtimeRequest = RuntimeJobInfoRequest.newBuilder()
                    .setJobKey(jobState.batchKey)
                    .build();

            return runtimeApi.getJobResult(runtimeRequest);
        }
        catch (StatusRuntimeException grpcError) {

            log.error("Error getting job result for [{}]: {}", jobState.batchKey, grpcError.getMessage());
            throw mapRuntimeApiError(grpcError);
        }
        finally {
            if (runtimeChannel != null)
                runtimeChannel.shutdown();
        }
    }

    private RuntimeJobResult getResultFromResultFile(BatchJobState<TBatchState> jobState) {

        var resultFile = String.format("job_result_%s.json", jobState.batchKey);

        try {

            var batchResultBytes = batchExecutor.getOutputFile(jobState.batchKey, jobState.batchState, "result", resultFile);
            var batchResult = ConfigParser.parseConfig(batchResultBytes, ConfigFormat.JSON, JobResult.class);

            return RuntimeJobResult.newBuilder()
                    .setJobId(batchResult.getJobId())
                    .setStatusCode(batchResult.getStatusCode())
                    .setStatusMessage(batchResult.getStatusMessage())
                    .putAllResults(batchResult.getResultsMap())
                    .build();
        }
        catch (EConfigParse parseError) {

            // Parsing and validation failures mean the job has definitely failed
            // Handle these as part of the result processing
            // These are not executor / communication errors and should not be retried

            var errorMessage = parseError.getMessage();
            var shortMessage = errorMessage.lines().findFirst().orElse("No details available");

            log.error("Invalid job result recorded in [{}]: {}", resultFile, shortMessage, parseError);

            throw new EExecutorFailure("Invalid job result: " + shortMessage, parseError);
        }
    }

    private InetSocketAddress getRuntimeApiAddress(BatchJobState<TBatchState> jobState) {

        var runtimeApiAddress = batchExecutor.getBatchAddress(jobState.batchKey, jobState.batchState);

        if (runtimeApiAddress == null) {

            var warning = String.format(
                    "Runtime API is not available for job [%s] (it should come up soon)",
                    jobState.batchKey);

            log.warn(warning);
            throw new EExecutorTemporaryFailure(warning);
        }

        return runtimeApiAddress;
    }

    private TracRuntimeApiGrpc.TracRuntimeApiBlockingStub getRuntimeApi(Channel clientChannel) {

        return TracRuntimeApiGrpc
                .newBlockingStub(clientChannel)
                .withCompression(CompressionClientInterceptor.COMPRESSION_TYPE)
                .withInterceptors(new CompressionClientInterceptor())
                .withInterceptors(new LoggingClientInterceptor(BatchJobExecutor.class));
    }

    private JobStatusCode mapStatusCode(BatchStatusCode batchStatusCode) {

        switch (batchStatusCode) {

            case QUEUED: return JobStatusCode.SUBMITTED;
            case RUNNING: return JobStatusCode.RUNNING;
            case COMPLETE: return JobStatusCode.FINISHING;
            case SUCCEEDED: return JobStatusCode.SUCCEEDED;
            case FAILED: return JobStatusCode.FAILED;
            case CANCELLED: return JobStatusCode.CANCELLED;

            case STATUS_UNKNOWN:
            default:
                return JobStatusCode.UNRECOGNIZED;
        }
    }

    private EExecutor mapRuntimeApiError(StatusRuntimeException grpcError) {

        switch (grpcError.getStatus().getCode()) {

            case UNAVAILABLE:
            case DEADLINE_EXCEEDED:
                throw new EExecutorTemporaryFailure(grpcError.getMessage(), grpcError);

            case UNAUTHENTICATED:
            case PERMISSION_DENIED:
                throw new EExecutorAccess(grpcError.getMessage(), grpcError);

            case INVALID_ARGUMENT:
            case FAILED_PRECONDITION:
                throw new EExecutorValidation(grpcError.getMessage(), grpcError);

            default:
                throw new EExecutorFailure(grpcError.getMessage(), grpcError);
        }
    }
}
