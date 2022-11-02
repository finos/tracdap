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

import org.finos.tracdap.common.config.ConfigFormat;
import org.finos.tracdap.common.config.ConfigParser;
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.exec.*;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.config.*;
import org.finos.tracdap.metadata.JobStatusCode;

import org.finos.tracdap.svc.orch.cache.IJobCache;
import org.finos.tracdap.svc.orch.cache.JobState;

import org.finos.tracdap.svc.orch.cache.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;


public class JobManagementService {

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);
    private static final Duration RETAIN_COMPLETE_DELAY = Duration.ofSeconds(60);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JobLifecycle jobLifecycle;
    private final IJobCache jobCache;
    private final IBatchExecutor jobExecutor;
    private final ScheduledExecutorService executorService;

    private ScheduledFuture<?> pollingTask;

    private final Validator validator = new Validator();

    public JobManagementService(
            JobLifecycle jobLifecycle,
            IJobCache jobCache,
            IBatchExecutor jobExecutor,
            ScheduledExecutorService executorService) {

        this.jobLifecycle = jobLifecycle;
        this.jobCache = jobCache;
        this.jobExecutor = jobExecutor;
        this.executorService = executorService;
    }

    public void start() {

        try {

            log.info("Starting job monitor service...");

            pollingTask = executorService.scheduleAtFixedRate(
                    this::poll,
                    POLL_INTERVAL.getSeconds(),
                    POLL_INTERVAL.getSeconds(),
                    TimeUnit.SECONDS);

            log.info("Job monitor service started OK");
        }
        catch (RejectedExecutionException e) {

            log.error("Job monitor service failed to start: {}", e.getMessage(), e);
            throw new EStartup("Job monitor service failed to start", e);
        }
    }

    public void stop() {

        log.info("Stopping job monitor service...");

        pollingTask.cancel(false);
    }

    public void poll() {

        pollExecutor();
        pollJobCache();
    }

    public void pollJobCache() {

        try {

            var finishedStates = List.of(JobStatusCode.FINISHING, JobStatusCode.CANCELLED);
            var finishedJobs = jobCache.pollJobs(job -> finishedStates.contains(job.statusCode));
            var queuedJobs = jobCache.pollJobs(job -> job.statusCode == JobStatusCode.QUEUED);

            for (var job : finishedJobs)

                executorService.schedule(
                        () -> jobOperation(job.jobKey, this::recordJobResult),
                        0L, TimeUnit.SECONDS);

            for (var job : queuedJobs)

                executorService.schedule(
                        () -> jobOperation(job.jobKey, this::submitJob),
                        0L, TimeUnit.SECONDS);
        }
        catch (Exception e) {

            log.error("Unexpected error polling job cache: {}", e.getMessage(), e);
        }
    }

    public void pollExecutor() {

        try  {

            var activeStates = List.of(JobStatusCode.SUBMITTED, JobStatusCode.RUNNING);
            var activeJobs = jobCache.pollJobs(job -> activeStates.contains(job.statusCode));

            var execStateMap = new HashMap<String, ExecutorState>();

            for (var job : activeJobs) {
                var executorState = JobState.deserialize(job.executorState, ExecutorState.class);
                execStateMap.put(job.jobKey, executorState);
            }

            var pollResults = jobExecutor.pollAllBatches(execStateMap);

            for (var result : pollResults) {

                log.info("Got state update for job: {}", result.jobKey);

                jobOperation(result.jobKey, (key, state, ticket) -> {

                    state.executorState = JobState.serialize(result.executorState);
                    state.statusCode = JobStatusCode.FINISHING;

                    jobCache.updateJob(key, state, ticket);
                });
            }
        }
        catch (Exception e) {

            log.error("Unexpected error polling executor: {}", e.getMessage(), e);
        }
    }

    private void submitJob(String jobKey, JobState jobState, Ticket ticket) {

        var execState = jobExecutor.createBatch(jobKey);
        execState = jobExecutor.createVolume(execState, "config", ExecutorVolumeType.CONFIG_DIR);
        execState = jobExecutor.createVolume(execState, "result", ExecutorVolumeType.RESULT_DIR);
        execState = jobExecutor.createVolume(execState, "log", ExecutorVolumeType.RESULT_DIR);
        execState = jobExecutor.createVolume(execState, "scratch", ExecutorVolumeType.SCRATCH_DIR);

        // No specialisation is needed to build the job config
        // This may change in the future, in which case add IJobLogic.buildJobConfig()

        var jobConfigJson = ConfigParser.quoteConfig(jobState.jobConfig, ConfigFormat.JSON);
        var sysConfigJson = ConfigParser.quoteConfig(jobState.sysConfig, ConfigFormat.JSON);
        execState = jobExecutor.writeFile(execState, "config", "job_config.json", jobConfigJson);
        execState = jobExecutor.writeFile(execState, "config", "sys_config.json", sysConfigJson);

        var launchCmd = LaunchCmd.trac();

        var launchArgs = List.of(
                LaunchArg.string("--sys-config"), LaunchArg.path("config", "sys_config.json"),
                LaunchArg.string("--job-config"), LaunchArg.path("config", "job_config.json"),
                LaunchArg.string("--job-result-dir"), LaunchArg.path("result", "."),
                LaunchArg.string("--job-result-format"), LaunchArg.string("json"),
                LaunchArg.string("--scratch-dir"), LaunchArg.path("scratch", "."));

        execState = jobExecutor.startBatch(execState, launchCmd, launchArgs);

        jobState.statusCode = JobStatusCode.SUBMITTED;
        jobState.executorState = JobState.serialize(execState);
        jobCache.updateJob(jobKey, jobState, ticket);

        log.info("Job submitted: [{}]", jobKey);
    }

    public void recordJobResult(String jobKey, JobState jobState, Ticket ticket) {

        log.info("Record job result: [{}]", jobKey);

        var execState = JobState.deserialize(jobState.executorState, ExecutorState.class);
        var execResult = jobExecutor.pollBatch(execState);

        jobState.statusCode = execResult.statusCode;
        jobState.statusMessage = execResult.statusMessage;

        if (execResult.statusCode == JobStatusCode.SUCCEEDED) {

            try {

                var jobResultFile = String.format("job_result_%s.json", jobKey);
                var jobResultBytes = jobExecutor.readFile(execState, "result", jobResultFile);

                jobState.jobResult = ConfigParser.parseConfig(jobResultBytes, ConfigFormat.JSON, JobResult.class);

                // If the validator is extended to cover the config interface,
                // The top level job result could be validated directly

                for (var result : jobState.jobResult.getResultsMap().entrySet()) {

                    log.info("Validating job result [{}]", result.getKey());
                    validator.validateFixedObject(result.getValue());
                }
            }
            catch (EExecutorFailure | EConfigParse | EValidation e) {

                log.error("Job [{}] succeeded but the response could not be processed", jobKey, e);

                var errorMessage = e.getMessage();
                var shortMessage = errorMessage.lines().findFirst().orElse("No details available");

                jobState.statusCode = JobStatusCode.FAILED;
                jobState.statusMessage = shortMessage;
            }
        }

        // This is what publishes metadata for the job to the metadata service
        jobLifecycle.processJobResult(jobState).toCompletableFuture().join();

        // Only once the results are recorded, update the cache and remove the physical job
        jobCache.updateJob(jobKey, jobState, ticket);
        jobExecutor.destroyBatch(jobKey, execState);

        // Schedule removal from the cache at some later time (allows check-job for some time after completion)
        executorService.schedule(
                () -> jobOperation(jobKey, this::deleteJob),
                RETAIN_COMPLETE_DELAY.getSeconds(), TimeUnit.SECONDS);

        if (jobState.statusCode == JobStatusCode.SUCCEEDED) {
            log.info("Job [{}] {}", jobKey, jobState.statusCode);
        }
        else {
            log.error("Job [{}] {}", jobKey, jobState.statusCode);
            log.error("{}", jobState.statusMessage);
            if (execResult.errorDetail != null)
                log.error(execResult.errorDetail);
        }
    }

    private void deleteJob(String jobKey, JobState jobState, Ticket ticket) {

        log.info("Removing job from cache: [{}] (status = {})", jobKey, jobState.statusCode.name());

        jobCache.deleteJob(jobKey, ticket);
    }

    @FunctionalInterface
    private interface JobOperationFunc {

        void apply(String jobKey, JobState jobState, Ticket ticket);
    }


    private void jobOperation(String jobKey, JobOperationFunc func) {

        try (var ctx = jobCache.useTicket(jobKey)) {

            if (ctx.superseded())
                return;

            var jobState = jobCache.readJob(jobKey);

            func.apply(jobKey, jobState, ctx.ticket());
        }
        catch (ECacheNotFound e) {
            log.warn("Job [{}] is no longer in the cache", jobKey);
        }
        catch (ECache e) {
            log.warn("Cache error while processing job: [{}] {}", jobKey, e.getMessage(), e);
        }
        catch (EExecutorUnavailable e) {
            log.warn("Executor is not responding or unavailable: [{}] {}", jobKey, e.getMessage(), e);
        }
        catch (Exception e) {

            try (var ctx = jobCache.useTicket(jobKey)) {

                if (ctx.superseded())
                    return;

                log.error("Job will be deleted due to an unexpected error: [{}] {}", jobKey, e.getMessage(), e);

                var jobState = jobCache.readJob(jobKey);

                jobState.statusCode = JobStatusCode.FAILED;
                jobState.statusMessage = e.getMessage();

                jobCache.updateJob(jobKey, jobState, ctx.ticket());

                executorService.schedule(
                        () -> jobOperation(jobKey, this::deleteJob),
                        RETAIN_COMPLETE_DELAY.getSeconds(), TimeUnit.SECONDS);
            }
            catch (Exception e2) {

                log.error("Error handling failed: [{}] {}", jobKey, e.getMessage(), e);
            }
        }
    }
}
