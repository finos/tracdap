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

import com.accenture.trac.api.JobStatus;
import com.accenture.trac.api.JobStatusCode;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.config.Job;
import com.accenture.trac.config.RepositoryConfig;
import com.accenture.trac.config.RuntimeConfig;
import com.accenture.trac.svc.orch.cache.IJobCache;
import com.accenture.trac.svc.orch.cache.JobState;
import com.accenture.trac.svc.orch.cache.TicketRequest;
import com.accenture.trac.svc.orch.exec.ExecutorState;
import com.accenture.trac.svc.orch.exec.IBatchExecutor;

import com.accenture.trac.svc.orch.jobs.JobLogic;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class JobManagementService {

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(10);

    private static final RuntimeConfig rtc = RuntimeConfig.newBuilder()
            .putRepositories("trac_git_repo", RepositoryConfig.newBuilder()
            .setRepoType("git")
            .setRepoUrl("https://github.com/accenture/trac")
            .build()).build();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IJobCache jobCache;
    private final IBatchExecutor jobExecutor;
    private final ScheduledExecutorService executorService;
    private ScheduledFuture<?> pollingTask;

    public JobManagementService(
            IJobCache jobCache,
            IBatchExecutor jobExecutor,
            ScheduledExecutorService executorService) {

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

        log.info("Polling job cache...");

        var completeStates = List.of(JobStatusCode.COMPLETE, JobStatusCode.FAILED, JobStatusCode.CANCELLED);
        var completeJobs = jobCache.pollJobs(job -> completeStates.contains(job.statusCode));

        for (var job : completeJobs)
            executorService.schedule(() -> recordJobResult(job.jobKey), 0L, TimeUnit.SECONDS);

        var queuedJobs = jobCache.pollJobs(job -> job.statusCode == JobStatusCode.QUEUED);

        for (var job : queuedJobs)
            executorService.schedule(() -> submitJob(job.jobKey), 0L, TimeUnit.SECONDS);
    }

    public void pollExecutor() {

        log.info("Polling executor...");

        var activeStates = List.of(JobStatusCode.SUBMITTED, JobStatusCode.RUNNING);
        var activeJobs = jobCache.pollJobs(job -> activeStates.contains(job.statusCode));

        var execStateMap = new HashMap<String, ExecutorState>();

        for (var job : activeJobs) {
            var executorState = JobState.deserialize(job.executorState, ExecutorState.class);
            execStateMap.put(job.jobKey, executorState);
        }

        var pollResults = jobExecutor.pollAllBatches(execStateMap);

        if (pollResults.isEmpty())
            return;

        try (var ctx = jobCache.useTicket(TicketRequest.forJob())) {

            for (var result : pollResults) {

                var job = jobCache.readJob(result.jobKey);
                job.statusCode = result.statusCode;
                job.executorState = JobState.serialize(result.executorState);

                jobCache.updateJob(job.jobKey, job, ctx.ticket());
            }
        }
    }

    public void submitJob(String jobKey) {

        log.info("Submit job for execution: [{}]", jobKey);

        var resourceTicket = TicketRequest.forResources();
        var jobTicket = TicketRequest.forJob();

        try (var ctx = jobCache.useTicket(resourceTicket, jobTicket)) {

            if (ctx.superseded())
                return;

            var jobState = jobCache.readJob(jobKey);
            var jobLogic = JobLogic.forJobType(jobState.jobType);

            var jobConfig = jobLogic.buildJobConfig(jobState.jobId, jobState.definition);
            var sysConfig = rtc;  // TODO: Real sys config for model runtime

            // TODO: Config format and file names

            var jobConfigJson = JsonFormat.printer().print(jobConfig);
            var sysConfigJson = JsonFormat.printer().print(sysConfig);

            var configMap = Map.ofEntries(
                    Map.entry("job_config.json", jobConfigJson),
                    Map.entry("sys_config.json", sysConfigJson));

            var execState = jobExecutor.createBatchSandbox(jobKey);
            execState = jobExecutor.writeTextConfig(jobKey, execState, configMap);
            execState = jobExecutor.startBatch(jobKey, execState, configMap.keySet());

            jobState.statusCode = JobStatusCode.SUBMITTED;
            jobState.executorState = JobState.serialize(execState);

            jobCache.updateJob(jobKey, jobState, ctx.ticket());

            log.info("Job submitted: [{}]", jobKey);
        }
        catch (InvalidProtocolBufferException e) {

            log.error("Job submit failed: [{}] {}", jobKey, e.getMessage(), e);

            // TODO: Error
            throw new EUnexpected(e);
        }
        catch (RuntimeException e) {

            log.error("Job submit failed: [{}] {}", jobKey, e.getMessage(), e);
            throw e;
        }
    }

    public void recordJobResult(String jobKey) {

        var job = jobCache.readJob(jobKey);

        log.info("Record job result [{}]: {}", jobKey, job.statusCode);
    }
}
