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

import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.exec.ExecutorJobInfo;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.JobStatusCode;
import org.finos.tracdap.svc.orch.cache.IJobCache;

import io.netty.util.concurrent.EventExecutorGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


public class JobManager {

    public static final Duration LAUNCH_DURATION = Duration.of(2, ChronoUnit.MINUTES);

    public static final int DEFAULT_JOB_LIMIT = 6;
    public static final Duration DEFAULT_CACHE_POLL_INTERVAL = Duration.of(2, ChronoUnit.SECONDS);
    public static final Duration DEFAULT_EXECUTOR_POLL_INTERVAL = Duration.of(30, ChronoUnit.SECONDS);
    public static final Duration STARTUP_DELAY = Duration.of(10, ChronoUnit.SECONDS);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JobProcessor processor;
    private final IJobCache<JobState> cache;
    private final EventExecutorGroup javaExecutor;

    private final int jobLimit;
    private final Duration cachePollInterval;
    private final Duration executorPollInterval;

    private ScheduledFuture<?> cachePollingTask = null;
    private ScheduledFuture<?> executorPollingTask = null;

    public JobManager(
            JobProcessor processor,
            IJobCache<JobState> cache,
            EventExecutorGroup javaExecutor) {

        this.processor = processor;
        this.cache = cache;
        this.javaExecutor = javaExecutor;

        jobLimit = DEFAULT_JOB_LIMIT;
        cachePollInterval = DEFAULT_CACHE_POLL_INTERVAL;
        executorPollInterval = DEFAULT_EXECUTOR_POLL_INTERVAL;
    }

    public void start() {

        try {

            log.info("Starting job manager service...");

            // Delay initial polls by the polling interval
            // This is to prevent polling while the service is still starting

            cachePollingTask = javaExecutor.scheduleAtFixedRate(
                    this::pollCache,
                    STARTUP_DELAY.getSeconds(),
                    cachePollInterval.getSeconds(),
                    TimeUnit.SECONDS);

            executorPollingTask = javaExecutor.scheduleAtFixedRate(
                    this::pollExecutor,
                    STARTUP_DELAY.getSeconds(),
                    executorPollInterval.getSeconds(),
                    TimeUnit.SECONDS);

            log.info("Job manager service started OK");
        }
        catch (RejectedExecutionException e) {

            log.error("Job manager service failed to start: {}", e.getMessage(), e);
            throw new EStartup("Job manager service failed to start", e);
        }
    }

    public void stop() {

        log.info("Stopping job manager service...");

        if (cachePollingTask != null) {
            cachePollingTask.cancel(false);
        }

        if (executorPollingTask != null) {
            executorPollingTask.cancel(false);
        }
    }

    public void pollCache() {

        try {

            // Process all available job updates
            // Any updates that get scheduled twice will be ignored as superseded

            // We could put some more intelligent logic here, e.g. an update queue / capacity
            // Or filter down the query so not all nodes attempt all updates
            // But the ticket.superseded() mechanism should be sufficient unless the load is extreme

            var updatedJobs = cache.queryState(STATUS_FOR_UPDATE);

            for (var update : updatedJobs)
                javaExecutor.submit(() -> processJobUpdate(update.key(), update.revision()));

            // Jobs that are ready to launch get special handling

            pollForLaunch();
        }
        catch (Exception e) {

            log.warn("There was an error polling the job cache: " + e.getMessage(), e);
            log.warn("Polling operation will be retried periodically");

            // TODO: Track successive polling errors, take action after a threshold
        }
    }

    public void pollForLaunch() {

        try {

            var runningJobs = cache.queryState(STATUS_FOR_RUNNING_JOBS);

            var launchCapacity = jobLimit - runningJobs.size();
            if (launchCapacity <= 0)
                return;

            var launchableJobs = cache.queryState(STATUS_FOR_LAUNCH);

            // Select the first jobs in the queue up to the available capacity
            // Launch selection could be a lot more intelligent!!!

            var selectedJobs = launchableJobs.size() > launchCapacity
                    ? launchableJobs.subList(0, launchCapacity)
                    : launchableJobs;

            for (var job : selectedJobs)
                javaExecutor.submit(() -> launchJob(job.key(), job.revision()));
        }
        catch (Exception e) {

            log.warn("There was an error polling the executor: " + e.getMessage(), e);
            log.warn("Polling operation will be retried periodically");

            // TODO: Track successive polling errors, take action after a threshold
        }
    }

    public void pollExecutor() {

        try {

            var runningJobs = cache.queryState(STATUS_FOR_RUNNING_JOBS);

            var pollRequests = runningJobs.stream()
                    .map(j -> Map.entry(j.key(), j.value()))
                    .collect(Collectors.toList());

            var pollResults = processor.pollExecutorJobs(pollRequests);

            for (var i = 0; i < runningJobs.size(); i++) {

                var job = runningJobs.get(i);
                var pollResult = pollResults.get(i);

                if (pollResult.getStatus() != job.value().batchStatus)
                    javaExecutor.submit(() -> recordPollResult(job.key(), job.revision(), pollResult));
            }
        }
        catch (Exception e) {

            log.warn("There was an error polling the executor: " + e.getMessage(), e);
            log.warn("Polling operation will be retried periodically");

            // TODO: Track successive polling errors, take action after a threshold
        }
    }

    public JobState addNewJob(JobState jobState) {

        try {

            // Job key is not known until the initial metadata is saved

            var newState_ = jobState.clone();
            newState_.tracStatus = JobStatusCode.QUEUED;
            newState_.cacheStatus = CacheStatus.QUEUED_IN_TRAC;

            var newState = processor.saveInitialMetadata(newState_);
            newState.jobKey = MetadataUtil.objectKey(newState.jobId);

            try (var ticket = cache.openNewTicket(newState.jobKey)) {

                // Duplicate job key, should never happen
                if (ticket.superseded())
                    throw new ECacheTicket("Job could not be created because it already exists");

                cache.addEntry(ticket, newState.cacheStatus, newState);
            }

            // Avoid polling delay if multiple updates are processed in succession
            // This will also tend to make all the updates in a sequence run on one orchestrator node

            javaExecutor.submit(this::pollForLaunch);

            return newState;
        }
        catch (Exception e) {

            // This method is called from the public API
            // Let errors propagate back to the client

            var message = String.format("Job was not accepted: %s", e.getMessage());
            log.error(message);

            throw new EJobFailure(message, e);
        }
    }

    public JobState queryJob(String jobKey) {

        var cacheEntry = cache.getLatestEntry(jobKey);

        return cacheEntry != null ? cacheEntry.value() : null;
    }

    public void launchJob(String jobKey, int revision) {

        try (var ticket = cache.openTicket(jobKey, revision, LAUNCH_DURATION)) {

            if (ticket.superseded())
                return;

            var cacheEntry = cache.getEntry(ticket);
            var jobState = cacheEntry.value();
            var newState = processor.launchJob(jobState);

            cache.updateEntry(ticket, newState.cacheStatus, newState);
        }
        catch (Exception e) {

            log.warn("There was a problem launching the job: " + e.getMessage(), e);
            log.warn("Launch operation will be retried");
        }
    }

    public void recordPollResult(String jobKey, int revision, ExecutorJobInfo executorJobInfo) {

        int newRevision = revision;
        String newCacheStatus = null;

        try (var ticket = cache.openTicket(jobKey, revision)) {

            if (ticket.superseded())
                return;

            var cacheEntry = cache.getEntry(ticket);
            var jobState = cacheEntry.value();
            var newState = processor.recordPollStatus(jobState, executorJobInfo);

            newRevision = cache.updateEntry(ticket, newState.cacheStatus, newState);
            newCacheStatus = newState.cacheStatus;
        }
        catch (Exception e) {

            log.warn("There was a problem polling the job: " + e.getMessage(), e);
            log.warn("Poll operation will be retried");
        }

        // Avoid polling delay if multiple updates are processed in succession
        // This will also tend to make all the updates in a sequence run on one orchestrator node

        if (newRevision > revision && STATUS_FOR_UPDATE.contains(newCacheStatus)) {
            var newRevision_ = newRevision;
            javaExecutor.submit(() -> processJobUpdate(jobKey, newRevision_));
        }
    }

    public void processJobUpdate(String jobKey, int revision) {

        int newRevision = revision;
        String newCacheStatus = null;

        try (var ticket = cache.openTicket(jobKey, revision)) {

            if (ticket.superseded())
                return;

            var cacheEntry = cache.getEntry(ticket);
            var jobState = cacheEntry.value();

            if (jobState.cacheStatus.equals(CacheStatus.READY_TO_REMOVE)) {
                cache.removeEntry(ticket);
                return;
            }

            var updateFunc = getUpdateFunc(jobState.cacheStatus);
            var newState = updateFunc.apply(jobState);

            newRevision = cache.updateEntry(ticket, newState.cacheStatus, newState);
            newCacheStatus = newState.cacheStatus;
        }
        catch (Exception e) {

            log.warn("There was a problem processing the job: " + e.getMessage(), e);
            log.warn("The operation will be retried");
        }

        // Avoid polling delay if multiple updates are processed in succession
        // This will also tend to make all the updates in a sequence run on one orchestrator node

        if (newRevision > revision && STATUS_FOR_UPDATE.contains(newCacheStatus)) {
            var newRevision_ = newRevision;
            javaExecutor.submit(() -> processJobUpdate(jobKey, newRevision_));
        }
    }

    private Function<JobState, JobState> getUpdateFunc(String cacheStatus) {

        if (!STATUS_FOR_UPDATE.contains(cacheStatus))
            throw new EUnexpected();

        if (cacheStatus.equals(CacheStatus.EXECUTOR_COMPLETE) || cacheStatus.equals(CacheStatus.EXECUTOR_SUCCEEDED))
            return processor::fetchJobResult;

        if (cacheStatus.equals(CacheStatus.EXECUTOR_FAILED))
            return processor::saveResultMetadata;

        if (cacheStatus.equals(CacheStatus.RESULTS_RECEIVED) || cacheStatus.equals(CacheStatus.RESULTS_INVALID))
            return processor::saveResultMetadata;

        if (cacheStatus.equals(CacheStatus.RESULTS_SAVED))
            return processor::cleanUpJob;

        // Should never be called, but not an error
        if (cacheStatus.equals(CacheStatus.READY_TO_REMOVE))
            return Function.identity();

        throw new EUnexpected();
    }

    private static final List<String> STATUS_FOR_LAUNCH = List.of(
            CacheStatus.QUEUED_IN_TRAC);

    private static final List<String> STATUS_FOR_RUNNING_JOBS = List.of(
            CacheStatus.SENT_TO_EXECUTOR,
            CacheStatus.QUEUED_IN_EXECUTOR,
            CacheStatus.RUNNING_IN_EXECUTOR);

    private static final List<String> STATUS_FOR_UPDATE = List.of(
            CacheStatus.EXECUTOR_COMPLETE,
            CacheStatus.EXECUTOR_SUCCEEDED,
            CacheStatus.EXECUTOR_FAILED,
            CacheStatus.RESULTS_RECEIVED,
            CacheStatus.RESULTS_INVALID,
            CacheStatus.RESULTS_SAVED,
            CacheStatus.READY_TO_REMOVE);
}
