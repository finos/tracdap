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

import org.finos.tracdap.common.auth.internal.InternalAuthProvider;
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.exec.ExecutorJobInfo;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.PluginConfig;
import org.finos.tracdap.metadata.JobStatusCode;
import org.finos.tracdap.svc.orch.cache.IJobCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


public class JobManager {

    public static final Duration STARTUP_DELAY = Duration.of(10, ChronoUnit.SECONDS);
    public static final Duration SCHEDULED_REMOVAL_DURATION = Duration.of(2, ChronoUnit.MINUTES);
    public static final Duration JOB_TIMEOUT = Duration.of(12, ChronoUnit.HOURS);

    public static final String POLL_INTERVAL_CONFIG_KEY = "pollInterval";
    public static final String TICKET_DURATION_CONFI_KEY = "ticketDuration";
    public static final String MAX_JOBS_CONFIG_KEY = "maxJobs";

    public static final int DEFAULT_CACHE_POLL_INTERVAL = 2;
    public static final int DEFAULT_CACHE_TICKET_DURATION = 10;
    public static final int DEFAULT_EXECUTOR_POLL_INTERVAL = 30;
    public static final int DEFAULT_EXECUTOR_TICKET_DURATION = 120;
    public static final int DEFAULT_EXECUTOR_JOB_LIMIT = 6;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JobProcessor processor;
    private final IJobCache<JobState> cache;
    private final ScheduledExecutorService javaExecutor;
    private final InternalAuthProvider internalAuth;

    private final Duration cachePollInterval;
    private final Duration cacheTicketDuration;
    private final Duration executorPollInterval;
    private final Duration executorTicketDuration;
    private final int executorJobLimit;

    private ScheduledFuture<?> cachePollingTask = null;
    private ScheduledFuture<?> executorPollingTask = null;

    public JobManager(
            PlatformConfig config,
            JobProcessor processor,
            IJobCache<JobState> cache,
            ScheduledExecutorService javaExecutor,
            InternalAuthProvider internalAuth) {

        this.processor = processor;
        this.cache = cache;
        this.javaExecutor = javaExecutor;
        this.internalAuth = internalAuth;

        cachePollInterval = Duration.ofSeconds(readIntegerProperty(config.getJobCache(), POLL_INTERVAL_CONFIG_KEY, DEFAULT_CACHE_POLL_INTERVAL));
        cacheTicketDuration = Duration.ofSeconds(readIntegerProperty(config.getJobCache(), TICKET_DURATION_CONFI_KEY, DEFAULT_CACHE_TICKET_DURATION));
        executorPollInterval = Duration.ofSeconds(readIntegerProperty(config.getExecutor(), POLL_INTERVAL_CONFIG_KEY, DEFAULT_EXECUTOR_POLL_INTERVAL));
        executorTicketDuration = Duration.ofSeconds(readIntegerProperty(config.getExecutor(), TICKET_DURATION_CONFI_KEY, DEFAULT_EXECUTOR_TICKET_DURATION));
        executorJobLimit = readIntegerProperty(config.getExecutor(), MAX_JOBS_CONFIG_KEY, DEFAULT_EXECUTOR_JOB_LIMIT);
    }

    private int readIntegerProperty(PluginConfig config, String propertyKey, int defaultValue) {

        if (!config.containsProperties(propertyKey))
            return defaultValue;

        var configValue = config.getPropertiesOrDefault(propertyKey, Integer.toString(defaultValue));

        try {
            return Integer.parseInt(configValue.trim());
        }
        catch (NumberFormatException e) {

            var message = String.format(
                    "Invalid config property [%s]: Expected an integer, got [%s]",
                    propertyKey, configValue);

            log.error(message);
            throw new EStartup(message);
        }
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

            var launchableJobs = cache.queryState(STATUS_FOR_LAUNCH);
            var runningJobs = cache.queryState(STATUS_FOR_RUNNING_JOBS, true);  // Include jobs with launch in progress
            var fetchResultJobs = cache.queryState(STATUS_FOR_FETCH_RESULTS);
            var updatedJobs = cache.queryState(STATUS_FOR_UPDATE);

            // A simple capacity cap to prevent spamming the executor with too many jobs
            // No queuing or prioritisation yet!!

            var launchCapacity = Math.max(executorJobLimit - runningJobs.size(), 0);
            var launchJobs = launchableJobs.size() > launchCapacity
                    ? launchableJobs.subList(0, launchCapacity)
                    : launchableJobs;

            for (var job : launchJobs)
                javaExecutor.submit(() -> launchJob(job.key(), job.revision()));

            for (var job : fetchResultJobs)
                javaExecutor.submit(() -> fetchJobResult(job.key(), job.revision()));

            for (var job : updatedJobs)
                javaExecutor.submit(() -> processJobUpdate(job.key(), job.revision()));
        }
        catch (Exception e) {

            log.warn("There was an error polling the job cache: " + e.getMessage(), e);
            log.warn("Polling operation will be retried periodically");

            // TODO: Track successive polling errors, take action after a threshold
        }
    }

    public void pollExecutor() {

        try {

            var runningJobs = cache.queryState(STATUS_FOR_RUNNING_JOBS)
                    // Only poll jobs that have an executor state
                    .stream().filter(j -> j.value().batchState != null)
                    .collect(Collectors.toList());

            var pollRequests = runningJobs.stream()
                    .map(j -> Map.entry(j.key(), j.value()))
                    .collect(Collectors.toList());

            var pollResults = processor.pollExecutorJobs(pollRequests);

            for (var i = 0; i < pollRequests.size(); i++) {

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

            newState_.credentials = internalAuth.createDelegateSession(newState_.owner, JOB_TIMEOUT);

            var newState = processor.saveInitialMetadata(newState_);
            newState.jobKey = MetadataUtil.objectKey(newState.jobId);

            try (var ticket = cache.openNewTicket(newState.jobKey, cacheTicketDuration)) {

                // Duplicate job key, should never happen
                if (ticket.superseded())
                    throw new ECacheTicket("Job could not be created because it already exists");

                cache.addEntry(ticket, newState.cacheStatus, newState);
            }

            // Avoid polling delay if multiple updates are processed in succession
            // This will also tend to make all the updates in a sequence run on one orchestrator node

            javaExecutor.submit(this::pollCache);

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

        // Launching a job means talking to the executor, so use the executor ticket duration

        try (var ticket = cache.openTicket(jobKey, revision, executorTicketDuration)) {

            if (ticket.superseded())
                return;

            var cacheEntry = cache.getEntry(ticket);
            var jobState = cacheEntry.value();

            // Reset auth processor in job state after deserialization
            internalAuth.setTokenProcessor(jobState.credentials);

            // Launching can take a long time for some executors
            // Setting a launch state lets the job count towards the running jobs total
            var launchState = processor.markAsPending(jobState);
            cache.updateEntry(ticket, launchState.cacheStatus, launchState);

            // Perform the launch
            var newState = processor.launchJob(launchState);
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

        try (var ticket = cache.openTicket(jobKey, revision, cacheTicketDuration)) {

            if (ticket.superseded())
                return;

            var cacheEntry = cache.getEntry(ticket);
            var jobState = cacheEntry.value();

            // Reset auth processor in job state after deserialization
            internalAuth.setTokenProcessor(jobState.credentials);

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

    public void fetchJobResult(String jobKey, int revision) {

        int newRevision = revision;
        String newCacheStatus = null;

        // Fetching a result means talking to the executor, so use the executor ticket duration

        try (var ticket = cache.openTicket(jobKey, revision, executorTicketDuration)) {

            if (ticket.superseded())
                return;

            var cacheEntry = cache.getEntry(ticket);
            var jobState = cacheEntry.value();

            // Reset auth processor in job state after deserialization
            internalAuth.setTokenProcessor(jobState.credentials);

            var newState = processor.fetchJobResult(jobState);

            newRevision = cache.updateEntry(ticket, newState.cacheStatus, newState);
            newCacheStatus = newState.cacheStatus;
        }
        catch (Exception e) {

            log.warn("There was a problem launching the job: " + e.getMessage(), e);
            log.warn("Launch operation will be retried");
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

        try (var ticket = cache.openTicket(jobKey, revision, cacheTicketDuration)) {

            if (ticket.superseded())
                return;

            var cacheEntry = cache.getEntry(ticket);
            var jobState = cacheEntry.value();

            // Reset auth processor in job state after deserialization
            internalAuth.setTokenProcessor(jobState.credentials);

            var updateFunc = getUpdateFunc(jobState.cacheStatus);
            var newState = updateFunc.apply(jobState);

            newRevision = cache.updateEntry(ticket, newState.cacheStatus, newState);
            newCacheStatus = newState.cacheStatus;

            // If the job is scheduled for removal, create a remove task
            if (newState.cacheStatus.equals(CacheStatus.SCHEDULED_TO_REMOVE)) {
                javaExecutor.schedule(
                        () -> removeFromCache(jobKey, revision + 1),
                        SCHEDULED_REMOVAL_DURATION.getSeconds(), TimeUnit.SECONDS);
            }
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

    public void removeFromCache(String jobKey, int revision) {

        log.info("REMOVING JOB FROM CACHE: [{}]", jobKey);

        try (var ticket = cache.openTicket(jobKey, revision, cacheTicketDuration)) {

            if (ticket.missing())
                return;

            cache.removeEntry(ticket);
        }
        catch (Exception e) {

            log.warn("There was a problem processing the job: " + e.getMessage(), e);
            log.warn("The operation will be retried");
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
            return processor::scheduleRemoval;

        throw new EUnexpected();
    }

    private static final List<String> STATUS_FOR_LAUNCH = List.of(
            CacheStatus.QUEUED_IN_TRAC);

    private static final List<String> STATUS_FOR_RUNNING_JOBS = List.of(
            CacheStatus.LAUNCH_IN_PROGRESS,
            CacheStatus.SENT_TO_EXECUTOR,
            CacheStatus.QUEUED_IN_EXECUTOR,
            CacheStatus.RUNNING_IN_EXECUTOR);

    private static final List<String> STATUS_FOR_FETCH_RESULTS = List.of(
            CacheStatus.EXECUTOR_COMPLETE,
            CacheStatus.EXECUTOR_SUCCEEDED);

    private static final List<String> STATUS_FOR_UPDATE = List.of(
            CacheStatus.EXECUTOR_FAILED,
            CacheStatus.RESULTS_RECEIVED,
            CacheStatus.RESULTS_INVALID,
            CacheStatus.RESULTS_SAVED,
            CacheStatus.READY_TO_REMOVE);

    // States that are not acted on:
    // CacheStatus.SCHEDULED_TO_REMOVE
}
