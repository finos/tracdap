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

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.finos.tracdap.common.auth.internal.InternalAuthProvider;
import org.finos.tracdap.common.exception.*;
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
    public static final int PROCESSING_RETRY_LIMIT = 2;

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

            var updatedJobs = cache.queryState(STATUS_FOR_UPDATE);

            for (var job : updatedJobs)
                javaExecutor.submit(() -> processJobUpdate(job.key(), job.revision(), job.getStatus()));


            // The launch scheduling operation is special - we don't want to launch everything in the queue!
            // Currently just a simple capacity cap to prevent spamming the executor with too many jobs
            // No queuing or prioritisation yet!!

            var launchableJobs = cache.queryState(STATUS_FOR_LAUNCH);
            var runningJobs = cache.queryState(STATUS_FOR_RUNNING_JOBS, true);  // Include jobs with launch in progress

            var launchCapacity = Math.max(executorJobLimit - runningJobs.size(), 0);
            var launchJobs = launchableJobs.size() > launchCapacity
                    ? launchableJobs.subList(0, launchCapacity)
                    : launchableJobs;

            for (var job : launchJobs)
                javaExecutor.submit(() -> processJobUpdate(job.key(), job.revision(), job.getStatus()));
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

                if (pollResult.getStatus() != job.value().batchStatus) {

                    var operation = (Function<JobState, JobState>) (jobState) -> processor.recordPollStatus(jobState, pollResult);
                    var timeout = executorTicketDuration;

                    javaExecutor.submit(() -> processJobUpdate(job.key(), job.revision(), timeout, operation));
                }
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



    private void processJobUpdate(String jobKey, int revision, String jobStatus) {

        var operation = getNextOperation(jobStatus);
        var timeout = getOperationTimeout(jobStatus);

        processJobUpdate(jobKey, revision, timeout, operation);
    }

    private void processJobUpdate(
            String jobKey, int revision, Duration timeout,
            Function<JobState, JobState> operation) {

        int newRevision = revision;
        String newCacheStatus = null;

        try (var ticket = cache.openTicket(jobKey, revision, timeout)) {

            if (ticket.superseded())
                return;

            var cacheEntry = cache.getEntry(ticket);
            var jobState = cacheEntry.value();

            // Reset auth processor in job state after deserialization
            internalAuth.setTokenProcessor(jobState.credentials);

            var newState = processRetryOrFail(operation, jobState);

            newRevision = cache.updateEntry(ticket, newState.cacheStatus, newState);
            newCacheStatus = newState.cacheStatus;

            // If the job is scheduled for removal, create a remove task
            if (newState.cacheStatus.equals(CacheStatus.SCHEDULED_TO_REMOVE)) {
                javaExecutor.schedule(
                        () -> removeFromCache(jobKey, revision + 1),
                        SCHEDULED_REMOVAL_DURATION.getSeconds(), TimeUnit.SECONDS);
            }
        }
        catch (Exception cacheError) {

            log.warn("There was a problem processing the job: " + cacheError.getMessage(), cacheError);
            log.warn("The operation will be retried");
        }

        // Avoid polling delay if multiple updates are processed in succession
        // This will also tend to make all the updates in a sequence run on one orchestrator node

        if (newRevision > revision && STATUS_FOR_UPDATE.contains(newCacheStatus)) {
            var newRevision_ = newRevision;
            var newCacheStatus_ = newCacheStatus;
            javaExecutor.submit(() -> processJobUpdate(jobKey, newRevision_, newCacheStatus_));
        }
    }

    private void removeFromCache(String jobKey, int revision) {

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

    private Function<JobState, JobState> getNextOperation(String jobStatus) {

        if (jobStatus.equals(CacheStatus.QUEUED_IN_TRAC))
            return processor::scheduleLaunch;

        if (jobStatus.equals(CacheStatus.LAUNCH_SCHEDULED))
            return processor::launchJob;

        if (jobStatus.equals(CacheStatus.EXECUTOR_COMPLETE) || jobStatus.equals(CacheStatus.EXECUTOR_SUCCEEDED))
            return processor::fetchJobResult;

        if (jobStatus.equals(CacheStatus.EXECUTOR_FAILED))
            return processor::saveResultMetadata;

        if (jobStatus.equals(CacheStatus.RESULTS_RECEIVED) || jobStatus.equals(CacheStatus.RESULTS_INVALID))
            return processor::saveResultMetadata;

        if (jobStatus.equals(CacheStatus.RESULTS_SAVED))
            return processor::cleanUpJob;

        // Should never be called, but not an error
        if (jobStatus.equals(CacheStatus.READY_TO_REMOVE))
            return processor::scheduleRemoval;

        throw new EUnexpected();
    }

    private Duration getOperationTimeout(String jobStatus) {

        if (STATUS_FOR_EXECUTOR_TASKS.contains(jobStatus))
            return executorTicketDuration;
        else
            return cacheTicketDuration;
    }

    private JobState processRetryOrFail(Function<JobState, JobState> operation, JobState jobState) {

        try {

            return operation.apply(jobState);
        }
        catch (Exception error) {

            if (!errorCanRetry(error)) {

                log.error("Error processing job [{}]: {}", jobState.jobKey, error.getMessage(), error);
                log.error("This error is fatal and cannot be retried");

                var newState = jobState.clone();
                newState.cacheStatus = CacheStatus.PROCESSING_FAILED;
                newState.statusMessage = error.getMessage();
                newState.exception = error;

                return newState;
            }

            else if (jobState.retries < PROCESSING_RETRY_LIMIT) {

                log.warn("Error processing job [{}]: {}", jobState.jobKey, error.getMessage(), error);
                log.warn("The operation can be retried");

                var newState = jobState.clone();
                newState.retries = jobState.retries + 1;

                return newState;
            }

            else {

                log.error("Error processing job [{}]: {}", jobState.jobKey, error.getMessage(), error);
                log.error("The retry limit has been reached");

                var newState = jobState.clone();
                newState.cacheStatus = CacheStatus.PROCESSING_FAILED;

                return newState;
            }
        }
    }

    private boolean errorCanRetry(Exception error) {

        if (error instanceof StatusException) {
            return GRPC_CAN_RETRY.contains(((StatusException) error).getStatus().getCode());
        }

        if (error instanceof StatusRuntimeException) {
            return GRPC_CAN_RETRY.contains(((StatusRuntimeException) error).getStatus().getCode());
        }

        return TRAC_CAN_RETRY.contains(error.getClass());
    }

    private static final List<String> STATUS_FOR_LAUNCH = List.of(
            CacheStatus.QUEUED_IN_TRAC);

    private static final List<String> STATUS_FOR_RUNNING_JOBS = List.of(
            CacheStatus.LAUNCH_SCHEDULED,
            CacheStatus.SENT_TO_EXECUTOR,
            CacheStatus.QUEUED_IN_EXECUTOR,
            CacheStatus.RUNNING_IN_EXECUTOR);

    private static final List<String> STATUS_FOR_UPDATE = List.of(
            CacheStatus.LAUNCH_SCHEDULED,
            CacheStatus.EXECUTOR_COMPLETE,
            CacheStatus.EXECUTOR_SUCCEEDED,
            CacheStatus.EXECUTOR_FAILED,
            CacheStatus.RESULTS_RECEIVED,
            CacheStatus.RESULTS_INVALID,
            CacheStatus.RESULTS_SAVED,
            CacheStatus.READY_TO_REMOVE);

    private static final List<String> STATUS_FOR_EXECUTOR_TASKS = List.of(
            CacheStatus.LAUNCH_SCHEDULED,
            CacheStatus.EXECUTOR_COMPLETE,
            CacheStatus.EXECUTOR_SUCCEEDED);

    private static final List<Status.Code> GRPC_CAN_RETRY = List.of(
            Status.Code.UNAVAILABLE,
            Status.Code.DEADLINE_EXCEEDED);

    private static final List<Class<? extends ETrac>> TRAC_CAN_RETRY = List.of(
            EExecutorUnavailable.class);

    // States that are not acted on:
    // CacheStatus.SCHEDULED_TO_REMOVE
}
