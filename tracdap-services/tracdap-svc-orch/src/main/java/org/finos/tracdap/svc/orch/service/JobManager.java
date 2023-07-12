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
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.cache.CacheEntry;
import org.finos.tracdap.common.cache.IJobCache;
import org.finos.tracdap.common.exec.ExecutorJobInfo;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.PluginConfig;
import org.finos.tracdap.metadata.JobStatusCode;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class JobManager {

    public static final Duration STARTUP_DELAY = Duration.of(10, ChronoUnit.SECONDS);
    public static final Duration SCHEDULED_REMOVAL_DURATION = Duration.of(2, ChronoUnit.MINUTES);
    public static final int PROCESSING_RETRY_LIMIT = 2;
    public static final int CACHE_POLL_ERROR_LIMIT = 100;
    public static final int EXECUTOR_POLL_ERROR_LIMIT = 20;
    public static final int JOB_REVISION_LIMIT = 100;

    public static final String POLL_INTERVAL_CONFIG_KEY = "pollInterval";
    public static final String TICKET_DURATION_CONFI_KEY = "ticketDuration";
    public static final String MAX_JOBS_CONFIG_KEY = "maxJobs";

    public static final int DEFAULT_CACHE_POLL_INTERVAL = 10;
    public static final int DEFAULT_CACHE_TICKET_DURATION = 10;
    public static final int DEFAULT_EXECUTOR_POLL_INTERVAL = 30;
    public static final int DEFAULT_EXECUTOR_TICKET_DURATION = 120;
    public static final int DEFAULT_EXECUTOR_JOB_LIMIT = 6;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JobProcessor processor;
    private final IJobCache<JobState> cache;
    private final ScheduledExecutorService javaExecutor;

    private final Duration cachePollInterval;
    private final Duration cacheTicketDuration;
    private final Duration executorPollInterval;
    private final Duration executorTicketDuration;
    private final int executorJobLimit;

    private ScheduledFuture<?> cachePollingTask = null;
    private ScheduledFuture<?> executorPollingTask = null;

    private final AtomicInteger cachePollErrorCount = new AtomicInteger(0);
    private final AtomicInteger executorPollErrorCount = new AtomicInteger(0);

    public JobManager(
            PlatformConfig config,
            JobProcessor processor,
            IJobCache<JobState> cache,
            ScheduledExecutorService javaExecutor) {

        this.processor = processor;
        this.cache = cache;
        this.javaExecutor = javaExecutor;

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

    public JobState addNewJob(JobState jobState) {

        try {

            // Job key is not known until the initial metadata is saved

            var newState_ = jobState.clone();
            newState_.tracStatus = JobStatusCode.QUEUED;
            newState_.cacheStatus = CacheStatus.QUEUED_IN_TRAC;

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

    private void pollCache() {

        try {

            // Process all available job updates
            // Any updates that get scheduled twice will be ignored as superseded

            // We could put some more intelligent logic here, e.g. an update queue / capacity
            // Or filter down the query so not all nodes attempt all updates
            // But the ticket.superseded() mechanism should be sufficient unless the load is extreme

            var updatedJobs = cache.queryState(STATUS_FOR_UPDATE);

            for (var job : updatedJobs) {
                var operation = getNextOperation(job);
                javaExecutor.submit(() -> processJobOperation(operation));
            }

            // The launch scheduling operation is special - we don't want to launch everything in the queue!
            // Currently just a simple capacity cap to prevent spamming the executor with too many jobs
            // No queuing or prioritisation yet!!

            var launchableJobs = cache.queryState(STATUS_FOR_LAUNCH);
            var runningJobs = cache.queryState(STATUS_FOR_RUNNING_JOBS, true);  // Include jobs with launch in progress

            var launchCapacity = Math.max(executorJobLimit - runningJobs.size(), 0);
            var launchJobs = launchableJobs.size() > launchCapacity
                    ? launchableJobs.subList(0, launchCapacity)
                    : launchableJobs;

            for (var job : launchJobs) {
                var operation = getNextOperation(job);
                javaExecutor.submit(() -> processJobOperation(operation));
            }

            // No polling errors, reset the error count
            cachePollErrorCount.set(0);
        }
        catch (ECache e) {

            // Errors communicating with the job cache are expected and may be intermittent
            // Wait until the cache comes back online, then job processing will resume

            // Later we could add more sophisticated handling, e.g. back off exponentially
            // If we provide service health endpoints, cache health and polling failures could be reported

            log.warn("There was a problem talking to the job cache: " + e.getMessage(), e);
            log.warn("Polling will continue, the cache may become available at a later time");
        }
        catch (Exception e) {

            // Other than polling itself, there is no logic in the polling loop that should throw errors
            // All processing logic is offloaded into job operations and processed independently
            // Any errors that occur in the loop are serious, because they will prevent the loop from ever completing
            // If this happens we are going to take down the orchestrator service, rather than continue as a zombie

            log.error("Unexpected error in cache polling loop: " + e.getMessage(), e);
            log.error("This is probably a bug, if it continues the orchestrator will be shut down");

            var count = cachePollErrorCount.incrementAndGet();

            if (count > 1 && count < CACHE_POLL_ERROR_LIMIT)
                log.error("Cache polling error has occurred [{}] times, the limit is [{}]", count, CACHE_POLL_ERROR_LIMIT);

            if (count >= CACHE_POLL_ERROR_LIMIT) {
                log.debug("FATAL: Cache polling error has occurred [{}] times, the limit has been reached", count);
                log.error("FATAL: The orchestrator service will now be terminated");
                System.exit(-1);
            }
        }
    }

    private void pollExecutor() {

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

                    var operation = getNextOperation(job, pollResult);
                    javaExecutor.submit(() -> processJobOperation(operation));
                }
            }
        }
        catch (ECache | EExecutor e) {

            // Handle expected errors similar to pollCache(), errors with both job cache and executor are expected

            var service = e instanceof ECache ? "job cache" : "executor";

            log.warn("There was a problem talking to the {}}: {}", service, e.getMessage(), e);
            log.warn("Polling will continue, the {} may become available at a later time", service);
        }
        catch (Exception e) {

            // Handle unexpected errors similar to pollCache()

            log.error("Unexpected error in executor polling loop: " + e.getMessage(), e);
            log.error("This is probably a bug, if it continues the orchestrator will be shut down");

            var count = executorPollErrorCount.incrementAndGet();

            if (count > 1 && count < EXECUTOR_POLL_ERROR_LIMIT)
                log.error("Executor polling error has occurred [{}] times, the limit is [{}]", count, EXECUTOR_POLL_ERROR_LIMIT);

            if (count >= EXECUTOR_POLL_ERROR_LIMIT) {
                log.debug("FATAL: Executor polling error has occurred [{}] times, the limit has been reached", count);
                log.error("FATAL: The orchestrator service will now be terminated");
                System.exit(-1);
            }
        }
    }

    // Job processing state machine
    // Choose the next operation to perform for a job, based on its current state
    // The polling loop will only request the next operation when the job is ready to be updated
    // E.g. when a launch slot is available, or the polling result changes

    private JobOperation getNextOperation(CacheEntry<JobState> cacheEntry) {

        return getNextOperation(cacheEntry.key(), cacheEntry.revision(), cacheEntry.status());
    }

    private JobOperation getNextOperation(String key, int revision, String cacheStatus) {

        var operation = new JobOperation();
        operation.jobKey = key;
        operation.revision = revision;
        operation.cacheStatus = cacheStatus;

        // Guard against infinite loops in job state processing with a hard revision limit
        // This should never happen, errors are handled by moving the job into nextErrorState()
        // If error processing fails, nextErrorState() will remove the job well before the revision limit

        // See comments for nextErrorState() - we probably want a way to record failed error handling

        if (revision > JOB_REVISION_LIMIT) {

            log.error("Internal job state error, key = [{}], revision = [{}], cache state = [{}]", key, revision, cacheStatus);
            log.error("Job revision limit has been hit, job will be removed without further processing");

            operation.operationName = "remove_from_cache";
            operation.operation = state -> null;
            operation.timeout = cacheTicketDuration;

            return operation;
        }

        switch (cacheStatus) {

            case CacheStatus.QUEUED_IN_TRAC:
                operation.operationName = "schedule_launch";
                operation.operation = processor::scheduleLaunch;
                operation.timeout = cacheTicketDuration;
                break;

            case CacheStatus.LAUNCH_SCHEDULED:
                operation.operationName = "launch_job";
                operation.operation = processor::launchJob;
                operation.timeout = executorTicketDuration;
                break;

            case CacheStatus.EXECUTOR_COMPLETE:
            case CacheStatus.EXECUTOR_SUCCEEDED:
                operation.operationName = "fetch_job_result";
                operation.operation = processor::fetchJobResult;
                operation.timeout = executorTicketDuration;
                break;

            case CacheStatus.EXECUTOR_FAILED:
            case CacheStatus.RESULTS_RECEIVED:
            case CacheStatus.RESULTS_INVALID:
                operation.operationName = "save_result_metadata";
                operation.operation = processor::saveResultMetadata;
                operation.timeout = cacheTicketDuration;
                break;

            case CacheStatus.RESULTS_SAVED:
            case CacheStatus.READY_FOR_CLEANUP:
                operation.operationName = "clean_up_job";
                operation.operation = processor::cleanUpJob;
                operation.timeout = cacheTicketDuration;
                break;

            case CacheStatus.READY_TO_REMOVE:
                operation.operationName = "schedule_removal";
                operation.operation = processor::scheduleRemoval;
                operation.timeout = cacheTicketDuration;
                break;

            case CacheStatus.REMOVAL_SCHEDULED:
                operation.operationName = "remove_from_cache";
                operation.operation = state -> null;
                operation.timeout = cacheTicketDuration;
                break;

            // PROCESSING_FAILED state can occur for legitimate reasons, e.g. failure of a job operation
            // We want to handle this state normally, it is an expected error

            case CacheStatus.PROCESSING_FAILED:
                operation.operationName = "handle_processing_failed";
                operation.operation = state -> processor.handleProcessingFailed(state, state.statusMessage, state.exception);
                operation.timeout = cacheTicketDuration;
                break;

            default:

                // In case of invalid state, we need to abort processing this job
                // This is an unexpected / internal error and should be flagged to the user as such
                // This error only affects the current job, the job can be cleaned up, normal operations continue.
                // Do not throw an exception here (this would crash the polling loop)

                log.error("Internal job state error, key = [{}], revision = [{}], cache state = [{}]", key, revision, cacheStatus);

                var message = "Internal job state error";
                var error = new ETracInternal(message);

                operation.operationName = "handle_processing_failed";
                operation.operation = state -> processor.handleProcessingFailed(state, message, error);
                operation.timeout = cacheTicketDuration;
        }

        return operation;
    }

    private JobOperation getNextOperation(CacheEntry<JobState> cacheEntry, ExecutorJobInfo pollResult) {

        var operation = new JobOperation();
        operation.jobKey = cacheEntry.key();
        operation.revision = cacheEntry.revision();
        operation.cacheStatus = cacheEntry.status();
        operation.timeout = cacheTicketDuration;

        if (STATUS_FOR_RUNNING_JOBS.contains(cacheEntry.status())) {

            operation.operationName = "record_job_status";
            operation.operation = state -> processor.recordJobStatus(state, pollResult);
        }
        else {

            // Invalid state for the current job (equivalent to the default switch case above)

            log.error(
                    "Internal job state error, key = [{}], revision = [{}], cache state = [{}]",
                    cacheEntry.key(), cacheEntry.revision(), cacheEntry.status());

            var message = "Internal job state error";
            var error = new ETracInternal(message);

            operation.operationName = "handle_processing_failed";
            operation.operation = state -> processor.handleProcessingFailed(state, message, error);
        }

        return operation;
    }

    // If a job operation fails the job goes to nextErrorState()
    // The default error state, PROCESSING_FAILED, will record the failure and clean up the job
    // However this processing can also fail, in which case moving to nextErrorState() again skips some parts of the cleanup
    // Eventually the job will be removed from the cache

    // TODO: Dormant state for failed error handling
    // We could record some state in the cache when error handling fails to indicate failed / partial cleanup
    // These jobs could be returned to at a later date and failures reported back to the metadata store

    // These error states only relate to failed operations, not failed cache updates
    // Cache errors need to be handled separately, see comments in processJobOperation()

    private String getNextErrorState(String cacheStatus) {

        if (cacheStatus.equals(CacheStatus.PROCESSING_FAILED))
            return CacheStatus.READY_FOR_CLEANUP;

        if (cacheStatus.equals(CacheStatus.READY_FOR_CLEANUP))
            return CacheStatus.READY_TO_REMOVE;

        return CacheStatus.PROCESSING_FAILED;
    }

    private void processJobOperation(JobOperation operation) {

        int newRevision = operation.revision;
        String newCacheStatus = null;
        boolean isRetry = false;

        try (var ticket = cache.openTicket(operation.jobKey, operation.revision, operation.timeout)) {

            if (ticket.superseded())
                return;

            var cacheEntry = cache.getEntry(ticket);
            var jobState = cacheEntry.value();

            var newState = processRetryOrFail(operation, jobState);

            if (newState != null) {
                newRevision = cache.updateEntry(ticket, newState.cacheStatus, newState);
                newCacheStatus = newState.cacheStatus;
                isRetry = newState.retries > 0;
            }
            else {
                // Null new state indicates the job can be removed
                cache.removeEntry(ticket);
            }
        }
        catch (Exception e) {

            // TODO: We need a way to prevent operations being retried when there are cache communication errors

            // One approach would be to keep a local map of failed job keys and record a local failure state
            // The polling logic could ignore jobs in the local failure map for some period of time
            // Operation code can be a factor, e.g. fetch results has different behavior than launch
            // Jobs in the local failure map can still be included for timeout / cleanup processing

            // Errors here could also be used to indicate cache health, see comments in pollCache()

            log.warn("There was a problem talking to the job cache: " + e.getMessage(), e);
            log.warn("Processing will continue, the cache may become available at a later time");
        }

        // If the next operation can happen right away, submit it now to avoid cache polling delay
        // This will also tend to make all the updates in a sequence run on one orchestrator node
        // This needs to happen after the ticket for the original operation has been closed
        // Errors here can be safely ignored, the job will be picked up in the next polling loop anyway

        if (newRevision > operation.revision && !isRetry) {
            if (newCacheStatus != null && STATUS_FOR_UPDATE.contains(newCacheStatus)) {
                var nextOperation = getNextOperation(operation.jobKey, newRevision, newCacheStatus);
                javaExecutor.submit(() -> processJobOperation(nextOperation));
            }
        }

        // Create a cleanup task if the job is scheduled for removal
        // If we add timeout processing we would need to add a last activity monitor to the cache
        // In that case, cleanup could be detected by the regular polling loop

        if (newRevision > operation.revision && CacheStatus.REMOVAL_SCHEDULED.equals(newCacheStatus)) {
            var removeOperation = getNextOperation(operation.jobKey, newRevision, newCacheStatus);
            javaExecutor.schedule(
                    () -> processJobOperation(removeOperation),
                    SCHEDULED_REMOVAL_DURATION.getSeconds(), TimeUnit.SECONDS);
        }
    }

    private JobState processRetryOrFail(JobOperation operation, JobState jobState) {

        try {

            if (jobState.retries == 0) {
                log.info("JOB OPERATION {}: [{}]",
                        operation.operationName,
                        operation.jobKey);
            }
            else {
                log.info("JOB OPERATION {} (attempt {} of {}): [{}]",
                        operation.operationName,
                        jobState.retries + 1,
                        PROCESSING_RETRY_LIMIT,
                        operation.jobKey);
            }

            var newState = operation.operation.apply(jobState);

            // Reset the retry counter after a successful operation
            if (newState != null)
                newState.retries = 0;

            return newState;
        }
        catch (Exception error) {

            var newState = jobState.clone();
            var canRetry = errorCanRetry(error);

            // First check if a retry is possible
            // If it is, just bump the retry counter

            if (canRetry) {

                newState.retries += 1;

                if (newState.retries < PROCESSING_RETRY_LIMIT) {

                    log.warn("JOB OPERATION FAILED {}: [{}] {}", operation.operationName, jobState.jobKey, error.getMessage(), error);
                    log.warn("The operation can be retried (this was attempt {} of {})", newState.retries, PROCESSING_RETRY_LIMIT);

                    return newState;
                }
            }

            // If retry is not possible we need to go to the next error state
            // This is to prevent infinite loops for errors that can't be cleaned up
            // The first error state handler is to report errors in TRAC with full metadata
            // The last handler will just remove the job from the cache

            // We could be more clever here for distributed caches
            // E.g. if an operation fails once on a node and there are other nodes available,
            // a different node should pick up the retry

            var nextErrorState = getNextErrorState(jobState.cacheStatus);

            log.error("JOB OPERATION FAILED {}: [{}] {}", operation.operationName, jobState.jobKey, error.getMessage(), error);

            if (canRetry)
                log.error("The retry limit has been reached (this was attempt {} of {})", newState.retries, PROCESSING_RETRY_LIMIT);
            else
                log.error("This error is fatal and cannot be retried");

            log.error("Job will be updated to the next error state: [{}] {}", jobState.jobKey, nextErrorState);

            newState.cacheStatus = nextErrorState;
            newState.statusMessage = error.getMessage();
            newState.exception = error;

            // Reset retries for cleanup operation
            newState.retries = 0;

            return newState;
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
            CacheStatus.READY_FOR_CLEANUP,
            CacheStatus.READY_TO_REMOVE,
            CacheStatus.PROCESSING_FAILED);

    private static final List<Status.Code> GRPC_CAN_RETRY = List.of(
            Status.Code.UNAVAILABLE,
            Status.Code.DEADLINE_EXCEEDED);

    private static final List<Class<? extends ETrac>> TRAC_CAN_RETRY = List.of(
            EExecutorUnavailable.class);

    // States that are not acted on:
    // CacheStatus.SCHEDULED_TO_REMOVE
}
