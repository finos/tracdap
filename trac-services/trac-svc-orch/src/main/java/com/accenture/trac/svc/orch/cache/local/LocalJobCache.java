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

package com.accenture.trac.svc.orch.cache.local;

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.svc.orch.cache.IJobCache;
import com.accenture.trac.svc.orch.cache.JobState;
import com.accenture.trac.svc.orch.cache.Ticket;
import com.accenture.trac.svc.orch.cache.TicketRequest;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;


public class LocalJobCache implements IJobCache {

    private final ConcurrentMap<String, LocalJobCacheEntry> cache;

    public LocalJobCache() {

        this.cache = new ConcurrentHashMap<>();
    }

    @Override
    public Ticket openTicket(TicketRequest request) {

        var operationTime = Instant.now();
        var duration = Duration.of(1, ChronoUnit.MINUTES);
        var ticket = Ticket.forDuration(request.jobKey(), operationTime, duration);

        var cacheEntry = cache.compute(request.jobKey(), (key, priorEntry) -> {

            var priorTicket = priorEntry != null ? priorEntry.ticket : null;

            if (priorTicket != null && operationTime.isBefore(priorTicket.expiry()))
                return priorEntry;

            var newEntry = priorEntry != null ? priorEntry.clone() : new LocalJobCacheEntry();
            newEntry.ticket = ticket;

            return newEntry;
        });

        if (cacheEntry.ticket != ticket)
            return Ticket.superseded(request.jobKey(), operationTime);

        return ticket;
    }

    @Override
    public void closeTicket(Ticket ticket) {

        cache.computeIfPresent(ticket.key(), (key, priorEntry) -> {

            if (priorEntry.ticket != ticket)
                return priorEntry;

            // If there is no job state associated with this cache entry, then remove it from the cache
            // This happens when an entry is deleted, or if a create operation fails
            if (priorEntry.jobState == null)
                return null;

            var newEntry = priorEntry.clone();
            newEntry.ticket = null;

            return newEntry;
        });
    }

    @Override
    public void createJob(String jobKey, JobState jobState, Ticket ticket) {

        var operationTime = Instant.now();

        var cacheEntry = cache.computeIfPresent(jobKey, (key, priorEntry) -> {

            if (priorEntry.ticket != ticket)
                return priorEntry;

            if (priorEntry.jobState != null)
                throw new EUnexpected();

            var newEntry = priorEntry.clone();
            newEntry.jobState = jobState;
            newEntry.lastActivity = operationTime;
            newEntry.revision = priorEntry.revision + 1;

            return newEntry;
        });

        if (cacheEntry == null || cacheEntry.ticket != ticket)
            throw new EUnexpected();  // todo: error handling?

    }

    @Override
    public JobState readJob(String jobKey) {

        return cache.get(jobKey).jobState.clone();
    }

    @Override
    public void updateJob(String jobKey, JobState jobState, Ticket ticket) {

        var operationTime = Instant.now();

        var cacheEntry = cache.compute(jobKey, (key, priorEntry) -> {

            if (priorEntry == null)
                throw new EUnexpected();

            if (priorEntry.ticket != ticket)
                return priorEntry;

            if (priorEntry.jobState == null)
                throw new EUnexpected();

            var newEntry = priorEntry.clone();
            newEntry.jobState = jobState;
            newEntry.lastActivity = operationTime;
            newEntry.revision = priorEntry.revision + 1;

            return newEntry;
        });

        if (cacheEntry.ticket != ticket)
            throw new EUnexpected();  // todo: error handling?
    }

    @Override
    public void deleteJob(String jobKey, Ticket ticket) {

        var operationTime = Instant.now();

        var cacheEntry = cache.computeIfPresent(jobKey, (key, priorEntry) -> {

            if (priorEntry.ticket != ticket)
                return priorEntry;

            if (priorEntry.jobState == null)
                throw new EUnexpected();

            var newEntry = priorEntry.clone();
            newEntry.jobState = null;
            newEntry.lastActivity = operationTime;
            newEntry.revision = priorEntry.revision + 1;

            return newEntry;
        });

        if (cacheEntry != null && cacheEntry.ticket != ticket)
            throw new EUnexpected();  // todo: error handling?
    }

    @Override
    public List<JobState> pollJobs(Function<JobState, Boolean> filter) {

        var pollWindow = Duration.of(2, ChronoUnit.SECONDS);

        var operationTime = Instant.now();
        var pollResult = new ArrayList<JobState>();

        cache.forEach((key, entry) -> {

            if (entry.ticket != null && operationTime.isBefore(entry.ticket.expiry()))
                return;

            if (entry.jobState == null || entry.lastActivity == null)
                return;

            if (entry.lastPoll != null && entry.lastPoll.isAfter(entry.lastActivity)) {

                var pollExpiry = entry.lastPoll.plus(pollWindow);

                if (operationTime.isBefore(pollExpiry))
                    return;
            }

            if (filter.apply(entry.jobState))
                pollResult.add(entry.jobState);
        });

        return pollResult;
    }
}
