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

import com.accenture.trac.svc.orch.cache.IJobCache;
import com.accenture.trac.svc.orch.cache.JobState;
import com.accenture.trac.svc.orch.cache.Ticket;
import com.accenture.trac.svc.orch.cache.TicketRequest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;


public class LocalJobCache implements IJobCache {

    private final Map<String, JobState> cache;

    public LocalJobCache() {

        this.cache = new ConcurrentHashMap<>();
    }

    @Override
    public void createJob(String jobId, JobState state) {

        var existing = cache.putIfAbsent(jobId, state);
    }

    @Override
    public JobState readJob(String jobId) {

        return cache.get(jobId);
    }

    @Override
    public void updateJob(String jobId, JobState state, Ticket ticket) {

        var newState = cache.computeIfPresent(jobId, (pk_, pv_) -> state);
    }

    @Override
    public void deleteJob(String jobId, Ticket ticket) {

        var existing = cache.remove(jobId);
    }

    @Override
    public List<JobState> pollJobs(Function<JobState, Boolean> filter) {

        return cache.values()
                .stream()
                .filter(filter::apply)
                .collect(Collectors.toList());
    }

    @Override
    public Ticket openTicket(TicketRequest request) {
        return null;
    }

    @Override
    public void closeTicket(Ticket ticket) {

    }
}
