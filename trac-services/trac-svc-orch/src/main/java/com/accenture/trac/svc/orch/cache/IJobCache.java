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

package com.accenture.trac.svc.orch.cache;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public interface IJobCache {

    default TicketContext useTicket(TicketRequest... request) {

        var tickets = Arrays.stream(request).map(this::openTicket).toArray(Ticket[]::new);

        return new TicketContext(this, tickets);
    }

    Ticket openTicket(TicketRequest request);

    void closeTicket(Ticket ticket);

    void createJob(String jobKey, JobState state, Ticket ticket);

    JobState readJob(String jobKey);

    void updateJob(String jobKey, JobState state, Ticket ticket);

    void deleteJob(String jobKey, Ticket ticket);

    List<JobState> pollJobs(Function<JobState, Boolean> filter);


}
