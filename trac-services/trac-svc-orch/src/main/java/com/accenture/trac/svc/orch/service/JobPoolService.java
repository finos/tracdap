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

import com.accenture.trac.svc.orch.cache.IJobCache;
import com.accenture.trac.svc.orch.cache.TicketRequest;
import com.accenture.trac.svc.orch.exec.IBatchExecutor;
import com.accenture.trac.svc.orch.exec.JobExecState;

import java.util.concurrent.ScheduledExecutorService;


public class JobPoolService {



    interface IJobController {

    }

    private final ScheduledExecutorService executor;
    private final IJobCache jobCache;
    private final IBatchExecutor jobRunner;

    public JobPoolService(
            ScheduledExecutorService executor,
            IJobCache jobCache,
            IBatchExecutor jobRunner) {

        this.executor = executor;
        this.jobCache = jobCache;
        this.jobRunner = jobRunner;
    }


    void addJob() {


        //jobCache.createJob();





    }


    void pollCache() {

    }

    void pollExecutor() {

    }

    void submitJob(String jobKey) {

        var resourceTicket = TicketRequest.forResources();
        var jobTicket = TicketRequest.forJob();

        try (var ctx = jobCache.useTicket(resourceTicket, jobTicket)) {

            if (ctx.superseded())
                return;

            jobRunner.createBatchSandbox(jobKey);
//            jobRunner.writeTextConfig();
//            jobRunner.startBatch();
//
//            jobCache.updateJob();
        }
    }

    void jobCompleted(String jobKey) {

        var jobTicket = TicketRequest.forJob();

        try (var ctx = jobCache.useTicket(jobTicket)) {

            if (ctx.superseded())
                return;

            var jobState = new JobExecState();

            jobRunner.readBatchResult(jobKey, jobState);

            // process / record results

            jobRunner.readBatchResult(jobKey, jobState);
            jobRunner.cleanUpBatch(jobKey, jobState);

//            jobCache.deleteJob();
        }
    }

    void removeJob(String jobKey) {

        try (var ctx = jobCache.useTicket(TicketRequest.forJob())) {

            if (ctx.superseded())
                return;

            jobRunner.createBatchSandbox(jobKey);


            jobCache.deleteJob(jobKey, ctx.ticket());
        }
    }

    void cancelJob() {

    }




    void pollJobs() {

        // find pending jobs
        // find resource slots

        var resourceTicket = TicketRequest.forResources();
        var jobTicket = TicketRequest.forJob();

        try (var ctx = jobCache.useTicket(resourceTicket, jobTicket)) {


        }


    }
}
