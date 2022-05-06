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

package org.finos.tracdap.svc.orch.jobs;

import org.finos.tracdap.api.JobRequest;
import org.finos.tracdap.api.JobStatus;
import org.finos.tracdap.api.JobStatusRequest;
import org.finos.tracdap.api.TracOrchestratorApiGrpc;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.JobStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class Helpers {

    private static final List<JobStatusCode> COMPLETED_JOB_STATES = List.of(
            JobStatusCode.SUCCEEDED,
            JobStatusCode.FAILED,
            JobStatusCode.CANCELLED);


    private static final Logger log = LoggerFactory.getLogger(Helpers.class);

    public static JobStatus runJob(
            TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClient,
            JobRequest jobRequest) {

        var jobStatus = orchClient.submitJob(jobRequest);
        log.info("Job ID: [{}]", MetadataUtil.objectKey(jobStatus.getJobId()));
        log.info("Job status: [{}] {}", jobStatus.getStatusCode(), jobStatus.getStatusMessage());

        var statusRequest = JobStatusRequest.newBuilder()
                .setTenant(jobRequest.getTenant())
                .setSelector(MetadataUtil.selectorFor(jobStatus.getJobId()))
                .build();

        while (!COMPLETED_JOB_STATES.contains(jobStatus.getStatusCode())) {

            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

            jobStatus = orchClient.checkJob(statusRequest);
            log.info("Job status: [{}] {}", jobStatus.getStatusCode(), jobStatus.getStatusMessage());
        }

        if (jobStatus.getStatusCode() != JobStatusCode.SUCCEEDED) {

            var msg = String.format("Test job failed: [%s] %s",
                    MetadataUtil.objectKey(jobStatus.getJobId()),
                    jobStatus.getStatusMessage());

            throw new RuntimeException(msg);
        }

        return jobStatus;
    }
}
