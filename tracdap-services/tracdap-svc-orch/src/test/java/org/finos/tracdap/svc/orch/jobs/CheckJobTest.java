/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
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
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


/**
 * Job status from {@code checkJob} for the selector forms clients send, for jobs in other tenants and for
 * jobs that don't exist, and the progress of a running job.
 */
@Tag("integration")
@Tag("int-e2e")
public class CheckJobTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String UNKNOWN_TENANT = "NO_SUCH_TENANT";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";

    private static final Set<JobStatusCode> COMPLETED_STATES = Set.of(
            JobStatusCode.SUCCEEDED, JobStatusCode.FAILED, JobStatusCode.CANCELLED);

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(E2E_CONFIG, List.of(E2E_TENANTS))
            .runDbDeploy(true)
            .runCacheDeploy(true)
            .addTenant(TEST_TENANT)
            .prepareLocalExecutor(true)
            .startService(TracMetadataService.class)
            .startService(TracDataService.class)
            .startService(TracOrchestratorService.class)
            .startService(TracAdminService.class)
            .build();

    @Test
    void checkJob_selectorForms() throws Exception {

        var jobId = startModelImport();

        var latestTagStatus = waitForCompletion(MetadataUtil.selectorFor(jobId, false, true));
        Assertions.assertEquals(JobStatusCode.SUCCEEDED, latestTagStatus.getStatusCode(), latestTagStatus.getStatusMessage());

        // The tag version the job was created with, a latest-object selector and an as-of selector all give
        // the same final status
        var asOf = DatetimeValue.newBuilder().setIsoDatetime(jobId.getTagTimestamp().getIsoDatetime());
        var asOfSelector = TagSelector.newBuilder()
                .setObjectType(ObjectType.JOB)
                .setObjectId(jobId.getObjectId())
                .setObjectAsOf(asOf)
                .setTagAsOf(asOf)
                .build();

        for (var selector : List.of(MetadataUtil.selectorFor(jobId), MetadataUtil.selectorForLatest(jobId), asOfSelector)) {

            var status = checkJob(TEST_TENANT, selector);

            Assertions.assertEquals(JobStatusCode.SUCCEEDED, status.getStatusCode());
            Assertions.assertEquals(jobId.getObjectId(), status.getJobId().getObjectId());
        }
    }

    @Test
    void checkJob_objectVersionTwoNotFound() throws Exception {

        var jobId = startModelImport();
        waitForCompletion(MetadataUtil.selectorFor(jobId));

        var selector = MetadataUtil.selectorFor(jobId).toBuilder().setObjectVersion(2).build();
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> checkJob(TEST_TENANT, selector));

        Assertions.assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void checkJob_unknownTenantNotFound() throws Exception {

        var jobId = startModelImport();

        // Asked straight after submission, while the job is held in the job cache
        var selector = MetadataUtil.selectorFor(jobId);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> checkJob(UNKNOWN_TENANT, selector));

        Assertions.assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        waitForCompletion(selector);
    }

    @Test
    void checkJob_unknownJobNotFound() {

        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.JOB)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> checkJob(TEST_TENANT, selector));

        Assertions.assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void checkJob_runningJobProgress() throws Exception {

        var releaseFile = Files.createTempDirectory("check_job_progress").resolve("release");
        var jobId = startWaitingJob(releaseFile);

        try {

            var running = waitForProgress(jobId, "Waiting for release");

            Assertions.assertEquals(JobStatusCode.RUNNING, running.getStatusCode());

            var modelStep = running.getProgress().getStepsList().stream()
                    .filter(step -> step.getStepType() == JobStepType.STEP_MODEL)
                    .findFirst().orElseThrow();

            Assertions.assertEquals(JobStepStatusCode.STEP_RUNNING, modelStep.getStatusCode());
            Assertions.assertTrue(modelStep.hasStartTime());

            var modelLine = running.getProgress().getLogEntriesList().stream()
                    .filter(entry -> entry.getMessage().equals("Waiting for release"))
                    .findFirst().orElseThrow();

            Assertions.assertEquals(JobLogSource.LOG_SOURCE_MODEL, modelLine.getSource());
            Assertions.assertEquals(JobStepType.STEP_MODEL, modelLine.getStepType());
        }
        finally {
            Files.createFile(releaseFile);
        }

        // A finished job has no progress section
        var finished = waitForCompletion(MetadataUtil.selectorFor(jobId));

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, finished.getStatusCode());
        Assertions.assertFalse(finished.hasProgress());
    }

    @Test
    void checkJob_pollBeforeSnapshotLogsNoError() throws Exception {

        var releaseFile = Files.createTempDirectory("check_job_progress").resolve("release");

        try (var errorLog = ErrorLogCapture.attach()) {

            // Polling starts as soon as the job is submitted, before the runtime writes its first snapshot
            var jobId = startWaitingJob(releaseFile);

            try {
                waitForProgress(jobId, "Waiting for release");
            }
            finally {
                Files.createFile(releaseFile);
            }

            waitForCompletion(MetadataUtil.selectorFor(jobId));

            Assertions.assertEquals(List.of(), errorLog.errorsMentioning("trac_job_progress"));
        }
    }

    private static TagHeader startWaitingJob(Path releaseFile) throws Exception {

        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("TRAC_LOCAL_REPO")
                .setPath("tracdap-services/tracdap-svc-orch/src/test/resources")
                .setEntryPoint("wait_for_release.WaitForRelease")
                .setVersion(GitHelpers.getCurrentCommit())
                .build();

        var modelTag = Helpers.doModelImport(platform, TEST_TENANT, modelStub, List.of(), List.of());

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_MODEL)
                        .setRunModel(org.finos.tracdap.metadata.RunModelJob.newBuilder()
                                .setModel(MetadataUtil.selectorFor(modelTag.getHeader()))
                                .putParameters("release_file", MetadataCodec.encodeValue(releaseFile.toString()))))
                .build();

        return Helpers.startJob(platform.orchClientBlocking(), jobRequest).getJobId();
    }

    private static JobStatus waitForProgress(TagHeader jobId, String logMessage) {

        var selector = MetadataUtil.selectorFor(jobId);
        var deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        var seen = new ArrayList<JobStatusCode>();

        // The first snapshot appears a little after the job starts running
        while (System.nanoTime() < deadline) {

            var status = checkJob(TEST_TENANT, selector);
            seen.add(status.getStatusCode());

            if (COMPLETED_STATES.contains(status.getStatusCode()))
                Assertions.fail("The job finished before its progress was seen: " + status.getStatusMessage());

            var logged = status.getProgress().getLogEntriesList().stream()
                    .anyMatch(entry -> entry.getMessage().equals(logMessage));

            if (logged)
                return status;

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(250));
        }

        return Assertions.fail("No progress was seen for the running job, statuses seen: " + seen);
    }

    private static TagHeader startModelImport() throws Exception {

        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("TRAC_LOCAL_REPO")
                .setPath("tracdap-services/tracdap-svc-orch/src/test/resources")
                .setEntryPoint("data_round_trip.DataRoundTripModel")
                .setVersion(GitHelpers.getCurrentCommit())
                .build();

        return Helpers.startModelImport(platform, TEST_TENANT, modelStub, List.of(), List.of());
    }

    private static JobStatus checkJob(String tenant, TagSelector selector) {

        var request = JobStatusRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(selector)
                .build();

        return platform.orchClientBlocking().checkJob(request);
    }

    private static JobStatus waitForCompletion(TagSelector selector) {

        var jobStatus = checkJob(TEST_TENANT, selector);

        while (!COMPLETED_STATES.contains(jobStatus.getStatusCode())) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            jobStatus = checkJob(TEST_TENANT, selector);
        }

        return jobStatus;
    }
}
