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

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.metadata.ImportModelJob;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.junit.jupiter.api.Assertions;
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

        var initialStatus = startJob(orchClient, jobRequest);
        return waitForJob(orchClient, jobRequest.getTenant(), initialStatus.getJobId());
    }

    public static JobStatus startJob(
            TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClient,
            JobRequest jobRequest) {

        var initialStatus = orchClient.submitJob(jobRequest);

        log.info("Job ID: [{}]", MetadataUtil.objectKey(initialStatus.getJobId()));
        log.info("Job initial status: [{}] {}", initialStatus.getStatusCode(), initialStatus.getStatusMessage());

        if (initialStatus.getStatusCode() == JobStatusCode.FAILED) {

            var msg = String.format("Test job failed: [%s] %s",
                    MetadataUtil.objectKey(initialStatus.getJobId()),
                    initialStatus.getStatusMessage());

            throw new RuntimeException(msg);
        }

        return initialStatus;
    }

    public static JobStatus waitForJob(
            TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClient,
            String tenant, TagHeader jobId) {

        var statusRequest = JobStatusRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(MetadataUtil.selectorFor(jobId))
                .build();

        var jobStatus = orchClient.checkJob(statusRequest);

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

    public static Tag doModelImport(
            PlatformTest platform, String tenant, ModelDefinition stubModel,
            List<TagUpdate> modelAttrs, List<TagUpdate> jobAttrs) {

        var jobId = startModelImport(platform, tenant, stubModel, modelAttrs, jobAttrs);
        return waitForModelImport(platform, tenant, jobId);
    }

    public static TagHeader startModelImport(
            PlatformTest platform, String tenant, ModelDefinition stubModel,
            List<TagUpdate> modelAttrs, List<TagUpdate> jobAttrs) {

        var orchClient = platform.orchClientBlocking();

        var importModel = ImportModelJob.newBuilder()
                .setLanguage(stubModel.getLanguage())
                .setRepository(stubModel.getRepository())
                .setPath(stubModel.getPath())
                .setEntryPoint(stubModel.getEntryPoint())
                .setVersion(stubModel.getVersion())
                .addAllModelAttrs(modelAttrs)
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(tenant)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_MODEL)
                        .setImportModel(importModel))
                .addAllJobAttrs(jobAttrs)
                .build();

        // Developer note: This test will fail running locally if the latest commit is not pushed to GitHub
        // It doesn't need to be merged, but the commit must exist on your origin / fork

        var jobStatus = startJob(orchClient, jobRequest);

        return jobStatus.getJobId();
    }

    public static org.finos.tracdap.metadata.Tag waitForModelImport(
            PlatformTest platform, String tenant, TagHeader jobId) {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var jobStatus = waitForJob(orchClient, tenant, jobId);
        var jobKey = MetadataUtil.objectKey(jobId);

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        var modelSearch = MetadataSearchRequest.newBuilder()
                .setTenant(tenant)
                .setSearchParams(SearchParameters.newBuilder()
                        .setObjectType(ObjectType.MODEL)
                        .setSearch(SearchExpression.newBuilder()
                                .setTerm(SearchTerm.newBuilder()
                                        .setAttrName("trac_create_job")
                                        .setAttrType(BasicType.STRING)
                                        .setOperator(SearchOperator.EQ)
                                        .setSearchValue(MetadataCodec.encodeValue(jobKey)))))
                .build();

        var modelSearchResult = metaClient.search(modelSearch);

        Assertions.assertEquals(1, modelSearchResult.getSearchResultCount());

        var searchResult = modelSearchResult.getSearchResult(0);
        var modelReq = MetadataReadRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(MetadataUtil.selectorFor(searchResult.getHeader()))
                .build();

        return metaClient.readObject(modelReq);
    }
}
