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

import com.accenture.trac.api.*;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.grpc.GrpcClientWrap;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.metadata.*;

import com.accenture.trac.svc.orch.cache.IJobCache;
import com.accenture.trac.svc.orch.cache.JobState;
import com.accenture.trac.svc.orch.jobs.JobLogic;
import io.grpc.MethodDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.accenture.trac.common.metadata.MetadataConstants.TRAC_JOB_STATUS_ATTR;
import static com.accenture.trac.common.metadata.MetadataConstants.TRAC_JOB_TYPE_ATTR;


public class JobApiService {

    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> CREATE_OBJECT_METHOD = TrustedMetadataApiGrpc.getCreateObjectMethod();
    private static final MethodDescriptor<MetadataBatchRequest, MetadataBatchResponse> READ_BATCH_METHOD = TrustedMetadataApiGrpc.getReadBatchMethod();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IJobCache jobCache;
    private final TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient;
    private final GrpcClientWrap grpcWrap;

    public JobApiService(IJobCache jobCache, TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient) {

        this.jobCache = jobCache;
        this.metaClient = metaClient;
        this.grpcWrap = new GrpcClientWrap(getClass());
    }

    public CompletableFuture<JobStatus> validateJob(JobRequest request) {

        var jobState = newJob(request);

        return CompletableFuture.completedFuture(jobState)

                .thenApply(s -> jobStatus(s, JobStatusCode.PREPARING))

                .thenCompose(this::assembleAndValidate)

                .thenApply(s -> jobStatus(s, JobStatusCode.VALIDATED))

                .thenApply(this::reportStatus);
    }

    public CompletableFuture<JobStatus> submitJob(JobRequest request) {

        var jobState = newJob(request);

        return CompletableFuture.completedFuture(jobState)

                .thenApply(s -> jobStatus(s, JobStatusCode.PREPARING))

                .thenCompose(this::assembleAndValidate)

                .thenApply(s -> jobStatus(s, JobStatusCode.VALIDATED))
                .thenApply(s -> jobStatus(s, JobStatusCode.PENDING))

                .thenCompose(this::saveMetadata)

                .thenCompose(this::submitForExecution)

                .thenApply(this::reportStatus);
    }

    void checkJob() {

    }

    void getJobResult() {

    }


    private JobState newJob(JobRequest request) {

        var jobState = new JobState();
        jobState.tenant = request.getTenant();
        jobState.jobType = request.getJob().getJobType();
        jobState.definition = request.getJob();
        jobState.jobRequest = request;

        return jobState;
    }

    private JobState jobStatus(JobState jobState, JobStatusCode statusCode) {

        jobState.statusCode = statusCode;
        return jobState;
    }


    private CompletionStage<JobState> assembleAndValidate(JobState jobState) {

        return CompletableFuture.completedFuture(jobState)

                .thenCompose(this::loadResources);

        // static validate
        // load related metadata
        // semantic validate
    }

    private JobState buildMetadata(JobState jobState) {

        var jobLogic = JobLogic.forJobType(jobState.jobType);



        return jobState;
    }

    private CompletionStage<JobState> saveMetadata(JobState jobState) {

        var jobObj = ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.JOB)
                .setJob(jobState.definition)
                .build();

        var ctrlJobAttrs = List.of(
                TagUpdate.newBuilder()
                        .setAttrName(TRAC_JOB_TYPE_ATTR)
                        .setValue(MetadataCodec.encodeValue(jobState.jobType.toString()))
                        .build(),
                TagUpdate.newBuilder()
                        .setAttrName(TRAC_JOB_STATUS_ATTR)
                        .setValue(MetadataCodec.encodeValue(jobState.statusCode.toString()))
                        .build());

        var freeJobAttrs = jobState.jobRequest.getJobAttrsList();

        var jobWriteReq = MetadataWriteRequest.newBuilder()
                .setTenant(jobState.tenant)
                .setObjectType(ObjectType.JOB)
                .setDefinition(jobObj)
                .addAllTagUpdates(ctrlJobAttrs)
                .addAllTagUpdates(freeJobAttrs)
                .build();

        var grpcCall = grpcWrap.unaryCall(
                CREATE_OBJECT_METHOD, jobWriteReq,
                metaClient::createObject);

        return grpcCall.thenApply(header -> {

            jobState.jobId = header;
            return jobState;
        });
    }

    private CompletionStage<JobState> submitForExecution(JobState jobState) {

        // TODO: Centralize this
        var jobKey = String.format("%s-v%d", jobState.jobId.getObjectId(), jobState.jobId.getObjectVersion());

        jobState.jobKey = jobKey;
        jobState.statusCode = JobStatusCode.QUEUED;

        jobCache.createJob(jobKey, jobState);

        return CompletableFuture.completedFuture(jobState);
    }

    private JobStatus reportStatus(JobState jobState) {

        var status = JobStatus.newBuilder();

        if (jobState.jobId != null)
            status.setJobId(jobState.jobId);

        status.setStatus(jobState.statusCode);

        return status.build();
    }

    private CompletionStage<JobState> loadResources(JobState jobState) {

        var jobLogic = JobLogic.forJobType(jobState.jobType);
        var resources = jobLogic.requiredMetadata(jobState.definition);

        if (resources.isEmpty()) {
            log.info("No additional metadata required");
            return CompletableFuture.completedFuture(jobState);
        }

        log.info("Loading additional required metadata...");

        var orderedKeys = new ArrayList<String>(resources.size());
        var orderedSelectors = new ArrayList<TagSelector>(resources.size());

        for (var resource : resources.entrySet()) {
            orderedKeys.add(resource.getKey());
            orderedSelectors.add(resource.getValue());
        }

        var batchRequest = MetadataBatchRequest.newBuilder()
                .setTenant(jobState.tenant)
                .addAllSelector(orderedSelectors)
                .build();

        return grpcWrap
                .unaryCall(READ_BATCH_METHOD, batchRequest, metaClient::readBatch)
                .thenApply(batchResponse -> loadResourcesResponse(batchResponse, orderedKeys, jobState));
    }

    private JobState loadResourcesResponse(
            MetadataBatchResponse batchResponse,
            List<String> orderedKeys,
            JobState jobState) {

        if (batchResponse.getTagCount() != orderedKeys.size())
            throw new EUnexpected();

        var jobLogic = JobLogic.forJobType(jobState.jobType);

        var resourceMapping = new HashMap<String, TagHeader>(orderedKeys.size());
        var resourceDefinitions = new HashMap<String, ObjectDefinition>(orderedKeys.size());

        for (var resourceIndex = 0; resourceIndex < orderedKeys.size(); resourceIndex++) {

            var resourceKey = orderedKeys.get(resourceIndex);
            var resource = batchResponse.getTag(resourceIndex);

            resourceMapping.put(resourceKey, resource.getHeader());
            resourceDefinitions.put(resourceKey, resource.getDefinition());
        }

        jobState.resources = resourceDefinitions;
        jobState.definition = jobLogic.freezeResources(jobState.definition, resourceMapping);

        return jobState;
    }
}
