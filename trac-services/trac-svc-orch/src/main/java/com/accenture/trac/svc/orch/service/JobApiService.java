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

import com.accenture.trac.metadata.JobType;
import com.accenture.trac.svc.orch.cache.IJobCache;
import com.accenture.trac.svc.orch.cache.JobState;
import com.accenture.trac.svc.orch.jobs.JobLogic;
import io.grpc.MethodDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        var state = new RequestState(request);

        return CompletableFuture.completedFuture(state)

                .thenApply(s -> jobStatus(s, JobStatusCode.PREPARING))

                .thenCompose(this::assembleAndValidate)

                .thenApply(s -> jobStatus(s, JobStatusCode.VALIDATED))

                .thenApply(this::reportStatus);
    }

    public CompletableFuture<JobStatus> submitJob(JobRequest request) {

        var state = new RequestState(request);

        return CompletableFuture.completedFuture(state)

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



    private RequestState jobStatus(RequestState state, JobStatusCode statusCode) {

        state.statusCode = statusCode;
        return state;
    }


    private CompletionStage<RequestState> assembleAndValidate(RequestState state) {

        return CompletableFuture.completedFuture(state)

                .thenCompose(this::loadJobMetadata);

        // static validate
        // load related metadata
        // semantic validate
    }

    private RequestState buildMetadata(RequestState request) {

        var jobLogic = JobLogic.forJobType(request.jobType);



        return request;
    }

    private CompletionStage<RequestState> saveMetadata(RequestState request) {

        var jobObj = ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.JOB)
                .setJob(request.jobDef)
                .build();

        var ctrlJobAttrs = List.of(
                TagUpdate.newBuilder()
                        .setAttrName(TRAC_JOB_TYPE_ATTR)
                        .setValue(MetadataCodec.encodeValue(request.jobType.toString()))
                        .build(),
                TagUpdate.newBuilder()
                        .setAttrName(TRAC_JOB_STATUS_ATTR)
                        .setValue(MetadataCodec.encodeValue(request.statusCode.toString()))
                        .build());

        var freeJobAttrs = request.jobRequest.getJobAttrsList();

        var jobWriteReq = MetadataWriteRequest.newBuilder()
                .setTenant(request.tenant)
                .setObjectType(ObjectType.JOB)
                .setDefinition(jobObj)
                .addAllTagUpdates(ctrlJobAttrs)
                .addAllTagUpdates(freeJobAttrs)
                .build();

        var grpcCall = grpcWrap.unaryCall(
                CREATE_OBJECT_METHOD, jobWriteReq,
                metaClient::createObject);

        return grpcCall.thenApply(header -> {

            request.jobId = header;
            return request;
        });
    }

    private CompletionStage<RequestState> submitForExecution(RequestState request) {

        var jobKey = String.format("%s-v%d", request.jobId.getObjectId(), request.jobId.getObjectVersion());

        // TODO: Can RequestState just use a JobState?
        var jobState = new JobState();
        jobState.tenant = request.tenant;
        jobState.jobKey = jobKey;
        jobState.jobId = request.jobId;
        jobState.jobType = request.jobType;
        jobState.jobRequest = request.jobRequest;
        jobState.definition = request.jobDef;
        jobState.statusCode = JobStatusCode.QUEUED;

        jobCache.createJob(jobKey, jobState);

        request.statusCode = JobStatusCode.QUEUED;

        return CompletableFuture.completedFuture(request);
    }

    private JobStatus reportStatus(RequestState request) {

        var status = JobStatus.newBuilder();

        if (request.jobId != null)
            status.setJobId(request.jobId);

        status.setStatus(request.statusCode);

        return status.build();
    }



    private static class RequestState {

        RequestState(JobRequest request) {

            this.tenant = request.getTenant();
            this.jobType = request.getJob().getJobType();
            this.jobDef = request.getJob();
            this.jobRequest = request;
        }

        String tenant;
        JobType jobType;
        JobDefinition jobDef;
        JobRequest jobRequest;

        TagHeader jobId;
        ObjectDefinition target;
        Map<String, ObjectDefinition> resources = new HashMap<>();

        JobStatusCode statusCode;
    }

    private CompletionStage<RequestState> loadJobMetadata(RequestState state) {

        var jobLogic = JobLogic.forJobType(state.jobType);
        var resources = jobLogic.requiredMetadata(state.jobDef);

        if (resources.isEmpty()) {
            log.info("No additional metadata required");
            return CompletableFuture.completedFuture(state);
        }

        log.info("Loading additional required metadata...");

        var orderedKeys = new ArrayList<String>(resources.size());
        var orderedSelectors = new ArrayList<TagSelector>(resources.size());

        for (var resource : resources.entrySet()) {
            orderedKeys.add(resource.getKey());
            orderedSelectors.add(resource.getValue());
        }

        var batchRequest = MetadataBatchRequest.newBuilder()
                .setTenant(state.tenant)
                .addAllSelector(orderedSelectors)
                .build();

        return grpcWrap
                .unaryCall(READ_BATCH_METHOD, batchRequest, metaClient::readBatch)
                .thenApply(batchResponse -> loadJobMetadataResponse(batchResponse, orderedKeys, state));
    }

    private RequestState loadJobMetadataResponse(
            MetadataBatchResponse batchResponse,
            List<String> orderedKeys,
            RequestState state) {

        if (batchResponse.getTagCount() != orderedKeys.size())
            throw new EUnexpected();

        for (var resourceIndex = 0; resourceIndex < orderedKeys.size(); resourceIndex++) {

            var resourceKey = orderedKeys.get(resourceIndex);
            var resourceDef = batchResponse.getTag(resourceIndex).getDefinition();

            state.resources.put(resourceKey, resourceDef);
        }

        return state;
    }
}
