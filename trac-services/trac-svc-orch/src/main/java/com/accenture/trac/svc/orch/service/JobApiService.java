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
import com.accenture.trac.common.exception.EMetadataNotFound;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.grpc.GrpcClientWrap;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.MetadataUtil;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

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

    public CompletionStage<JobStatus> checkJob(JobStatusRequest request) {

        // TODO: Keys for selectors
        var jobKey = String.format("%s-%s-v%d",
                request.getSelector().getObjectType(),
                request.getSelector().getObjectId(),
                request.getSelector().getObjectVersion());

        var cachedState = jobCache.readJob(jobKey);

        // TODO: Should there be a different error for jobs not found in the cache? EJobNotLive?
        if (cachedState == null) {
            var message = String.format("Job not found (it may have completed): [%s]", jobKey);
            log.error(message);
            throw new EMetadataNotFound(message);
        }

        var jobStatus = JobStatus.newBuilder()
                .setJobId(cachedState.jobId)
                .setStatus(cachedState.statusCode)
                .setMessage(cachedState.statusCode.toString())
                .build();

        return CompletableFuture.completedFuture(jobStatus);
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

        var jobKey = MetadataUtil.objectKey(jobState.jobId);

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

        return loadResources(jobState, resources);
    }

    private CompletionStage<JobState> loadResources(JobState jobState, List<TagSelector> resources) {

        log.info("Loading additional required metadata...");

        var orderedKeys = new ArrayList<String>(resources.size());
        var orderedSelectors = new ArrayList<TagSelector>(resources.size());

        for (var selector : resources) {
            orderedKeys.add(MetadataUtil.objectKey(selector));
            orderedSelectors.add(selector);
        }

        var batchRequest = MetadataBatchRequest.newBuilder()
                .setTenant(jobState.tenant)
                .addAllSelector(orderedSelectors)
                .build();

        return grpcWrap
                .unaryCall(READ_BATCH_METHOD, batchRequest, metaClient::readBatch)
                .thenCompose(batchResponse -> loadResourcesResponse(batchResponse, orderedKeys, jobState));
    }

    private CompletionStage<JobState> loadResourcesResponse(
            MetadataBatchResponse batchResponse,
            List<String> mappingKeys,
            JobState jobState) {

        if (batchResponse.getTagCount() != mappingKeys.size())
            throw new EUnexpected();

        var jobLogic = JobLogic.forJobType(jobState.jobType);

        var resources = new HashMap<String, ObjectDefinition>(mappingKeys.size());
        var mappings = new HashMap<String, String>(mappingKeys.size());

        for (var resourceIndex = 0; resourceIndex < mappingKeys.size(); resourceIndex++) {

            var resourceTag = batchResponse.getTag(resourceIndex);
            var resourceKey = MetadataUtil.objectKey(resourceTag.getHeader());
            var mappingKey = mappingKeys.get(resourceIndex);

            resources.put(resourceKey, resourceTag.getDefinition());

            if (!resourceKey.equals(mappingKey))
                mappings.put(mappingKey, resourceKey);
        }

        jobState.resources.putAll(resources);
        jobState.resourceMappings.putAll(mappings);

        var extraResources = jobLogic.requiredMetadata(resources);

        extraResources = extraResources.stream()
                .filter(selector -> {
                    var key = MetadataUtil.objectKey(selector);
                    return !jobState.resources.containsKey(key) && !jobState.resourceMappings.containsKey(key);
                })
                .collect(Collectors.toList());

        if (!extraResources.isEmpty())
            return loadResources(jobState, extraResources);
        else
            return CompletableFuture.completedFuture(jobState);
    }
}
