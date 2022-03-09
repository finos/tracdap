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

import org.finos.tracdap.api.MetadataBatchRequest;
import org.finos.tracdap.api.MetadataBatchResponse;
import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.api.TrustedMetadataApiGrpc;
import com.accenture.trac.common.exception.EInputValidation;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.grpc.GrpcClientWrap;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.MetadataConstants;
import com.accenture.trac.common.metadata.MetadataUtil;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.RuntimeConfig;
import org.finos.tracdap.config.StorageSettings;
import org.finos.tracdap.metadata.*;
import com.accenture.trac.svc.orch.cache.JobState;
import com.accenture.trac.svc.orch.jobs.JobLogic;
import io.grpc.MethodDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static com.accenture.trac.common.metadata.MetadataCodec.encodeValue;
import static com.accenture.trac.common.metadata.MetadataConstants.*;
import static com.accenture.trac.common.metadata.MetadataConstants.TRAC_CREATE_JOB;
import static com.accenture.trac.common.metadata.MetadataUtil.selectorFor;

public class JobLifecycle {

    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> CREATE_OBJECT_METHOD = TrustedMetadataApiGrpc.getCreateObjectMethod();
    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> UPDATE_OBJECT_METHOD = TrustedMetadataApiGrpc.getUpdateObjectMethod();
    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> UPDATE_TAG_METHOD = TrustedMetadataApiGrpc.getUpdateTagMethod();
    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> PREALLOCATE_ID_METHOD = TrustedMetadataApiGrpc.getPreallocateIdMethod();
    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> CREATE_PREALLOCATED_OBJECT_METHOD = TrustedMetadataApiGrpc.getCreatePreallocatedObjectMethod();
    private static final MethodDescriptor<MetadataBatchRequest, MetadataBatchResponse> READ_BATCH_METHOD = TrustedMetadataApiGrpc.getReadBatchMethod();

    private final Logger log = LoggerFactory.getLogger(JobLifecycle.class);

    private final PlatformConfig platformConfig;
    private final TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient;
    private final GrpcClientWrap grpcWrap;

    public JobLifecycle(
            PlatformConfig platformConfig,
            TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient) {

        this.platformConfig = platformConfig;
        this.metaClient = metaClient;
        this.grpcWrap = new GrpcClientWrap(getClass());
    }

    CompletionStage<JobState> assembleAndValidate(JobState jobState) {

        return CompletableFuture.completedFuture(jobState)

                .thenApply(this::convertFlowToModel)

                .thenCompose(this::loadResources)
                .thenCompose(this::allocateResultIds)
                .thenApply(this::buildJobConfig);

        // static validate
        // semantic validate
    }

   JobState convertFlowToModel(JobState jobState) {

        if (jobState.definition.getJobType() != JobType.RUN_FLOW)
            return jobState;

        var runFlow = jobState.definition.getRunFlow();

        if (runFlow.getModelsCount() != 1)
            throw new EInputValidation("Run flow must be supplied with a single model for the preview implementation");

        var modelSelector = runFlow.getModelsMap().values().stream().findFirst();

        if (modelSelector.isEmpty())
            throw new EInputValidation("Run flow must be supplied with a single model for the preview implementation");

        var runModel = RunModelJob.newBuilder()
                .setModel(modelSelector.get())
                .putAllParameters(runFlow.getParametersMap())
                .putAllInputs(runFlow.getInputsMap())
                .putAllOutputs(runFlow.getOutputsMap())
                .putAllPriorOutputs(runFlow.getPriorOutputsMap())
                .addAllOutputAttrs(runFlow.getOutputAttrsList())
                .build();

        var jobDef = jobState.definition.toBuilder()
                .setJobType(JobType.IMPORT_MODEL)
                .setRunModel(runModel)
                .build();

        var newState = jobState.clone();
        newState.definition = jobDef;

        return newState;
    }

    CompletionStage<JobState> loadResources(JobState jobState) {

        var jobLogic = JobLogic.forJobType(jobState.jobType);
        var resources = jobLogic.requiredMetadata(jobState.definition);

        if (resources.isEmpty()) {
            log.info("No additional metadata required");
            return CompletableFuture.completedFuture(jobState);
        }

        return loadResources(jobState, resources);
    }

    CompletionStage<JobState> loadResources(JobState jobState, List<TagSelector> resources) {

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
                .thenCompose(batchResponse -> loadResourcesResponse(jobState, orderedKeys, batchResponse));
    }

    CompletionStage<JobState> loadResourcesResponse(
            JobState jobState, List<String> mappingKeys,
            MetadataBatchResponse batchResponse) {

        if (batchResponse.getTagCount() != mappingKeys.size())
            throw new EUnexpected();

        var jobLogic = JobLogic.forJobType(jobState.jobType);

        var resources = new HashMap<String, ObjectDefinition>(mappingKeys.size());
        var mappings = new HashMap<String, TagHeader>(mappingKeys.size());

        for (var resourceIndex = 0; resourceIndex < mappingKeys.size(); resourceIndex++) {

            var resourceTag = batchResponse.getTag(resourceIndex);
            var resourceKey = MetadataUtil.objectKey(resourceTag.getHeader());
            var mappingKey = mappingKeys.get(resourceIndex);

            resources.put(resourceKey, resourceTag.getDefinition());
            mappings.put(mappingKey, resourceTag.getHeader());
        }

        jobState.resources.putAll(resources);
        jobState.resourceMapping.putAll(mappings);

        var extraResources = jobLogic.requiredMetadata(resources).stream()
                .filter(selector -> !jobState.resources.containsKey(MetadataUtil.objectKey(selector)))
                .filter(selector -> !jobState.resourceMapping.containsKey(MetadataUtil.objectKey(selector)))
                .collect(Collectors.toList());

        if (!extraResources.isEmpty())
            return loadResources(jobState, extraResources);

        return CompletableFuture.completedFuture(jobState);
    }

    CompletionStage<JobState> allocateResultIds(JobState jobState) {

        // TODO: Single job timestamp - requires changes in meta svc for this to actually be used
        // meta svc must accept object timestamps as out-of-band gRPC metadata for trusted API calls
        var jobTimestamp = Instant.now();

        var jobLogic = JobLogic.forJobType(jobState.jobType);

        var priorResultIds = jobLogic.priorResultIds(
                jobState.definition,
                jobState.resources, jobState.resourceMapping);

        var newResultIds = jobLogic.newResultIds(
                jobState.tenant, jobState.definition,
                jobState.resources, jobState.resourceMapping);

        for (var priorId : priorResultIds.entrySet()) {

            var resultId = MetadataUtil.nextObjectVersion(priorId.getValue(), jobTimestamp);
            jobState.resultMapping.put(priorId.getKey(), resultId);
        }

        CompletionStage<JobState> state_ = CompletableFuture.completedFuture(jobState);

        for (var idRequest : newResultIds.entrySet()) {

            state_ = state_.thenCompose(s -> allocateResultId(s, jobTimestamp, idRequest.getKey(), idRequest.getValue()));
        }

        return state_.thenApply(this::setResultIds);
    }

    CompletionStage<JobState> allocateResultId(
            JobState jobState, Instant jobTimestamp,
            String resultKey, MetadataWriteRequest idRequest) {

        var preallocateResult = grpcWrap.unaryCall(PREALLOCATE_ID_METHOD, idRequest, metaClient::preallocateId);

        return preallocateResult.thenApply(preallocatedId -> {

            var resultId = MetadataUtil.nextObjectVersion(preallocatedId, jobTimestamp);
            jobState.resultMapping.put(resultKey, resultId);

            return jobState;
        });
    }

    JobState setResultIds(JobState jobState) {

        var jobLogic = JobLogic.forJobType(jobState.jobType);

        jobState.definition = jobLogic.setResultIds(
                jobState.definition, jobState.resultMapping,
                jobState.resources, jobState.resourceMapping);

        return jobState;
    }

    JobState buildJobConfig(JobState jobState) {

        jobState.jobConfig = JobConfig.newBuilder()
                //.setJobId(jobState.jobId)
                .setJob(jobState.definition)
                .putAllResources(jobState.resources)
                .putAllResourceMapping(jobState.resourceMapping)
                .putAllResultMapping(jobState.resultMapping)
                .build();

        jobState.sysConfig = RuntimeConfig.newBuilder()
                .setStorageSettings(StorageSettings.newBuilder()
                .setDefaultStorage(platformConfig.getServices().getData().getDefaultStorageKey())
                .setDefaultFormat(platformConfig.getServices().getData().getDefaultStorageFormat()))
                .putAllStorage(platformConfig.getServices().getData().getStorageMap())
                .putAllRepositories(platformConfig.getServices().getOrch().getRepositoriesMap())
                .build();

        return jobState;
    }

    CompletionStage<JobState> saveInitialMetadata(JobState jobState) {

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

            jobState.jobConfig = jobState.jobConfig.toBuilder()
                    .setJobId(header)
                    .build();

            return jobState;
        });
    }

    CompletionStage<Void> processJobResult(JobState jobState) {

        log.info("Record job result [{}]: {}", jobState.jobKey, jobState.statusCode);

        var jobLogic = JobLogic.forJobType(jobState.jobType);
        var metaUpdates = jobLogic.buildResultMetadata(jobState.tenant, jobState.jobConfig, jobState.jobResult);
        var jobUpdate = buildJobSucceededUpdate(jobState);

        CompletionStage<?> updateResult = CompletableFuture.completedFuture(0);

        for (var update : metaUpdates) {

            var update_ = applyJobAttrs(jobState, update);
            updateResult = updateResult.thenCompose(x -> saveResultMetadata(update_));
        }

        updateResult = updateResult.thenCompose(x -> saveResultMetadata(jobUpdate));

        return updateResult.thenApply(x -> null);
    }

    private CompletionStage<TagHeader> saveResultMetadata(MetadataWriteRequest update) {

        if (!update.hasDefinition())
            return grpcWrap.unaryCall(UPDATE_TAG_METHOD, update, metaClient::updateTag);

        if (!update.hasPriorVersion())
            return grpcWrap.unaryCall(CREATE_OBJECT_METHOD, update, metaClient::createObject);

        if (update.getPriorVersion().getObjectVersion() < MetadataConstants.OBJECT_FIRST_VERSION)
            return grpcWrap.unaryCall(CREATE_PREALLOCATED_OBJECT_METHOD, update, metaClient::createPreallocatedObject);
        else
            return grpcWrap.unaryCall(UPDATE_OBJECT_METHOD, update, metaClient::updateObject);
    }

    private MetadataWriteRequest buildJobSucceededUpdate(JobState jobState) {

        var attrUpdates = List.of(
                TagUpdate.newBuilder()
                        .setAttrName(TRAC_JOB_STATUS_ATTR)
                        .setValue(encodeValue(jobState.statusCode.toString()))
                        .build());

        return MetadataWriteRequest.newBuilder()
                .setTenant(jobState.tenant)
                .setObjectType(ObjectType.JOB)
                .setPriorVersion(selectorFor(jobState.jobId))
                .addAllTagUpdates(attrUpdates)
                .build();
    }

    private MetadataWriteRequest applyJobAttrs(JobState jobState, MetadataWriteRequest request) {

        if (!request.hasDefinition())
            return request;

        var builder = request.toBuilder();

        builder.addTagUpdates(TagUpdate.newBuilder()
                .setAttrName(TRAC_UPDATE_JOB)
                .setValue(MetadataCodec.encodeValue(jobState.jobKey)));

        if (!request.hasPriorVersion() || request.getPriorVersion().getObjectVersion() == 0) {

            builder.addTagUpdates(TagUpdate.newBuilder()
                    .setAttrName(TRAC_CREATE_JOB)
                    .setValue(MetadataCodec.encodeValue(jobState.jobKey)));
        }

        return builder.build();
    }
}
