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

package org.finos.tracdap.svc.orch.service;

import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.TrustedMetadataApiGrpc;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.RuntimeConfig;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.orch.jobs.JobLogic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.finos.tracdap.common.metadata.MetadataCodec.encodeValue;
import static org.finos.tracdap.common.metadata.MetadataConstants.*;
import static org.finos.tracdap.common.metadata.MetadataUtil.selectorFor;


public class JobProcessorHelpers {

    private static final String JOB_RESULT_KEY = "trac_job_result";
    private static final String JOB_LOG_FILE_KEY = "trac_job_log_file";
    private static final String JOB_LOG_STORAGE_KEY = "trac_job_log_file:STORAGE";

    private final Logger log = LoggerFactory.getLogger(JobProcessorHelpers.class);

    private final PlatformConfig platformConfig;
    private final TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub metaClient;


    public JobProcessorHelpers(
            PlatformConfig platformConfig,
            TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub metaClient) {

        this.platformConfig = platformConfig;
        this.metaClient = metaClient;
    }

    /**
     * Add a request without sending it.
     */
    private static void addUpdateToWriteBatch(MetadataWriteBatchRequest.Builder builder, MetadataWriteRequest update) {
        if (!update.hasDefinition()) {
            builder.addUpdateTags(update);
        } else if (!update.hasPriorVersion()) {
            builder.addCreateObjects(update);
        } else if (update.getPriorVersion().getObjectVersion() < OBJECT_FIRST_VERSION) {
            builder.addCreatePreallocatedObjects(update);
        } else {
            builder.addUpdateObjects(update);
        }
    }

    /**
     * Remove tenant from write request.
     * Necessary when you want to add the request to a batch write request.
     */
    private static MetadataWriteRequest scrapTenant(MetadataWriteRequest request) {
        return MetadataWriteRequest.newBuilder(request)
                .clearTenant().build();
    }

    JobState applyTransform(JobState jobState) {

        var logic = JobLogic.forJobType(jobState.jobType);
        var metadata = new MetadataBundle(jobState.resources, jobState.resourceMapping);

        jobState.definition = logic.applyTransform(jobState.definition, metadata, platformConfig);

        var updatedMetadata = logic.applyMetadataTransform(jobState.definition, metadata, platformConfig);
        jobState.resources = updatedMetadata.getResources();
        jobState.resourceMapping = updatedMetadata.getResourceMapping();

        return jobState;
    }

    JobState loadResources(JobState jobState) {

        var jobLogic = JobLogic.forJobType(jobState.jobType);
        var resources = jobLogic.requiredMetadata(jobState.definition);

        if (resources.isEmpty()) {
            log.info("No additional metadata required");
            return jobState;
        }

        return loadResources(jobState, resources);
    }

    JobState loadResources(JobState jobState, List<TagSelector> resources) {

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

        var client = metaClient.withCallCredentials(jobState.credentials);
        var batchResponse = client.readBatch(batchRequest);

        return loadResourcesResponse(jobState, orderedKeys, batchResponse);
    }

    JobState loadResourcesResponse(
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

        return jobState;
    }

    JobState allocateResultIds(JobState jobState) {

        // TODO: Single job timestamp - requires changes in meta svc for this to actually be used
        // meta svc must accept object timestamps as out-of-band gRPC metadata for trusted API calls
        var jobTimestamp = Instant.now();

        var newResultIds = new HashMap<String, MetadataWriteRequest>();

        var resultId = MetadataWriteRequest.newBuilder()
                .setObjectType(ObjectType.RESULT)
                .build();

        var logFileId = MetadataWriteRequest.newBuilder()
                .setObjectType(ObjectType.FILE)
                .build();

        var logStorageId = MetadataWriteRequest.newBuilder()
                .setObjectType(ObjectType.STORAGE)
                .build();

        var jobLogic = JobLogic.forJobType(jobState.jobType);

        var priorResultIds = jobLogic.priorResultIds(
                jobState.definition,
                jobState.resources, jobState.resourceMapping);

        var jobResultIds = jobLogic.newResultIds(
                jobState.tenant, jobState.definition,
                jobState.resources, jobState.resourceMapping);

        newResultIds.put(JOB_RESULT_KEY, resultId);
        newResultIds.put(JOB_LOG_FILE_KEY, logFileId);
        newResultIds.put(JOB_LOG_STORAGE_KEY, logStorageId);
        newResultIds.putAll(jobResultIds);

        for (var priorId : priorResultIds.entrySet()) {

            var nextId = MetadataUtil.nextObjectVersion(priorId.getValue(), jobTimestamp);
            jobState.resultMapping.put(priorId.getKey(), nextId);
        }

        jobState = allocateResultIds(jobState, jobTimestamp, newResultIds);

        return setResultIds(jobState);
    }

    JobState allocateResultIds(
            JobState jobState, Instant jobTimestamp,
            Map<String, MetadataWriteRequest> newResultIds) {

        if (newResultIds.isEmpty()) {
            return jobState;
        }

        var keys = new ArrayList<>(newResultIds.keySet());
        var requests = new ArrayList<MetadataWriteRequest>();

        for (var key : keys) {
            var request = newResultIds.get(key);
            request = scrapTenant(request);

            requests.add(request);
        }

        var request = MetadataWriteBatchRequest.newBuilder()
                .setTenant(jobState.tenant)
                .addAllPreallocateIds(requests)
                .build();

        var client = metaClient.withCallCredentials(jobState.credentials);

        var preallocatedIds = client.writeBatch(request)
                .getPreallocateIdsList();

        for (int i = 0; i < keys.size(); i++) {
            var resultKey = keys.get(i);
            var preallocatedId = preallocatedIds.get(i);

            var resultId = MetadataUtil.nextObjectVersion(preallocatedId, jobTimestamp);

            jobState.resultMapping.put(resultKey, resultId);
        }

        return jobState;
    }

    JobState setResultIds(JobState jobState) {

        var jobLogic = JobLogic.forJobType(jobState.jobType);

        jobState.definition = jobLogic.setResultIds(
                jobState.definition, jobState.resultMapping,
                jobState.resources, jobState.resourceMapping);

        return jobState;
    }

    JobState buildJobConfig(JobState jobState) {

        var resultId = jobState.resultMapping.get(JOB_RESULT_KEY);

        jobState.definition = jobState.definition.toBuilder()
                .setResultId(MetadataUtil.selectorFor(resultId))
                .build();

        // Do not set jobId, it is not available yet
        jobState.jobConfig = JobConfig.newBuilder()
                .setJob(jobState.definition)
                .putAllResources(jobState.resources)
                .putAllResourceMapping(jobState.resourceMapping)
                .putAllResultMapping(jobState.resultMapping)
                .build();

        var storageConfig = platformConfig.getStorage();

        // Pass down tenant storage overrides if they are configured (the runtime doesn't know about tenants atm)
        if (platformConfig.containsTenants(jobState.tenant)) {

            var tenantConfig = platformConfig.getTenantsOrThrow(jobState.tenant);

            var storageUpdate = storageConfig.toBuilder();

            if (tenantConfig.hasDefaultBucket())
                storageUpdate.setDefaultBucket(tenantConfig.getDefaultBucket());

            if (tenantConfig.hasDefaultFormat())
                storageUpdate.setDefaultFormat(tenantConfig.getDefaultFormat());

            storageConfig = storageUpdate.build();
        }

        jobState.sysConfig = RuntimeConfig.newBuilder()
                .setStorage(storageConfig)
                .putAllRepositories(platformConfig.getRepositoriesMap())
                .build();

        return jobState;
    }

    JobState saveInitialMetadata(JobState jobState) {

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
                        .setValue(MetadataCodec.encodeValue(jobState.tracStatus.toString()))
                        .build());

        var freeJobAttrs = jobState.jobRequest.getJobAttrsList();

        var jobWriteReq = MetadataWriteRequest.newBuilder()
                .setTenant(jobState.tenant)
                .setObjectType(ObjectType.JOB)
                .setDefinition(jobObj)
                .addAllTagUpdates(ctrlJobAttrs)
                .addAllTagUpdates(freeJobAttrs)
                .build();

        var client = metaClient.withCallCredentials(jobState.credentials);

        var jobId = client.createObject(jobWriteReq);

        jobState.jobId = jobId;

        jobState.jobConfig = jobState.jobConfig
                .toBuilder()
                .setJobId(jobId)
                .build();

        return jobState;
    }

    void processJobResult(JobState jobState) {

        log.info("Record job result [{}]: {}", jobState.jobKey, jobState.tracStatus);

        var batchUpdate = MetadataWriteBatchRequest.newBuilder();
        batchUpdate.setTenant(jobState.tenant);

        var commonUpdates = buildCommonResults(jobState);

        var jobLogic = JobLogic.forJobType(jobState.jobType);
        var jobUpdates = jobState.tracStatus == JobStatusCode.SUCCEEDED
                ? jobLogic.buildResultMetadata(jobState.tenant, jobState.jobConfig, jobState.executorResult)
                : List.<MetadataWriteRequest>of();

        for (var update : commonUpdates) {
            addUpdateToWriteBatch(batchUpdate, update);
        }

        for (var update : jobUpdates) {
            var update_ = applyJobAttrs(jobState, scrapTenant(update));
            addUpdateToWriteBatch(batchUpdate, update_);
        }

        var batch = batchUpdate.build();
        var batchNotEmpty = isAnyToSend(batch);

        if (batchNotEmpty) {
            var metadataClient = metaClient.withCallCredentials(jobState.credentials);
            metadataClient.writeBatch(batch);
        }
    }

    List<MetadataWriteRequest> buildCommonResults(JobState jobState) {

        var commonResults = new ArrayList<MetadataWriteRequest>();

        var jobUpdate = jobState.tracStatus == JobStatusCode.SUCCEEDED
                ? buildJobSucceededUpdate(jobState)
                : buildJobFailedUpdate(jobState);

        commonResults.add(scrapTenant(jobUpdate));

        if (jobState.resultMapping.containsKey(JOB_RESULT_KEY)) {

            var resultId = jobState.resultMapping.get(JOB_RESULT_KEY);

            if (resultId != null) {

                var resultObj = jobState.executorResult.getResultsOrThrow(MetadataUtil.objectKey(resultId));
                var resultUpdate = MetadataWriteRequest.newBuilder()
                        .setObjectType(ObjectType.RESULT)
                        .setPriorVersion(MetadataUtil.preallocated(resultId))
                        .setDefinition(resultObj)
                        .build();

                commonResults.add(resultUpdate);
            }
        }

        if (jobState.resultMapping.containsKey(JOB_LOG_FILE_KEY)) {

            var logFileId = jobState.resultMapping.get(JOB_LOG_FILE_KEY);
            var logStorageId = jobState.resultMapping.get(JOB_LOG_STORAGE_KEY);

            if (logFileId != null && logStorageId != null) {

                var logFileObj = jobState.executorResult.getResultsOrThrow(MetadataUtil.objectKey(logFileId));
                var logFileUpdate = MetadataWriteRequest.newBuilder()
                        .setObjectType(ObjectType.FILE)
                        .setPriorVersion(MetadataUtil.preallocated(logFileId))
                        .setDefinition(logFileObj)
                        .build();

                var logStorageObj = jobState.executorResult.getResultsOrThrow(MetadataUtil.objectKey(logStorageId));
                var logStorageUpdate = MetadataWriteRequest.newBuilder()
                        .setObjectType(ObjectType.STORAGE)
                        .setPriorVersion(MetadataUtil.preallocated(logStorageId))
                        .setDefinition(logStorageObj)
                        .build();

                commonResults.add(applyJobAttrs(jobState, logFileUpdate));
                commonResults.add(applyJobAttrs(jobState, logStorageUpdate));
            }

        }

        return commonResults;
    }

    private static boolean isAnyToSend(MetadataWriteBatchRequest request) {

        return request.getCreatePreallocatedObjectsCount() > 0 ||
                request.getCreateObjectsCount() > 0 ||
                request.getUpdateObjectsCount() > 0 ||
                request.getUpdateTagsCount() > 0;
    }

    private MetadataWriteRequest buildJobSucceededUpdate(JobState jobState) {

        var attrUpdates = List.of(
                TagUpdate.newBuilder()
                        .setAttrName(TRAC_JOB_STATUS_ATTR)
                        .setValue(encodeValue(jobState.tracStatus.toString()))
                        .build());

        return MetadataWriteRequest.newBuilder()
                .setTenant(jobState.tenant)
                .setObjectType(ObjectType.JOB)
                .setPriorVersion(selectorFor(jobState.jobId))
                .addAllTagUpdates(attrUpdates)
                .build();
    }

    private MetadataWriteRequest buildJobFailedUpdate(JobState jobState) {

        var attrUpdates = List.of(
                TagUpdate.newBuilder()
                        .setAttrName(TRAC_JOB_STATUS_ATTR)
                        .setValue(encodeValue(jobState.tracStatus.toString()))
                        .build(),
                TagUpdate.newBuilder()
                        .setAttrName(TRAC_JOB_ERROR_MESSAGE_ATTR)
                        .setValue(encodeValue(jobState.statusMessage))
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
