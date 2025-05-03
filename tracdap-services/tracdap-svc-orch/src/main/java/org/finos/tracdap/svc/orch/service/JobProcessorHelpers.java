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
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.config.ConfigHelpers;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.config.IDynamicResources;
import org.finos.tracdap.common.exception.EConsistencyValidation;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.config.*;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.orch.jobs.JobLogic;

import io.grpc.stub.AbstractStub;
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
    private final IDynamicResources resources;
    private final GrpcConcern commonConcerns;

    private final InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient;
    private final ConfigManager configManager;


    public JobProcessorHelpers(
            PlatformConfig platformConfig,
            IDynamicResources resources,
            GrpcConcern commonConcerns,
            PluginRegistry registry) {

        this.platformConfig = platformConfig;
        this.resources = resources;
        this.commonConcerns = commonConcerns;

        this.metaClient = registry.getSingleton(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub.class);
        this.configManager = registry.getSingleton(ConfigManager.class);
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
        var metadata = new MetadataBundle(jobState.objectMapping, jobState.objects, jobState.tags);

        jobState.definition = logic.applyTransform(jobState.definition, metadata, resources);

        var updatedMetadata = logic.applyMetadataTransform(jobState.definition, metadata, resources);
        jobState.objects = updatedMetadata.getObjects();
        jobState.objectMapping = updatedMetadata.getObjectMapping();

        return jobState;
    }

    JobState loadMetadata(JobState jobState) {

        var jobLogic = JobLogic.forJobType(jobState.jobType);
        var selectors = jobLogic.requiredMetadata(jobState.definition);

        if (selectors.isEmpty()) {
            log.info("No additional metadata required");
            return jobState;
        }

        return loadMetadata(jobState, selectors);
    }

    JobState loadMetadata(JobState jobState, List<TagSelector> selectors) {

        log.info("Loading additional required metadata...");

        var orderedKeys = new ArrayList<String>(selectors.size());
        var orderedSelectors = new ArrayList<TagSelector>(selectors.size());

        for (var selector : selectors) {
            orderedKeys.add(MetadataUtil.objectKey(selector));
            orderedSelectors.add(selector);
        }

        var batchRequest = MetadataBatchRequest.newBuilder()
                .setTenant(jobState.tenant)
                .addAllSelector(orderedSelectors)
                .build();

        var client = configureClient(metaClient, jobState);
        var batchResponse = client.readBatch(batchRequest);

        return loadMetadataResponse(jobState, orderedKeys, batchResponse);
    }

    JobState loadMetadataResponse(
            JobState jobState, List<String> orderedKeys,
            MetadataBatchResponse batchResponse) {

        if (batchResponse.getTagCount() != orderedKeys.size())
            throw new EUnexpected();

        var jobLogic = JobLogic.forJobType(jobState.jobType);

        var objectMapping = new HashMap<String, TagHeader>(orderedKeys.size());
        var objects = new HashMap<String, ObjectDefinition>(orderedKeys.size());;
        var tags = new HashMap<String, Tag>(orderedKeys.size());

        for (var i = 0; i < orderedKeys.size(); i++) {

            var orderedKey = orderedKeys.get(i);
            var orderedTag = batchResponse.getTag(i);

            var objectKey = MetadataUtil.objectKey(orderedTag.getHeader());
            var object_ = orderedTag.getDefinition();
            var tag = orderedTag.toBuilder().clearDefinition().build();

            objectMapping.put(orderedKey, tag.getHeader());
            objects.put(objectKey, object_);
            tags.put(objectKey, tag);
        }

        jobState.objectMapping.putAll(objectMapping);
        jobState.objects.putAll(objects);
        jobState.tags.putAll(tags);

        var extraResources = jobLogic.requiredMetadata(objects).stream()
                .filter(selector -> !jobState.objects.containsKey(MetadataUtil.objectKey(selector)))
                .filter(selector -> !jobState.objectMapping.containsKey(MetadataUtil.objectKey(selector)))
                .collect(Collectors.toList());

        if (!extraResources.isEmpty())
            return loadMetadata(jobState, extraResources);

        return jobState;
    }

    JobState allocateResultIds(JobState jobState) {

        // TODO: Single job timestamp - requires changes in meta svc for this to actually be used
        // meta svc must accept object timestamps as gRPC metadata for internal API calls
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
                jobState.objects, jobState.objectMapping);

        var jobResultIds = jobLogic.newResultIds(
                jobState.tenant, jobState.definition,
                jobState.objects, jobState.objectMapping);

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

        var client = configureClient(metaClient, jobState);

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
                jobState.objects, jobState.objectMapping);

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
                .putAllObjects(jobState.objects)
                .putAllObjectMapping(jobState.objectMapping)
                .putAllResultMapping(jobState.resultMapping)
                .build();

        var sysConfig = RuntimeConfig.newBuilder();
        var storageConfig = StorageConfig.newBuilder();

        var internalStorage = resources.getMatchingEntries(
                resource -> resource.getResourceType() == ResourceType.INTERNAL_STORAGE);

        for (var storageEntry : internalStorage.entrySet()) {
            var storageKey = storageEntry.getKey();
            var storage = translateResourceConfig(storageKey, storageEntry.getValue(), jobState);
            storageConfig.putBuckets(storageKey, storage);
        }

        var externalStorage = resources.getMatchingEntries(
                resource -> resource.getResourceType() == ResourceType.EXTERNAL_STORAGE);

        for (var storageEntry : externalStorage.entrySet()) {
            var storageKey = storageEntry.getKey();
            var storage = translateResourceConfig(storageKey, storageEntry.getValue(), jobState);
            storageConfig.putExternal(storageKey, storage);
        }

        // Default storage / format still on platform config file for now
        if (platformConfig.containsTenants(jobState.tenant)) {
            var tenantConfig = platformConfig.getTenantsOrThrow(jobState.tenant);
            storageConfig.setDefaultBucket(tenantConfig.getDefaultBucket());
            storageConfig.setDefaultFormat(tenantConfig.getDefaultFormat());
        }
        else {
            storageConfig.setDefaultBucket(platformConfig.getStorage().getDefaultBucket());
            storageConfig.setDefaultFormat(platformConfig.getStorage().getDefaultFormat());
        }

        sysConfig.setStorage(storageConfig);

        var repositories = resources.getMatchingEntries(
                resource -> resource.getResourceType() == ResourceType.MODEL_REPOSITORY);

        for (var repoEntry : repositories.entrySet()) {

            var repoKey = repoEntry.getKey();

            // Only translate repositories required for the job
            if (jobUsesRepository(repoKey, jobState)) {
                var repoConfig = translateResourceConfig(repoKey, repoEntry.getValue(), jobState);
                sysConfig.putRepositories(repoKey, repoConfig);
            }
        }

        jobState.sysConfig = sysConfig.build();

        return jobState;
    }

    private boolean jobUsesRepository(String repoKey, JobState jobState) {

        // This method filters repo resources for the currently known job types
        // TODO: Generic resource filtering in the IJobLogic interface

        // Import model jobs can refer to repositories directly
        if (jobState.jobType == JobType.IMPORT_MODEL) {
            var importModelJob = jobState.definition.getImportModel();
            if (importModelJob.getRepository().equals(repoKey)) {
                return true;
            }
        }

        // Check if any resources refer to the repo key
        for (var object : jobState.objects.values()) {
            if (object.getObjectType() == ObjectType.MODEL) {
                if (object.getModel().getRepository().equals(repoKey)) {
                    return true;
                }
            }
        }

        return false;
    }

    private PluginConfig translateResourceConfig(String resourceKey, ResourceDefinition resource, JobState jobState) {

        var pluginConfig = ConfigHelpers.resourceToPluginConfig(resource).toBuilder();

        var tenantScope = String.format("/%s/%s/", ConfigKeys.TENANT_SCOPE, jobState.tenant);

        for (var secretEntry : pluginConfig.getSecretsMap().entrySet()) {

            var propertyName = secretEntry.getKey();
            var secretAlias = secretEntry.getValue();

            // Secret loader will not load the secret if the alias is not valid
            // This handling provides more meaningful errors
            if (secretAlias.isBlank() || !secretAlias.startsWith(tenantScope)) {

                var message = String.format("Resource configuration for [%s] is not valid", resourceKey);
                var detail = String.format("Inconsistent secret alias for [%s]", propertyName);

                log.error("{}: {}", message, detail);
                throw new EConsistencyValidation(message);
            }

            var secret = configManager.loadPassword(secretAlias);

            pluginConfig.putProperties(propertyName, secret);
        }

        pluginConfig.clearSecrets();

        return pluginConfig.build();
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

        var client = configureClient(metaClient, jobState);

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
            var metadataClient = configureClient(metaClient, jobState);
            metadataClient.writeBatch(batch);
        }
    }

    List<MetadataWriteRequest> buildCommonResults(JobState jobState) {

        var commonResults = new ArrayList<MetadataWriteRequest>();

        var jobUpdate = jobState.tracStatus == JobStatusCode.SUCCEEDED
                ? buildJobSucceededUpdate(jobState)
                : buildJobFailedUpdate(jobState);

        commonResults.add(scrapTenant(jobUpdate));

        // For severe error cases, the result object may not be available
        // Then no additional objects can be recorded
        if (jobState.executorResult == null)
            return commonResults;

        if (jobState.resultMapping.containsKey(JOB_RESULT_KEY)) {

            var resultId = jobState.resultMapping.get(JOB_RESULT_KEY);
            var resultKey = resultId != null ? MetadataUtil.objectKey(resultId) : null;

            if (resultKey != null && jobState.executorResult.containsResults(resultKey)) {

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
            var logFileKey = logFileId != null ? MetadataUtil.objectKey(logFileId) : null;
            var logStorageKey = logStorageId != null ? MetadataUtil.objectKey(logStorageId) : null;

            if (logFileKey != null && jobState.executorResult.containsResults(logFileKey) &&
                logStorageKey != null && jobState.executorResult.containsResults(logStorageKey)) {

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

    <TStub extends AbstractStub<TStub>>
    TStub configureClient(TStub clientStub, JobState jobState) {

        if (jobState.clientConfig == null)
            jobState.clientConfig = jobState.clientState.restore(commonConcerns);

        return jobState.clientConfig.configureClient(clientStub);
    }
}
