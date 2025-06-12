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
import org.finos.tracdap.api.internal.RuntimeJobResult;
import org.finos.tracdap.api.internal.RuntimeJobResultAttrs;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EConsistencyValidation;
import org.finos.tracdap.common.exception.EJobResult;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.common.service.TenantConfigManager;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.config.*;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.orch.jobs.JobLogic;

import io.grpc.stub.AbstractStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.finos.tracdap.common.metadata.MetadataCodec.encodeValue;
import static org.finos.tracdap.common.metadata.MetadataConstants.*;


public class JobProcessorHelpers {

    private final Logger log = LoggerFactory.getLogger(JobProcessorHelpers.class);

    private final InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient;
    private final GrpcConcern commonConcerns;

    private final TenantConfigManager tenantState;

    private final ConfigManager configManager;
    private final Validator validator = new Validator();


    public JobProcessorHelpers(
            TenantConfigManager tenantState,
            GrpcConcern commonConcerns,
            PluginRegistry registry) {

        this.tenantState = tenantState;
        this.commonConcerns = commonConcerns;

        this.metaClient = registry.getSingleton(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub.class);
        this.configManager = registry.getSingleton(ConfigManager.class);
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

    private JobState loadMetadata(JobState jobState, List<TagSelector> selectors) {

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

    private JobState loadMetadataResponse(
            JobState jobState, List<String> orderedKeys,
            MetadataBatchResponse batchResponse) {

        if (batchResponse.getTagCount() != orderedKeys.size())
            throw new EUnexpected();

        var objectMapping = new HashMap<String, TagHeader>(orderedKeys.size());
        var objects = new HashMap<String, ObjectDefinition>(orderedKeys.size());
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

        var dependencies = loadMetadataDependencies(batchResponse.getTagList());

        var missingDependencies = dependencies.stream()
                .filter(selector -> !jobState.objects.containsKey(MetadataUtil.objectKey(selector)))
                .filter(selector -> !jobState.objectMapping.containsKey(MetadataUtil.objectKey(selector)))
                .collect(Collectors.toList());

        if (!missingDependencies.isEmpty())
            return loadMetadata(jobState, missingDependencies);

        return jobState;
    }

    private List<TagSelector> loadMetadataDependencies(List<Tag> tags) {

        var dependencies = new ArrayList<TagSelector>();

        for (var tag : tags) {

            var obj = tag.getDefinition();

            if (obj.getObjectType() == ObjectType.DATA) {

                var dataDef = obj.getData();
                dependencies.add(dataDef.getStorageId());

                if (dataDef.hasSchemaId())
                    dependencies.add(dataDef.getSchemaId());
            }

            else if (obj.getObjectType() == ObjectType.FILE) {

                var fileDef = obj.getFile();
                dependencies.add(fileDef.getStorageId());
            }
        }

        return dependencies;
    }

    JobState applyTransform(JobState jobState) {

        var jobLogic = JobLogic.forJobType(jobState.jobType);
        var metadata = new MetadataBundle(jobState.objectMapping, jobState.objects, jobState.tags);
        var tenantConfig = tenantState.getTenantConfig(jobState.tenant);

        var updatedDefinition = jobLogic.applyJobTransform(jobState.definition, metadata, tenantConfig);
        var updatedMetadata = jobLogic.applyMetadataTransform(updatedDefinition, metadata, tenantConfig);

        jobState.definition = updatedDefinition;
        jobState.objects = updatedMetadata.getObjects();
        jobState.objectMapping = updatedMetadata.getObjectMapping();

        return jobState;
    }

    JobState saveJobDefinition(JobState jobState) {

        var client = configureClient(metaClient, jobState);

        // Preallocate result ID - needed as part of job definition

        var resultIdRequest = MetadataWriteRequest.newBuilder()
                .setTenant(jobState.tenant)
                .setObjectType(ObjectType.RESULT)
                .build();

        // Use version 1 of the RESULT ID (preallocated version is 0)
        var resultPreallocated = client.preallocateId(resultIdRequest);
        var resultId = MetadataUtil.nextObjectVersion(resultPreallocated, Instant.now());

        jobState.resultId = resultId;
        jobState.definition = jobState.definition.toBuilder()
                .setResultId(MetadataUtil.selectorFor(resultId))
                .build();

        // Save job definition

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

        jobState.jobId = client.createObject(jobWriteReq);

        return jobState;
    }

    JobState preallocateObjectIds(JobState jobState) {

        var batchRequest = MetadataWriteBatchRequest.newBuilder().setTenant(jobState.tenant);

        // IDs required for the job log file
        batchRequest.addPreallocateIds(preallocate(ObjectType.FILE));
        batchRequest.addPreallocateIds(preallocate(ObjectType.STORAGE));

        // Add required IDs based on the job type
        var jobLogic = JobLogic.forJobType(jobState.jobType);
        var metadata = new MetadataBundle(jobState.objectMapping, jobState.objects, jobState.tags);
        var requiredIds = jobLogic.expectedOutputs(jobState.definition, metadata);

        for (var requirement : requiredIds.entrySet()) {

            var objectType = requirement.getKey();
            var count = requirement.getValue();

            for (var i = 0; i < count; i++) {
                var request = MetadataWriteRequest.newBuilder().setObjectType(objectType);
                batchRequest.addPreallocateIds(request);
            }
        }

        // Allocate all the IDs in a single batch
        var metadataClient = configureClient(metaClient, jobState);
        var batchResponse = metadataClient.writeBatch(batchRequest.build());

        jobState.preallocatedIds = batchResponse.getPreallocateIdsList();

        return jobState;
    }

    private MetadataWriteRequest.Builder preallocate(ObjectType objectType) {
        return MetadataWriteRequest.newBuilder().setObjectType(objectType);
    }

    JobState buildJobConfig(JobState jobState) {

        var jobLogic  = JobLogic.forJobType(jobState.jobType);
        var metadata = new MetadataBundle(jobState.objectMapping, jobState.objects, jobState.tags);
        var tenantConfig = tenantState.getTenantConfig(jobState.tenant);

        jobState.jobConfig = JobConfig.newBuilder()
                .setJobId(jobState.jobId)
                .setJob(jobState.definition)
                .putAllObjectMapping(jobState.objectMapping)
                .putAllObjects(jobState.objects)
                .putAllTags(jobState.tags)
                .setResultId(jobState.resultId)
                .addAllPreallocatedIds(jobState.preallocatedIds)
                .build();

        var requiredResources = jobLogic.requiredResources(jobState.definition, metadata, tenantConfig);

        var sysConfig = RuntimeConfig.newBuilder();
        sysConfig.putAllProperties(tenantConfig.getPropertiesMap());

        for (var resourceKey : requiredResources) {

            if (tenantConfig.containsResources(resourceKey)) {
                var resourceConfig = tenantConfig.getResourcesOrThrow(resourceKey);
                var resource = translateResourceConfig(resourceKey, resourceConfig, jobState);
                sysConfig.putResources(resourceKey, resource);
            }
            else {
                // This condition should already be picked up during job consistency validation
                var message = String.format("Required resource [%s] not found", resourceKey);
                log.error(message);
                throw new EConsistencyValidation(message);
            }
        }

        // Always send system resources to the runtime
        for (var resourceKey : tenantConfig.getResourcesMap().keySet()) {
            if (resourceKey.startsWith(ConfigKeys.TRAC_PREFIX)) {
                var resourceConfig = tenantConfig.getResourcesOrThrow(resourceKey);
                var resource = translateResourceConfig(resourceKey, resourceConfig, jobState);
                sysConfig.putResources(resourceKey, resource);
            }
        }

        jobState.sysConfig = sysConfig.build();

        return jobState;
    }

    private ResourceDefinition translateResourceConfig(String resourceKey, ResourceDefinition resource, JobState jobState) {

        var translated = resource.toBuilder();

        var tenantScope = String.format("/%s/%s/", ConfigKeys.TENANT_SCOPE, jobState.tenant);

        for (var secretEntry : translated.getSecretsMap().entrySet()) {

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

            translated.putProperties(propertyName, secret);
        }

        translated.clearSecrets();

        return translated.build();
    }


    void processJobResult(JobState jobState) {

        if (jobState.tracStatus != JobStatusCode.FINISHING || jobState.runtimeResult == null) {

            var result = ResultDefinition.newBuilder().setJobId(MetadataUtil.selectorFor(jobState.jobId));

            if (jobState.tracStatus == JobStatusCode.FAILED || jobState.tracStatus == JobStatusCode.CANCELLED)
                result.setStatusCode(jobState.tracStatus);
            else
                result.setStatusCode(JobStatusCode.FAILED);

            if (jobState.statusMessage != null && !jobState.statusMessage.isEmpty())
                result.setStatusMessage(jobState.statusMessage);
            else
                result.setStatusMessage("No details available");

            var jobResult = RuntimeJobResult.newBuilder()
                    .setJobId(jobState.jobId)
                    .setResultId(jobState.resultId)
                    .setResult(result);

            jobState.jobResult = jobResult.build();
            jobState.tracStatus = jobResult.getResult().getStatusCode();

            return;
        }

        // A result is available and ready to be processed
        var runtimeResult = jobState.runtimeResult;

        // Apply validation to the runtime result - partially consistent results will be rejected
        // This is safest, but has the potential to lose useful error info in some cases
        validator.validateFixedObject(runtimeResult);

        var resultIds = buildResultLookup(runtimeResult);

        var jobLogic = JobLogic.forJobType(jobState.jobType);
        var jobResult = jobLogic.processResult(jobState.jobConfig, runtimeResult, resultIds);

        var finalResult = addCommonOutputs(jobResult, runtimeResult, resultIds, jobState);

        // Unexpected outputs from the runtime will not be consumed by the results processing logic
        // So long as all the expected outputs are available, treat this as a warning
        if (finalResult.getObjectIdsCount() < runtimeResult.getObjectIdsCount() ||
            finalResult.getObjectsCount() < runtimeResult.getObjectsCount() ||
            finalResult.getAttrsCount() < runtimeResult.getAttrsCount()) {

            log.warn("Job result included unexpected outputs, which have been ignored [{}]", jobState.jobKey);
        }

        jobState.jobResult = finalResult;

        // Copy final result status to the job state (used for status polling in the orchestrator API)
        jobState.tracStatus = finalResult.getResult().getStatusCode();
        jobState.statusMessage = finalResult.getResult().getStatusMessage();
    }

    private RuntimeJobResult addCommonOutputs(
            RuntimeJobResult jobResult, RuntimeJobResult runtimeResult,
            Map<String, TagHeader> resultIds, JobState jobState) {

        var resultDef = runtimeResult.getResult().toBuilder();

        // If status is not a completed status, set status to failed

        if (resultDef.getStatusCode() != JobStatusCode.SUCCEEDED &&
            resultDef.getStatusCode() != JobStatusCode.FAILED &&
            resultDef.getStatusCode() != JobStatusCode.CANCELLED) {

            resultDef.setStatusCode(JobStatusCode.FAILED);

            if (jobState.statusMessage != null && !jobState.statusMessage.isEmpty())
                resultDef.setStatusMessage(jobState.statusMessage);
            else
                resultDef.setStatusMessage("No details available");
        }

        // Include the main RESULT object

        var finalResult = jobResult.toBuilder()
                .setResultId(runtimeResult.getResultId())
                .setResult(resultDef);

        // Add FILE and STORAGE objects for the job log

        var logFileSelector = runtimeResult.getResult().getLogFileId();
        var logFileId = resultIds.get(logFileSelector.getObjectId());
        var logFileKey = logFileId != null ? MetadataUtil.objectKey(logFileId) : null;

        checkResultAvailable(logFileSelector, logFileKey, runtimeResult);

        var logFileDef = runtimeResult.getObjectsOrThrow(logFileKey);

        var logStorageSelector = logFileDef.getFile().getStorageId();
        var logStorageId = resultIds.get(logStorageSelector.getObjectId());
        var logStorageKey = logStorageId != null ? MetadataUtil.objectKey(logStorageId) : null;

        checkResultAvailable(logStorageSelector, logStorageKey, runtimeResult);

        var logStorageDef = runtimeResult.getObjectsOrThrow(logStorageKey);

        finalResult.addObjectIds(logFileId);
        finalResult.addObjectIds(logStorageId);
        finalResult.putObjects(logFileKey, logFileDef);
        finalResult.putObjects(logStorageKey, logStorageDef);


        // Include controlled job attrs on all outputs

        for (var objectId : finalResult.getObjectIdsList()) {

            var objectKey = MetadataUtil.objectKey(objectId);

            var attrs = finalResult
                    .getAttrsOrDefault(objectKey, RuntimeJobResultAttrs.getDefaultInstance())
                    .toBuilder();

            attrs.addAttrs(TagUpdate.newBuilder()
                    .setAttrName(TRAC_UPDATE_JOB)
                    .setValue(MetadataCodec.encodeValue(jobState.jobKey)));


            if (objectId.getObjectVersion() == MetadataConstants.OBJECT_FIRST_VERSION) {

                attrs.addAttrs(TagUpdate.newBuilder()
                        .setAttrName(TRAC_CREATE_JOB)
                        .setValue(MetadataCodec.encodeValue(jobState.jobKey)));
            }

            finalResult.putAttrs(objectKey, attrs.build());
        }

        return finalResult.build();
    }

    private void checkResultAvailable(TagSelector selector, String outputKey, RuntimeJobResult jobResult) {

        var displayKey = MetadataUtil.objectKey(selector);

        if (outputKey == null)
            throw new EJobResult(String.format("Missing object ID in job result: [%s]", displayKey));

        if (!jobResult.containsObjects(outputKey))
            throw new EJobResult(String.format("Missing definition in job result: [%s]", displayKey));
    }

    private Map<String, TagHeader> buildResultLookup(RuntimeJobResult runtimeResult) {

        var duplicates = new HashSet<String>();

        var resultLookup = runtimeResult
                .getObjectIdsList().stream()
                .collect(Collectors.toMap(TagHeader::getObjectId, Function.identity(),
                (id1, id2) -> { duplicates.add(MetadataUtil.objectKey(id2)); return id1; }));

        if (!duplicates.isEmpty()) {

            if (duplicates.size() == 1) {
                var duplicateKey = duplicates.stream().findAny().get();
                throw new EJobResult(String.format("Duplicate object ID in job result: [%s]", duplicateKey));
            }
            else {
                throw new EJobResult(String.format("Job result contains %d duplicate object IDs", duplicates.size()));
            }
        }

        return resultLookup;
    }

    void saveJobResult(JobState jobState) {

        var jobResult = jobState.jobResult;

        // Lookup for preallocated IDs, used to decide which objects are new and which are pre-alloc
        var preallocatedIds = jobState.jobConfig.getPreallocatedIdsList().stream()
                .map(TagHeader::getObjectId)
                .collect(Collectors.toSet());

        // Collect metadata updates to send in a single batch
        var preallocated = new ArrayList<MetadataWriteRequest>();
        var newObjects = new ArrayList<MetadataWriteRequest>();
        var newVersions = new ArrayList<MetadataWriteRequest>();
        var newTags = new ArrayList<MetadataWriteRequest>();

        // Update tags on the original job object
        var jobAttrs = jobAttrs(jobResult.getResult());

        var jobWriteReq = MetadataWriteRequest.newBuilder()
                .setObjectType(ObjectType.JOB)
                .setPriorVersion(MetadataUtil.selectorFor(jobState.jobId))
                .addAllTagUpdates(jobAttrs)
                .build();

        newTags.add(jobWriteReq);

        // Create the result object
        var resultObj = ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.RESULT)
                .setResult(jobResult.getResult());

        var resultWriteReq = MetadataWriteRequest.newBuilder()
                .setObjectType(ObjectType.RESULT)
                .setPriorVersion(MetadataUtil.preallocated(jobResult.getResultId()))
                .setDefinition(resultObj)
                .build();

        // RESULT is always preallocated
        preallocated.add(resultWriteReq);

        // Add the individual objects created by the job
        for (var objectId : jobResult.getObjectIdsList()) {

            var objectKey = MetadataUtil.objectKey(objectId);

            if (!jobResult.containsObjects(objectKey))
                throw new EJobResult(String.format("Missing definition in job result: [%s]", objectKey));

            var definition = jobResult.getObjectsOrThrow(objectKey);

            var writeRequest = MetadataWriteRequest.newBuilder()
                    .setObjectType(objectId.getObjectType())
                    .setDefinition(definition);

            if (jobResult.containsAttrs(objectKey)) {
                var attrs = jobResult.getAttrsOrThrow(objectKey);
                writeRequest.addAllTagUpdates(attrs.getAttrsList());
            }

            if (preallocatedIds.contains(objectId.getObjectId())) {
                writeRequest.setPriorVersion(MetadataUtil.preallocated(objectId));
                preallocated.add(writeRequest.build());
            }
            else if (objectId.getObjectVersion() > MetadataConstants.OBJECT_FIRST_VERSION) {
                writeRequest.setPriorVersion(MetadataUtil.priorVersion(objectId));
                newVersions.add(writeRequest.build());
            }
            else {
                newObjects.add(writeRequest.build());
            }
        }

        // Send metadata updates as a single batch
        var batchRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(jobState.tenant)
                .addAllCreatePreallocatedObjects(preallocated)
                .addAllCreateObjects(newObjects)
                .addAllUpdateObjects(newVersions)
                .addAllUpdateTags(newTags)
                .build();

        var metadataClient = configureClient(metaClient, jobState);
        var batchResponse = metadataClient.writeBatch(batchRequest);

        log.info("RESULT SAVED: {} object(s) created, {} object(s) updated, {} tag(s) updated",
                batchResponse.getCreateObjectsCount() + batchResponse.getCreatePreallocatedObjectsCount(),
                batchResponse.getUpdateObjectsCount(),
                batchResponse.getUpdateTagsCount());
    }

    private List<TagUpdate> jobAttrs(ResultDefinition result) {

        if (result.getStatusCode() == JobStatusCode.SUCCEEDED) {

            return List.of(
                    TagUpdate.newBuilder()
                            .setAttrName(TRAC_JOB_STATUS_ATTR)
                            .setValue(encodeValue(result.getStatusCode().toString()))
                            .build());
        }
        else {

            return List.of(
                    TagUpdate.newBuilder()
                            .setAttrName(TRAC_JOB_STATUS_ATTR)
                            .setValue(encodeValue(result.getStatusCode().toString()))
                            .build(),
                    TagUpdate.newBuilder()
                            .setAttrName(TRAC_JOB_ERROR_MESSAGE_ATTR)
                            .setValue(encodeValue(result.getStatusMessage()))
                            .build());
        }
    }

    <TStub extends AbstractStub<TStub>>
    TStub configureClient(TStub clientStub, JobState jobState) {

        if (jobState.clientConfig == null)
            jobState.clientConfig = jobState.clientState.restore(commonConcerns);

        return jobState.clientConfig.configureClient(clientStub);
    }
}
