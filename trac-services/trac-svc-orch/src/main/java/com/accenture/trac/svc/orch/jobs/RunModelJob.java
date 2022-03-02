/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.orch.jobs;


import com.accenture.trac.api.JobRequest;
import com.accenture.trac.api.MetadataWriteRequest;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.MetadataConstants;
import com.accenture.trac.common.metadata.MetadataUtil;
import com.accenture.trac.config.JobConfig;
import com.accenture.trac.config.JobResult;
import com.accenture.trac.metadata.*;
import org.checkerframework.checker.units.qual.A;

import java.util.*;


public class RunModelJob implements IJobLogic {

    private static final String TRAC_MODEL_RESOURCE_NAME = "trac_model";

    @Override
    public List<TagSelector> requiredMetadata(JobDefinition job) {

        if (job.getJobType() != JobType.RUN_MODEL)
            throw new EUnexpected();

        var runModel = job.getRunModel();

        var resources = new ArrayList<TagSelector>(runModel.getInputsCount() + 1);
        resources.add(runModel.getModel());
        resources.addAll(runModel.getInputsMap().values());

        return resources;
    }

    @Override
    public List<TagSelector> requiredMetadata(Map<String, ObjectDefinition> newResources) {

        var resources = new ArrayList<TagSelector>();

        for (var obj : newResources.values()) {

            if (obj.getObjectType() != ObjectType.DATA)
                continue;

            var dataDef = obj.getData();
            resources.add(dataDef.getStorageId());

            if (dataDef.hasSchemaId())
                resources.add(dataDef.getSchemaId());
        }

        return resources;
    }



    public Map<String, MetadataWriteRequest> preRunMetadata(
            String tenant, JobRequest request,
            Map<String, ObjectDefinition> resources,
            Map<String, String> resourceMapping) {

        var outputMap = new HashMap<String, MetadataWriteRequest>();

        var modelId = request.getJob().getRunModel().getModel();
        var selectorKey = MetadataUtil.objectKey(modelId);
        var mappedKey = resourceMapping.get(selectorKey);
        var modelKey = mappedKey != null ? mappedKey : selectorKey;
        var modelDef = resources.get(modelKey).getModel();

        for (var output : modelDef.getOutputsMap().entrySet()) {

            var dataKey = String.format("%s:%s", output.getKey(), ObjectType.DATA);
            var dataReq = MetadataWriteRequest.newBuilder()
                    .setTenant(tenant)
                    .setObjectType(ObjectType.DATA)
                    .build();

            var storageKey = String.format("%s:%s", output.getKey(), ObjectType.STORAGE);
            var storageReq = MetadataWriteRequest.newBuilder()
                    .setTenant(tenant)
                    .setObjectType(ObjectType.STORAGE)
                    .build();

            outputMap.put(dataKey, dataReq);
            outputMap.put(storageKey, storageReq);
        }

        return outputMap;
    }

    public JobDefinition setOutputIds(JobDefinition job, Map<String, TagHeader> outputIds,
                                      Map<String, ObjectDefinition> resources,
                                      Map<String, String> resourceMapping) {

        var runModel = job.getRunModel().toBuilder();

        var modelId = job.getRunModel().getModel();
        var selectorKey = MetadataUtil.objectKey(modelId);
        var mappedKey = resourceMapping.get(selectorKey);
        var modelKey = mappedKey != null ? mappedKey : selectorKey;
        var modelDef = resources.get(modelKey).getModel();

        for (var output : modelDef.getOutputsMap().keySet()) {

            var dataKey = String.format("%s:%s", output, ObjectType.DATA);
            var dataId = outputIds.get(dataKey);
            var dataSelector = MetadataUtil.selectorFor(dataId);
            runModel.putOutputs(output, dataSelector);
        }

        return job.toBuilder()
                .setRunModel(runModel)
                .build();
    }

    @Override
    public List<MetadataWriteRequest> buildResultMetadata(String tenant, JobConfig jobConfig, JobResult jobResult) {

        var updates = new ArrayList<MetadataWriteRequest>();

        var runModel = jobConfig.getJob().getRunModel();

        for (var output: runModel.getOutputsMap().entrySet()) {

            var outputName = output.getKey();

            // TODO: String constants

            var dataIdLookup = outputName + ":DATA";
            var dataId = jobConfig.getResultMappingOrThrow(dataIdLookup);
            var dataKey = MetadataUtil.objectKey(dataId);
            var dataObj = jobResult.getResultsOrThrow(dataKey);

            var storageIdLookup = outputName + ":STORAGE";
            var storageId = jobConfig.getResultMappingOrThrow(storageIdLookup);
            var storageKey = MetadataUtil.objectKey(storageId);
            var storageObj = jobResult.getResultsOrThrow(storageKey);

            var priorDataSelector = runModel.containsPriorOutputs(outputName)
                    ? runModel.getPriorOutputsOrThrow(outputName)
                    : MetadataUtil.preallocated(runModel.getOutputsOrThrow(outputName));

            var priorStorageSelector = runModel.containsPriorOutputs(outputName)
                    ? priorStorageSelector(priorDataSelector, jobConfig)
                    : MetadataUtil.preallocated(dataObj.getData().getStorageId());

            var controlledAttrs = List.of(
                    TagUpdate.newBuilder()
                    .setAttrName("trac_job_output")
                    .setValue(MetadataCodec.encodeValue(outputName))
                    .build());

            var suppliedAttrs = jobConfig.getJob()
                    .getRunModel()
                    .getOutputAttrsList();

            var dataUpdate = MetadataWriteRequest.newBuilder()
                    .setTenant(tenant)
                    .setObjectType(ObjectType.DATA)
                    .setPriorVersion(priorDataSelector)
                    .setDefinition(dataObj)
                    .addAllTagUpdates(controlledAttrs)
                    .addAllTagUpdates(suppliedAttrs)
                    .build();

            updates.add(dataUpdate);

            var storageAttrs = List.of(
                    TagUpdate.newBuilder()
                    .setAttrName(MetadataConstants.TRAC_STORAGE_OBJECT_ATTR)
                    .setValue(MetadataCodec.encodeValue(dataKey))
                    .build());

            var storageUpdate = MetadataWriteRequest.newBuilder()
                    .setTenant(tenant)
                    .setObjectType(ObjectType.STORAGE)
                    .setPriorVersion(priorStorageSelector)
                    .setDefinition(storageObj)
                    .addAllTagUpdates(storageAttrs)
                    .build();

            updates.add(storageUpdate);
        }

        return updates;
    }

    private TagSelector priorStorageSelector(TagSelector priorDataSelector, JobConfig jobConfig) {

        var dataKey = MetadataUtil.objectKey(priorDataSelector);

        if (jobConfig.containsResourceMapping(dataKey)) {
            var dataId = jobConfig.getResourceMappingOrDefault(dataKey, null);
            var dataSelector = MetadataUtil.selectorFor(dataId);
            dataKey = MetadataUtil.objectKey(dataSelector);
        }

        var dataObj = jobConfig.getResourcesOrThrow(dataKey);

        var storageSelector = dataObj.getData().getStorageId();
        var storageKey = MetadataUtil.objectKey(storageSelector);

        if (jobConfig.containsResourceMapping(storageKey)) {

            var storageId = jobConfig.getResourceMappingOrDefault(storageKey, null);
            storageSelector = MetadataUtil.selectorFor(storageId);
        }

        return storageSelector;
    }
}
