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


import com.accenture.trac.api.MetadataWriteRequest;
import com.accenture.trac.config.JobConfig;
import com.accenture.trac.config.JobResult;
import com.accenture.trac.metadata.*;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.MetadataConstants;
import com.accenture.trac.common.metadata.MetadataUtil;

import java.util.*;


public class RunModelJob implements IJobLogic {

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

    @Override
    public Map<String, MetadataWriteRequest> newResultIds(
            String tenant, JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var runModel = job.getRunModel();
        var resultMapping = new HashMap<String, MetadataWriteRequest>();

        var modelKey = MetadataUtil.objectKey(runModel.getModel());
        var modelId = resourceMapping.get(modelKey);
        var modelDef = resources.get(MetadataUtil.objectKey(modelId)).getModel();

        for (var output : modelDef.getOutputsMap().entrySet()) {

            if (runModel.containsPriorOutputs(output.getKey()))
                continue;

            var dataKey = String.format("%s:%s", output.getKey(), ObjectType.DATA);
            var storageKey = String.format("%s:%s", output.getKey(), ObjectType.STORAGE);

            var dataReq = MetadataWriteRequest.newBuilder()
                    .setTenant(tenant)
                    .setObjectType(ObjectType.DATA)
                    .build();

            var storageReq = MetadataWriteRequest.newBuilder()
                    .setTenant(tenant)
                    .setObjectType(ObjectType.STORAGE)
                    .build();

            resultMapping.put(dataKey, dataReq);
            resultMapping.put(storageKey, storageReq);
        }

        return resultMapping;
    }

    @Override
    public Map<String, TagHeader> priorResultIds(
            JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var runModel = job.getRunModel();
        var resultMapping = new HashMap<String, TagHeader>();

        var modelKey = MetadataUtil.objectKey(runModel.getModel());
        var modelId = resourceMapping.get(modelKey);
        var modelDef = resources.get(MetadataUtil.objectKey(modelId)).getModel();

        for (var output : modelDef.getOutputsMap().entrySet()) {

            var priorOutput = runModel.getPriorOutputsOrDefault(output.getKey(), null);

            if (priorOutput == null)
                continue;

            var priorDataKey = MetadataUtil.objectKey(priorOutput);
            var priorDataId = resourceMapping.get(priorDataKey);
            var priorDataDef = resources.get(MetadataUtil.objectKey(priorDataId)).getData();

            var priorStorageKey = MetadataUtil.objectKey(priorDataDef.getStorageId());
            var priorStorageId = resourceMapping.get(priorStorageKey);

            var dataKey = String.format("%s:%s", output.getKey(), ObjectType.DATA);
            var storageKey = String.format("%s:%s", output.getKey(), ObjectType.STORAGE);

            resultMapping.put(dataKey, priorDataId);
            resultMapping.put(storageKey, priorStorageId);
        }

        return resultMapping;
    }

    @Override
    public JobDefinition setResultIds(
            JobDefinition job, Map<String, TagHeader> resultMapping,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var runModel = job.getRunModel().toBuilder();
        runModel.clearOutputs();

        var modelKey = MetadataUtil.objectKey(runModel.getModel());
        var modelId = resourceMapping.get(modelKey);
        var modelDef = resources.get(MetadataUtil.objectKey(modelId)).getModel();

        for (var output : modelDef.getOutputsMap().keySet()) {

            var dataKey = String.format("%s:%s", output, ObjectType.DATA);
            var dataId = resultMapping.get(dataKey);
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
