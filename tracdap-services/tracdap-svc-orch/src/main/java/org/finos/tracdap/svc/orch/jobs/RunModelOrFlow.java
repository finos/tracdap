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

package org.finos.tracdap.svc.orch.jobs;


import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.api.internal.RuntimeJobResult;
import org.finos.tracdap.common.exception.EConsistencyValidation;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.MetadataUtil;

import java.util.*;


public abstract class RunModelOrFlow {

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

    public Map<String, MetadataWriteRequest> newResultIds(
            String tenant, Set<String> outputKeys,
            Map<String, TagSelector> priorOutputsMap) {

        var resultMapping = new HashMap<String, MetadataWriteRequest>();

        for (var outputKey : outputKeys) {

            if (priorOutputsMap.containsKey(outputKey))
                continue;

            var dataKey = String.format("%s:%s", outputKey, ObjectType.DATA);
            var storageKey = String.format("%s:%s", outputKey, ObjectType.STORAGE);

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

    public Map<String, TagHeader> priorResultIds(
            Set<String> outputKeys, Map<String, TagSelector> priorOutputsMap,
            Map<String, ObjectDefinition> resources, Map<String, TagHeader> resourceMapping) {

        var resultMapping = new HashMap<String, TagHeader>();

        for (var outputKey : outputKeys) {

            var priorOutput = priorOutputsMap.getOrDefault(outputKey, null);

            if (priorOutput == null)
                continue;

            var priorDataKey = MetadataUtil.objectKey(priorOutput);
            var priorDataId = resourceMapping.get(priorDataKey);
            var priorDataDef = resources.get(MetadataUtil.objectKey(priorDataId)).getData();

            var priorStorageKey = MetadataUtil.objectKey(priorDataDef.getStorageId());
            var priorStorageId = resourceMapping.get(priorStorageKey);

            var dataKey = String.format("%s:%s", outputKey, ObjectType.DATA);
            var storageKey = String.format("%s:%s", outputKey, ObjectType.STORAGE);

            resultMapping.put(dataKey, priorDataId);
            resultMapping.put(storageKey, priorStorageId);
        }

        return resultMapping;
    }

    public Map<String, TagSelector> setResultIds(
            Set<String> outputKeys,
            Map<String, TagHeader> resultMapping) {

        var outputSelectors = new HashMap<String, TagSelector>();

        for (var outputKey : outputKeys) {

            var dataKey = String.format("%s:%s", outputKey, ObjectType.DATA);
            var dataId = resultMapping.get(dataKey);
            var dataSelector = MetadataUtil.selectorFor(dataId);
            outputSelectors.put(outputKey, dataSelector);
        }

        return outputSelectors;
    }

    public List<MetadataWriteRequest> buildResultMetadata(
            String tenant, JobConfig jobConfig, RuntimeJobResult jobResult,
            Map<String, ModelOutputSchema> expectedOutputs,
            Map<String, TagSelector> outputs, Map<String, TagSelector> priorOutputs,
            List<TagUpdate> outputAttrs, Map<String, List<TagUpdate>> perNodeOutputAttrs) {

        var updates = new ArrayList<MetadataWriteRequest>();

        for (var output: expectedOutputs.entrySet()) {

            var outputName = output.getKey();
            var outputSchema = output.getValue();

            // TODO: String constants

            var dataIdLookup = outputName + ":DATA";
            var dataId = jobConfig.getResultMappingOrThrow(dataIdLookup);
            var dataKey = MetadataUtil.objectKey(dataId);

            if (!jobResult.containsResults(dataKey)) {
                if (outputSchema.getOptional())
                    continue;
                else
                    throw new EConsistencyValidation(String.format("Missing required output [%s]", outputName));
            }

            var storageIdLookup = outputName + ":STORAGE";
            var storageId = jobConfig.getResultMappingOrThrow(storageIdLookup);
            var storageKey = MetadataUtil.objectKey(storageId);

            var dataObj = jobResult.getResultsOrThrow(dataKey);
            var storageObj = jobResult.getResultsOrThrow(storageKey);

            var priorDataSelector = priorOutputs.containsKey(outputName)
                    ? priorOutputs.get(outputName)
                    : MetadataUtil.preallocated(outputs.get(outputName));

            var priorStorageSelector = priorOutputs.containsKey(outputName)
                    ? priorStorageSelector(priorDataSelector, jobConfig)
                    : MetadataUtil.preallocated(dataObj.getData().getStorageId());

            var controlledAttrs = List.of(
                    TagUpdate.newBuilder()
                    .setAttrName("trac_job_output")
                    .setValue(MetadataCodec.encodeValue(outputName))
                    .build());

            var nodeOutputAttrs = perNodeOutputAttrs.get(outputName);
            if (nodeOutputAttrs == null) {
                nodeOutputAttrs = List.of();
            }

            var dataUpdate = MetadataWriteRequest.newBuilder()
                    .setTenant(tenant)
                    .setObjectType(ObjectType.DATA)
                    .setPriorVersion(priorDataSelector)
                    .setDefinition(dataObj)
                    .addAllTagUpdates(controlledAttrs)
                    .addAllTagUpdates(outputAttrs)
                    .addAllTagUpdates(nodeOutputAttrs)
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
