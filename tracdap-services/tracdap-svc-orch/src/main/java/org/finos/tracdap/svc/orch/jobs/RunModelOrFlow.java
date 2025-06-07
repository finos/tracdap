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

package org.finos.tracdap.svc.orch.jobs;

import org.finos.tracdap.api.internal.RuntimeJobResult;
import org.finos.tracdap.api.internal.RuntimeJobResultAttrs;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.exception.EJobResult;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.MetadataUtil;

import java.util.*;


public abstract class RunModelOrFlow {

    protected void addRequiredStorage(MetadataBundle metadata, TenantConfig tenantConfig, Set<String> resources) {

        // Default storage is always required for outputs
        if (tenantConfig.containsProperties(ConfigKeys.STORAGE_DEFAULT_LOCATION)) {
            var defaultStorage = tenantConfig.getPropertiesOrThrow(ConfigKeys.STORAGE_DEFAULT_LOCATION);
            resources.add(defaultStorage);
        }

        // Add explicitly referenced storage locations
        metadata.getObjects().values().stream()
                .filter(obj -> obj.getObjectType() == ObjectType.STORAGE)
                .forEach(obj -> obj.getStorage().getDataItemsMap().values()
                .forEach(item -> item.getIncarnationsList().stream()
                .filter(incarnation -> incarnation.getIncarnationStatus() == IncarnationStatus.INCARNATION_AVAILABLE)
                .forEach(incarnation -> incarnation.getCopiesList().stream()
                .filter(copy -> copy.getCopyStatus() == CopyStatus.COPY_AVAILABLE)
                .map(StorageCopy::getStorageKey)
                .forEach(resources::add))));

    }

    protected Map<ObjectType, Integer> expectedOutputs(
            Map<String, ModelOutputSchema> outputs,
            Map<String, TagSelector> priorOutputs) {

        var requiredIds = new HashMap<ObjectType, Integer>();

        for (var output : outputs.entrySet()) {

            if (priorOutputs.containsKey(output.getKey()))
                continue;

            var outputType = output.getValue().getObjectType();

            if (outputType == ObjectType.DATA) {
                requiredIds.compute(ObjectType.DATA, (key, value) -> value == null ? 1 : value + 1);
                requiredIds.compute(ObjectType.STORAGE, (key, value) -> value == null ? 1 : value + 1);
            }
            else if (outputType == ObjectType.FILE) {
                requiredIds.compute(ObjectType.FILE, (key, value) -> value == null ? 1 : value + 1);
                requiredIds.compute(ObjectType.STORAGE, (key, value) -> value == null ? 1 : value + 1);
            }
            else {
                throw new EUnexpected();  // TODO
            }
        }

        return requiredIds;
    }

    protected RuntimeJobResult processResult(
            RuntimeJobResult runtimeResult, Map<String, ModelOutputSchema> expectedOutputs,
            List<TagUpdate> jobAttrs, Map<String, List<TagUpdate>> perNodeAttrs,
            Map<String, TagHeader> resultIds) {

        var jobResult = RuntimeJobResult.newBuilder();

        for (var output: expectedOutputs.entrySet()) {

            var outputName = output.getKey();
            var modelOutput = output.getValue();

            // Ignore optional outputs that were not produced
            if (modelOutput.getOptional() & !runtimeResult.getResult().containsOutputs(outputName))
                continue;

            processOutput(
                    outputName, runtimeResult, resultIds,
                    jobAttrs, perNodeAttrs,
                    jobResult);
        }

        return jobResult.build();
    }

    private void processOutput(
            String outputName, RuntimeJobResult runtimeResult, Map<String, TagHeader> resultIds,
            List<TagUpdate> jobAttrs, Map<String, List<TagUpdate>> perNodeAttrs,
            RuntimeJobResult.Builder jobResult) {

        // Look up the result objects

        var result = runtimeResult.getResult();

        if (!result.containsOutputs(outputName))
            throw new EJobResult(String.format("Missing required output [%s]", outputName));

        var outputSelector = result.getOutputsOrThrow(outputName);
        var outputId = resultIds.get(outputSelector.getObjectId());
        var outputKey = outputId != null ? MetadataUtil.objectKey(outputId) : null;

        checkResultAvailable(outputSelector, outputKey, runtimeResult);

        var outputDef = runtimeResult.getObjectsOrThrow(outputKey);
        var outputAttrs = runtimeResult.getAttrsOrDefault(outputKey, RuntimeJobResultAttrs.getDefaultInstance()).toBuilder();

        var storageSelector = getStorageKey(outputDef);
        var storageId = resultIds.get(storageSelector.getObjectId());
        var storageKey = storageId != null ? MetadataUtil.objectKey(storageId) : null;

        checkResultAvailable(storageSelector, storageKey, runtimeResult);

        var storageDef = runtimeResult.getObjectsOrThrow(storageKey);
        var storageAttrs = runtimeResult.getAttrsOrDefault(outputKey, RuntimeJobResultAttrs.getDefaultInstance()).toBuilder();

        // Add user attrs (job level and flow node attrs)

        outputAttrs.addAllAttrs(jobAttrs);

        if (perNodeAttrs.containsKey(outputName))
            outputAttrs.addAllAttrs(perNodeAttrs.get(outputName));

        // Add controlled attrs

        outputAttrs.addAttrs(TagUpdate.newBuilder()
                .setAttrName("trac_job_output")
                .setValue(MetadataCodec.encodeValue(outputName))
                .build());

        storageAttrs.addAttrs(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_STORAGE_OBJECT_ATTR)
                .setValue(MetadataCodec.encodeValue(outputKey))
                .build());

        // Add objects to the job result

        jobResult.addObjectIds(outputId);
        jobResult.putObjects(outputKey, outputDef);
        jobResult.putAttrs(outputKey, outputAttrs.build());

        jobResult.addObjectIds(storageId);
        jobResult.putObjects(storageKey, storageDef);
        jobResult.putAttrs(storageKey, storageAttrs.build());
    }

    private void checkResultAvailable(TagSelector selector, String outputKey, RuntimeJobResult jobResult) {

        var displayKey = MetadataUtil.objectKey(selector);

        if (outputKey == null)
            throw new EJobResult(String.format("Missing object ID in job result: [%s]", displayKey));

        if (!jobResult.containsObjects(outputKey))
            throw new EJobResult(String.format("Missing definition in job result: [%s]", displayKey));
    }

    private TagSelector getStorageKey(ObjectDefinition outputDef) {

        if (outputDef.getObjectType() == ObjectType.DATA)
            return outputDef.getData().getStorageId();

        if (outputDef.getObjectType() == ObjectType.FILE)
            return outputDef.getFile().getStorageId();

        throw new ETracInternal(String.format("Unsupported job output type [%s]", outputDef.getObjectType().name()));
    }
}
