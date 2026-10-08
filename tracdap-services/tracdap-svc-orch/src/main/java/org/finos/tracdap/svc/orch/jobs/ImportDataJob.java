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

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.ResourceBundle;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.metadata.*;

import java.util.*;


public class ImportDataJob extends RunModelOrFlow implements IJobLogic {

    @Override
    public List<TagSelector> requiredMetadata(JobDefinition job) {

        if (job.getJobType() != JobType.IMPORT_DATA)
            throw new EUnexpected();

        var schemas = new ArrayList<TagSelector>();

        for (var capture : job.getImportData().getCapturesMap().values()) {
            if (capture.hasSchemaId())
                schemas.add(capture.getSchemaId());
        }

        return schemas;
    }

    @Override
    public List<String> requiredResources(JobDefinition job, MetadataBundle metadata) {

        var storageKeys = new HashSet<String>();

        for (var capture : job.getImportData().getCapturesMap().values())
            storageKeys.add(capture.getLocation().getStorageKey());

        return new ArrayList<>(storageKeys);
    }

    @Override
    public JobDefinition applyJobTransform(JobDefinition job, MetadataBundle metadata, ResourceBundle resources) {

        var importData = job.getImportData().toBuilder();

        for (var capture : job.getImportData().getCapturesMap().entrySet()) {

            var location = ExternalLocationDetails.recordLocationDetails(capture.getValue().getLocation(), resources);
            var recorded = capture.getValue().toBuilder().setLocation(location).build();

            importData.putCaptures(capture.getKey(), recorded);
        }

        return job.toBuilder()
                .setImportData(importData)
                .build();
    }

    @Override
    public MetadataBundle applyMetadataTransform(JobDefinition job, MetadataBundle metadata, ResourceBundle resources) {

        return metadata;
    }

    @Override
    public Map<ObjectType, Integer> expectedOutputs(JobDefinition job, MetadataBundle metadata) {

        return expectedOutputs(captureOutputs(job.getImportData()), Map.of());
    }

    @Override
    public JobResult processResult(JobConfig jobConfig, JobResult jobResult, Map<String, TagHeader> resultIds) {

        var importData = jobConfig.getJob().getImportData();
        var perCaptureAttrs = new HashMap<String, List<TagUpdate>>();

        for (var capture : importData.getCapturesMap().entrySet()) {
            perCaptureAttrs.put(capture.getKey(), capture.getValue().getDataAttrsList());
            perCaptureAttrs.put(fileOutputName(capture.getKey()), capture.getValue().getFileAttrsList());
        }

        return processResult(
                jobResult, captureOutputs(importData),
                importData.getOutputAttrsList(),
                perCaptureAttrs, resultIds);
    }

    private static Map<String, ModelOutputSchema> captureOutputs(org.finos.tracdap.metadata.ImportDataJob importData) {

        var dataOutput = ModelOutputSchema.newBuilder().setObjectType(ObjectType.DATA).setDynamic(true).build();
        var fileOutput = ModelOutputSchema.newBuilder().setObjectType(ObjectType.FILE).build();

        var outputs = new HashMap<String, ModelOutputSchema>();

        for (var captureName : importData.getCapturesMap().keySet()) {
            outputs.put(captureName, dataOutput);
            outputs.put(fileOutputName(captureName), fileOutput);
        }

        return outputs;
    }

    private static String fileOutputName(String captureName) {
        return captureName + "_file";
    }
}
