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
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.ResourceBundle;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.metadata.*;

import java.util.*;


public class ExportDataJob extends RunModelOrFlow implements IJobLogic {

    @Override
    public List<TagSelector> requiredMetadata(JobDefinition job) {

        if (job.getJobType() != JobType.EXPORT_DATA)
            throw new EUnexpected();

        var exportData = job.getExportData();

        var resources = new ArrayList<TagSelector>(exportData.getInputsCount() + 1);
        resources.add(exportData.getModel());
        resources.addAll(exportData.getInputsMap().values());
        resources.addAll(exportData.getPriorOutputsMap().values());

        return resources;
    }

    @Override
    public List<String> requiredResources(JobDefinition job, MetadataBundle metadata) {

        var resources = new HashSet<String>();

        addRequiredStorage(metadata, resources);

        var modelObj = metadata.getObject(job.getExportData().getModel());
        var modelRepo = modelObj.getModel().getRepository();
        resources.add(modelRepo);

        resources.addAll(job.getExportData().getStorageAccessList());

        return new ArrayList<>(resources);
    }

    @Override
    public JobDefinition applyJobTransform(JobDefinition job, MetadataBundle metadata, ResourceBundle resources) {

        // No transformations currently required
        return job;
    }

    @Override
    public MetadataBundle applyMetadataTransform(JobDefinition job, MetadataBundle metadata, ResourceBundle resources) {

        return metadata;
    }

    @Override
    public Map<ObjectType, Integer> expectedOutputs(JobDefinition job, MetadataBundle metadata) {

        var exportDataJob = job.getExportData();

        var modelObj = metadata.getObject(exportDataJob.getModel());
        var model = modelObj.getModel();

        return expectedOutputs(model.getOutputsMap(), exportDataJob.getPriorOutputsMap());
    }

    @Override
    public JobResult processResult(JobConfig jobConfig, JobResult jobResult, Map<String, TagHeader> resultIds) {

        var exportData = jobConfig.getJob().getExportData();

        var modelKey = MetadataUtil.objectKey(exportData.getModel());
        var modelId = jobConfig.getObjectMappingMap().get(modelKey);
        var modelDef = jobConfig.getObjectsMap().get(MetadataUtil.objectKey(modelId)).getModel();

        return processResult(
                jobResult,
                modelDef.getOutputsMap(),
                exportData.getOutputAttrsList(),
                Map.of(), resultIds);
    }
}
