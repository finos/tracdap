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
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.metadata.*;

import java.util.*;


public class RunModelJob extends RunModelOrFlow implements IJobLogic {

    @Override
    public List<TagSelector> requiredMetadata(JobDefinition job) {

        if (job.getJobType() != JobType.RUN_MODEL)
            throw new EUnexpected();

        var runModel = job.getRunModel();

        var resources = new ArrayList<TagSelector>(runModel.getInputsCount() + 1);
        resources.add(runModel.getModel());
        resources.addAll(runModel.getInputsMap().values());
        resources.addAll(runModel.getPriorOutputsMap().values());

        return resources;
    }

    @Override
    public List<String> requiredResources(JobDefinition job, MetadataBundle metadata, TenantConfig tenantConfig) {

        var resources = new HashSet<String>();

        // Storage requirements are the same for model / flow jobs
        addRequiredStorage(metadata, tenantConfig, resources);

        // Include repo resource for the model
        var modelObj = metadata.getObject(job.getRunModel().getModel());
        var modelRepo = modelObj.getModel().getRepository();
        resources.add(modelRepo);

        // Add all target resources selected for the job
        resources.addAll(job.getRunModel().getResourcesMap().values());

        return new ArrayList<>(resources);
    }

    @Override
    public JobDefinition applyJobTransform(JobDefinition job, MetadataBundle metadata, TenantConfig tenantConfig) {

        // No transformations currently required
        return job;
    }

    @Override
    public MetadataBundle applyMetadataTransform(JobDefinition job, MetadataBundle metadata, TenantConfig tenantConfig) {

        return metadata;
    }

    @Override
    public Map<ObjectType, Integer> expectedOutputs(JobDefinition job, MetadataBundle metadata) {

        var runModelJob = job.getRunModel();

        var modelObj = metadata.getObject(runModelJob.getModel());
        var model = modelObj.getModel();

        return expectedOutputs(model.getOutputsMap(), runModelJob.getPriorOutputsMap());
    }

    @Override
    public JobResult processResult(JobConfig jobConfig, JobResult jobResult, Map<String, TagHeader> resultIds) {

        var runModel = jobConfig.getJob().getRunModel();

        var modelKey = MetadataUtil.objectKey(runModel.getModel());
        var modelId = jobConfig.getObjectMappingMap().get(modelKey);
        var modelDef = jobConfig.getObjectsMap().get(MetadataUtil.objectKey(modelId)).getModel();

        return processResult(
                jobResult,
                modelDef.getOutputsMap(),
                runModel.getOutputAttrsList(),
                Map.of(), resultIds);
    }
}
