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
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.metadata.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class RunModelJob extends RunModelOrFlow implements IJobLogic {

    @Override
    public JobDefinition applyTransform(JobDefinition job, PlatformConfig platformConfig) {

        // No transformations currently required
        return job;
    }

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
    public Map<String, MetadataWriteRequest> newResultIds(
            String tenant, JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var runModel = job.getRunModel();

        var modelKey = MetadataUtil.objectKey(runModel.getModel());
        var modelId = resourceMapping.get(modelKey);
        var modelDef = resources.get(MetadataUtil.objectKey(modelId)).getModel();

        return newResultIds(
                tenant, modelDef.getOutputsMap().keySet(),
                runModel.getPriorOutputsMap());
    }

    @Override
    public Map<String, TagHeader> priorResultIds(
            JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var runModel = job.getRunModel();

        var modelKey = MetadataUtil.objectKey(runModel.getModel());
        var modelId = resourceMapping.get(modelKey);
        var modelDef = resources.get(MetadataUtil.objectKey(modelId)).getModel();

        return priorResultIds(
                modelDef.getOutputsMap().keySet(), runModel.getPriorOutputsMap(),
                resources, resourceMapping);
    }

    @Override
    public JobDefinition setResultIds(
            JobDefinition job, Map<String, TagHeader> resultMapping,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var modelKey = MetadataUtil.objectKey(job.getRunModel().getModel());
        var modelId = resourceMapping.get(modelKey);
        var modelDef = resources.get(MetadataUtil.objectKey(modelId)).getModel();

        var modelOutputs = setResultIds(modelDef.getOutputsMap().keySet(), resultMapping);

        var runModel = job.getRunModel().toBuilder()
                .clearOutputs()
                .putAllOutputs(modelOutputs);

        return job.toBuilder()
                .setRunModel(runModel)
                .build();
    }

    @Override
    public List<MetadataWriteRequest> buildResultMetadata(String tenant, JobConfig jobConfig, JobResult jobResult) {

        var runModel = jobConfig.getJob().getRunModel();

        return buildResultMetadata(
                tenant, runModel.getOutputsMap(), runModel.getPriorOutputsMap(),
                runModel.getOutputAttrsList(), Map.of(),
                jobConfig, jobResult);
    }
}
