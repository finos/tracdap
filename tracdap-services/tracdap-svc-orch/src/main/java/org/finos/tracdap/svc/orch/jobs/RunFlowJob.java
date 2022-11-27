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
import java.util.Set;
import java.util.stream.Collectors;


public class RunFlowJob extends RunModelOrFlow implements IJobLogic {

    @Override
    public JobDefinition applyTransform(JobDefinition job, PlatformConfig platformConfig) {

        // No transformations currently required
        return job;
    }

    @Override
    public List<TagSelector> requiredMetadata(JobDefinition job) {

        if (job.getJobType() != JobType.RUN_FLOW)
            throw new EUnexpected();

        var runFlow = job.getRunFlow();

        var resources = new ArrayList<TagSelector>(runFlow.getInputsCount() + runFlow.getModelsCount() + 1);
        resources.add(runFlow.getFlow());
        resources.addAll(runFlow.getInputsMap().values());
        resources.addAll(runFlow.getModelsMap().values());

        return resources;
    }

    @Override
    public Map<String, MetadataWriteRequest> newResultIds(
            String tenant, JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var runFlow = job.getRunFlow();

        var outputs = flowOutputs(runFlow.getFlow(), resources, resourceMapping);

        return newResultIds(tenant, outputs, runFlow.getPriorOutputsMap());
    }

    @Override
    public Map<String, TagHeader> priorResultIds(
            JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var runFlow = job.getRunFlow();

        var outputs = flowOutputs(runFlow.getFlow(), resources, resourceMapping);

        return priorResultIds(outputs, runFlow.getPriorOutputsMap(), resources, resourceMapping);
    }

    private Set<String> flowOutputs(
            TagSelector flowSelector,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var flowKey = MetadataUtil.objectKey(flowSelector);
        var flowId = resourceMapping.get(flowKey);
        var flowDef = resources.get(MetadataUtil.objectKey(flowId)).getFlow();

        return flowDef.getNodesMap().entrySet().stream()
                .filter(nodeEntry -> nodeEntry.getValue().getNodeType() == FlowNodeType.OUTPUT_NODE)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    public JobDefinition setResultIds(
            JobDefinition job, Map<String, TagHeader> resultMapping,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var flowOutputNames = flowOutputs(job.getRunFlow().getFlow(), resources, resourceMapping);

        var flowOutputSelectors = setResultIds(flowOutputNames, resultMapping);

        var runFlow = job.getRunFlow().toBuilder()
                .clearOutputs()
                .putAllOutputs(flowOutputSelectors);

        return job.toBuilder()
                .setRunFlow(runFlow)
                .build();
    }

    @Override
    public List<MetadataWriteRequest> buildResultMetadata(String tenant, JobConfig jobConfig, JobResult jobResult) {

        var runFlow = jobConfig.getJob().getRunFlow();

        return buildResultMetadata(
                tenant, runFlow.getOutputsMap(), runFlow.getPriorOutputsMap(),
                runFlow.getOutputAttrsList(), jobConfig, jobResult);
    }
}
