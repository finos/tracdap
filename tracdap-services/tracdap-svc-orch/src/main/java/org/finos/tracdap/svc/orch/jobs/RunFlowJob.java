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
import org.finos.tracdap.common.graph.GraphBuilder;
import org.finos.tracdap.common.graph.NodeNamespace;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.metadata.*;

import java.util.*;
import java.util.stream.Collectors;


public class RunFlowJob extends RunModelOrFlow implements IJobLogic {

    @Override
    public JobDefinition applyTransform(JobDefinition job, MetadataBundle metadata, PlatformConfig platformConfig) {

        // No transformations currently required
        return job;
    }

    @Override
    public MetadataBundle applyMetadataTransform(JobDefinition job, MetadataBundle metadata, PlatformConfig platformConfig) {

        // Running the graph builder will apply any required auto-wiring and type inference to the flow
        // This creates a strictly consistent flow that can be sent to the runtime

        var flowSelector = job.getRunFlow().getFlow();

        var jobNamespace = NodeNamespace.ROOT;
        var builder = new GraphBuilder(jobNamespace, metadata);
        var graph = builder.buildJob(job);

        var strictFlow = builder.exportFlow(graph);

        var strictFlowObj = ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.FLOW)
                .setFlow(strictFlow)
                .build();

        return metadata.withUpdate(flowSelector, strictFlowObj);
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
        resources.addAll(runFlow.getPriorOutputsMap().values());

        return resources;
    }

    @Override
    public Map<String, MetadataWriteRequest> newResultIds(
            String tenant, JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var runFlow = job.getRunFlow();

        var outputFlowNodes = getFlowOutputNodes(runFlow.getFlow(), resources, resourceMapping);
        var outputs = getFlowOutputNames(outputFlowNodes);

        return newResultIds(tenant, outputs, runFlow.getPriorOutputsMap());
    }

    @Override
    public Map<String, TagHeader> priorResultIds(
            JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var runFlow = job.getRunFlow();

        var outputFlowNodes = getFlowOutputNodes(runFlow.getFlow(), resources, resourceMapping);
        var outputs = getFlowOutputNames(outputFlowNodes);

        return priorResultIds(outputs, runFlow.getPriorOutputsMap(), resources, resourceMapping);
    }

    private static Set<String> getFlowOutputNames(Map<String, FlowNode> outputFlowNodes) {
        return new HashSet<>(outputFlowNodes.keySet());
    }

    private static Map<String, FlowNode> getFlowOutputNodes(
            TagSelector flowSelector,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {
        var flowKey = MetadataUtil.objectKey(flowSelector);
        var flowId = resourceMapping.get(flowKey);
        var flowDef = resources.get(MetadataUtil.objectKey(flowId)).getFlow();

        return flowDef.getNodesMap().entrySet().stream()
                .filter(nodeEntry -> nodeEntry.getValue().getNodeType() == FlowNodeType.OUTPUT_NODE)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public JobDefinition setResultIds(
            JobDefinition job, Map<String, TagHeader> resultMapping,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        var outputFlowNodes = getFlowOutputNodes(job.getRunFlow().getFlow(), resources, resourceMapping);
        var flowOutputNames = getFlowOutputNames(outputFlowNodes);

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
        var outputFlowNodes = getFlowOutputNodes(
                jobConfig.getJob().getRunFlow().getFlow(),
                jobConfig.getResourcesMap(),
                jobConfig.getResourceMappingMap()
        );

        var perNodeOutputAttrs = outputFlowNodes.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getNodeAttrsList()));

        return buildResultMetadata(
                tenant, runFlow.getOutputsMap(), runFlow.getPriorOutputsMap(),
                runFlow.getOutputAttrsList(), perNodeOutputAttrs,
                jobConfig, jobResult);
    }
}
