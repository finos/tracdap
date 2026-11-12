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

import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.metadata.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


public class RunJobOutputAttrsTest {

    private static final List<String> OUTPUTS = List.of("output_1", "output_2");

    private static final ModelOutputSchema DATA_OUTPUT = ModelOutputSchema.newBuilder()
            .setObjectType(ObjectType.DATA)
            .build();

    private static TagHeader newId(ObjectType objectType) {

        return TagHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();
    }

    private static TagUpdate attr(String attrName, String value) {

        return TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_OR_REPLACE_ATTR)
                .setAttrName(attrName)
                .setValue(MetadataCodec.encodeValue(value))
                .build();
    }

    private static TagUpdateList attrs(TagUpdate... attrs) {

        return TagUpdateList.newBuilder().addAllAttrs(List.of(attrs)).build();
    }

    @Test
    void runModel_perOutputAttrsOnTheirOutputOnly() {

        var runModel = org.finos.tracdap.metadata.RunModelJob.newBuilder()
                .addOutputAttrs(attr("job_tag", "monthly"))
                .putPerOutputAttrs("output_1", attrs(attr("key", "output_1"), attr("name", "Output one")));

        var result = runModel(runModel);

        Assertions.assertEquals(
                List.of("job_tag", "key", "name"),
                attrNames(result, "output_1"));

        Assertions.assertEquals(
                List.of("job_tag"),
                attrNames(result, "output_2"));
    }

    @Test
    void runModel_perOutputAttrsAfterJobAttrs() {

        var runModel = org.finos.tracdap.metadata.RunModelJob.newBuilder()
                .addOutputAttrs(attr("name", "Job-wide name"))
                .putPerOutputAttrs("output_1", attrs(attr("name", "Output one")));

        var result = runModel(runModel);

        Assertions.assertEquals("Output one", lastValue(result, "output_1", "name"));
        Assertions.assertEquals("Job-wide name", lastValue(result, "output_2", "name"));
    }

    @Test
    void runFlow_perOutputAttrsAfterNodeAttrs() {

        var runFlow = org.finos.tracdap.metadata.RunFlowJob.newBuilder()
                .addOutputAttrs(attr("job_tag", "monthly"))
                .putPerOutputAttrs("output_1", attrs(attr("name", "Output one")));

        var nodeAttrs = Map.of(
                "output_1", List.of(attr("name", "Stale node name")),
                "output_2", List.of(attr("name", "Node name")));

        var result = runFlow(runFlow, nodeAttrs);

        Assertions.assertEquals(
                List.of("job_tag", "name", "name"),
                attrNames(result, "output_1"));

        Assertions.assertEquals("Output one", lastValue(result, "output_1", "name"));
        Assertions.assertEquals("Node name", lastValue(result, "output_2", "name"));
    }

    private static JobResult runModel(org.finos.tracdap.metadata.RunModelJob.Builder runModel) {

        var modelId = newId(ObjectType.MODEL);
        var modelSelector = MetadataUtil.selectorFor(modelId);

        var modelDef = ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setModel(ModelDefinition.newBuilder()
                        .putOutputs("output_1", DATA_OUTPUT)
                        .putOutputs("output_2", DATA_OUTPUT))
                .build();

        var jobConfig = JobConfig.newBuilder()
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_MODEL)
                        .setRunModel(runModel.setModel(modelSelector)))
                .putObjectMapping(MetadataUtil.objectKey(modelSelector), modelId)
                .putObjects(MetadataUtil.objectKey(modelId), modelDef)
                .build();

        var resultIds = new HashMap<String, TagHeader>();
        var runtimeResult = runtimeResult(resultIds);

        return new RunModelJob().processResult(jobConfig, runtimeResult, resultIds);
    }

    private static JobResult runFlow(org.finos.tracdap.metadata.RunFlowJob.Builder runFlow, Map<String, List<TagUpdate>> nodeAttrs) {

        var flowId = newId(ObjectType.FLOW);
        var flowSelector = MetadataUtil.selectorFor(flowId);

        var flow = FlowDefinition.newBuilder();

        for (var output : OUTPUTS) {
            flow.putOutputs(output, DATA_OUTPUT);
            flow.putNodes(output, FlowNode.newBuilder()
                    .setNodeType(FlowNodeType.OUTPUT_NODE)
                    .addAllNodeAttrs(nodeAttrs.getOrDefault(output, List.of()))
                    .build());
        }

        var flowDef = ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.FLOW)
                .setFlow(flow)
                .build();

        var jobConfig = JobConfig.newBuilder()
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_FLOW)
                        .setRunFlow(runFlow.setFlow(flowSelector)))
                .putObjectMapping(MetadataUtil.objectKey(flowSelector), flowId)
                .putObjects(MetadataUtil.objectKey(flowId), flowDef)
                .build();

        var resultIds = new HashMap<String, TagHeader>();
        var runtimeResult = runtimeResult(resultIds);

        return new RunFlowJob().processResult(jobConfig, runtimeResult, resultIds);
    }

    // A runtime result with a dataset and its storage for each output, registering their IDs in resultIds
    private static JobResult runtimeResult(Map<String, TagHeader> resultIds) {

        var result = ResultDefinition.newBuilder();
        var jobResult = JobResult.newBuilder();

        for (var output : OUTPUTS) {

            var dataId = newId(ObjectType.DATA);
            var storageId = newId(ObjectType.STORAGE);

            resultIds.put(dataId.getObjectId(), dataId);
            resultIds.put(storageId.getObjectId(), storageId);

            result.putOutputs(output, MetadataUtil.selectorFor(dataId));

            jobResult.putObjects(MetadataUtil.objectKey(dataId), ObjectDefinition.newBuilder()
                    .setObjectType(ObjectType.DATA)
                    .setData(DataDefinition.newBuilder().setStorageId(MetadataUtil.selectorFor(storageId)))
                    .build());

            jobResult.putObjects(MetadataUtil.objectKey(storageId), ObjectDefinition.newBuilder()
                    .setObjectType(ObjectType.STORAGE)
                    .build());
        }

        return jobResult.setResult(result).build();
    }

    private static List<TagUpdate> outputAttrs(JobResult result, String outputName) {

        return result.getAttrsMap().values().stream()
                .map(attrs -> attrs.getAttrsList())
                .filter(attrs -> attrs.stream().anyMatch(attr ->
                        attr.getAttrName().equals("trac_job_output") &&
                        MetadataCodec.decodeStringValue(attr.getValue()).equals(outputName)))
                .findFirst()
                .orElseThrow();
    }

    private static List<String> attrNames(JobResult result, String outputName) {

        return outputAttrs(result, outputName).stream()
                .map(TagUpdate::getAttrName)
                .filter(name -> !name.startsWith("trac_"))
                .collect(Collectors.toList());
    }

    private static String lastValue(JobResult result, String outputName, String attrName) {

        var attrs = outputAttrs(result, outputName).stream()
                .filter(attr -> attr.getAttrName().equals(attrName))
                .collect(Collectors.toList());

        return MetadataCodec.decodeStringValue(attrs.get(attrs.size() - 1).getValue());
    }
}
