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

package org.finos.tracdap.common.validation.consistency;

import org.finos.tracdap.api.TracErrorItem;
import org.finos.tracdap.common.exception.EValidation;
import org.finos.tracdap.common.graph.GraphBuilder;
import org.finos.tracdap.common.graph.NodeNamespace;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.ResourceBundle;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.test.flows.FlowScenario;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Validate every scenario in the flow scenario catalogue and build its strict flow, as the orchestrator does.
 */
class FlowScenarioTest {

    private static final Path TRAC_REPO_DIR = FlowScenario.findTracRepoDir();

    private final Validator validator = new Validator();

    static List<String> scenarios() {
        return FlowScenario.scenarioNames(TRAC_REPO_DIR);
    }

    @ParameterizedTest
    @MethodSource("scenarios")
    void scenario(String scenarioName) {

        var scenario = FlowScenario.load(TRAC_REPO_DIR, scenarioName);
        var job = new ScenarioJob(scenario);

        if (!checkValid(scenario, () -> validator.validateFixedObject(scenario.flow())))
            return;

        if (scenario.flowV2().isPresent() && !checkValid(scenario, () -> validator.validateFixedObject(scenario.flowV2().get())))
            return;

        if (!checkValid(scenario, () -> validator.validateConsistency(job.definition(false), job.metadata, ResourceBundle.empty())))
            return;

        if (scenario.outcome() == FlowScenario.Outcome.REJECTED)
            Assertions.fail(String.format("Expected [%s] to be rejected", scenarioName));

        if (scenario.rerun())
            validator.validateConsistency(job.definition(true), job.metadata, ResourceBundle.empty());

        var graphBuilder = new GraphBuilder(NodeNamespace.ROOT, job.metadata, ResourceBundle.empty());
        var graph = graphBuilder.buildJob(job.definition(scenario.rerun()));
        var strictFlow = graphBuilder.exportFlow(graph);

        checkEntries("inputs", scenario.flowInputs(), strictFlow.getInputsMap());
        checkEntries("outputs", scenario.flowOutputs(), strictFlow.getOutputsMap());
    }

    private boolean checkValid(FlowScenario scenario, Runnable validation) {

        try {
            validation.run();
            return true;
        }
        catch (EValidation e) {

            if (scenario.outcome() != FlowScenario.Outcome.REJECTED)
                Assertions.fail(String.format("Scenario [%s] failed validation: %s", scenario.name(), e.getMessage()), e);

            var details = e.getDetails().getItemsList().stream()
                    .map(TracErrorItem::getDetail)
                    .collect(Collectors.toList());

            Assertions.assertTrue(
                    details.stream().anyMatch(detail -> detail.startsWith(scenario.error())),
                    String.format("Expected an error starting [%s], got %s", scenario.error(), details));

            return false;
        }
    }

    private static <T extends Message> void checkEntries(
            String section, Map<String, FlowScenario.ExpectedEntry> expected, Map<String, T> strict) {

        Assertions.assertEquals(expected.keySet(), strict.keySet(), "Flow " + section);

        for (var entry : expected.entrySet()) {

            var strictEntry = strict.get(entry.getKey());
            var objectType = strictEntry instanceof ModelInputSchema
                    ? ((ModelInputSchema) strictEntry).getObjectType()
                    : ((ModelOutputSchema) strictEntry).getObjectType();
            var optional = strictEntry instanceof ModelInputSchema
                    ? ((ModelInputSchema) strictEntry).getOptional()
                    : ((ModelOutputSchema) strictEntry).getOptional();

            Assertions.assertEquals(entry.getValue().objectType, objectType, String.format("Object type of flow %s [%s]", section, entry.getKey()));
            Assertions.assertEquals(entry.getValue().optional, optional, String.format("Optional flag of flow %s [%s]", section, entry.getKey()));
        }
    }

    /**
     * The objects a scenario's job refers to, held in memory as the orchestrator holds them after loading.
     * Supplied inputs and prior outputs take the schema or file type of the model socket they connect to.
     */
    private static class ScenarioJob {

        final FlowScenario scenario;
        final MetadataBundle metadata = MetadataBundle.empty();

        final TagHeader flowId;
        final Map<String, TagHeader> modelIds = new LinkedHashMap<>();
        final Map<String, TagHeader> inputIds = new LinkedHashMap<>();
        final Map<String, TagHeader> priorOutputIds = new LinkedHashMap<>();

        ScenarioJob(FlowScenario scenario) {

            this.scenario = scenario;

            flowId = add(ObjectType.FLOW, 1, ObjectDefinition.newBuilder()
                    .setObjectType(ObjectType.FLOW)
                    .setFlow(scenario.flow())
                    .build());

            scenario.flowV2().ifPresent(flowV2 -> add(flowId, 2, ObjectDefinition.newBuilder()
                    .setObjectType(ObjectType.FLOW)
                    .setFlow(flowV2)
                    .build()));

            for (var node : scenario.models().keySet()) {
                modelIds.put(node, add(ObjectType.MODEL, 1, ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.MODEL)
                        .setModel(scenario.modelDefinition(node, "scenario_repo", "scenario"))
                        .build()));
            }

            for (var input : scenario.inputs().keySet()) {
                var requirement = scenario.inputRequirement(input);
                inputIds.put(input, addObject(requirement.getObjectType(), requirement.getSchema(), requirement.getFileType()));
            }

            for (var output : scenario.outputs().keySet()) {
                var requirement = scenario.outputRequirement(output);
                priorOutputIds.put(output, addObject(requirement.getObjectType(), requirement.getSchema(), requirement.getFileType()));
            }
        }

        JobDefinition definition(boolean withPriorOutputs) {

            var runFlow = RunFlowJob.newBuilder()
                    .setFlow(MetadataUtil.selectorFor(flowId))
                    .putAllParameters(scenario.parameters());

            modelIds.forEach((node, id) -> runFlow.putModels(node, MetadataUtil.selectorFor(id)));
            inputIds.forEach((input, id) -> runFlow.putInputs(input, MetadataUtil.selectorFor(id)));

            if (withPriorOutputs)
                priorOutputIds.forEach((output, id) -> runFlow.putPriorOutputs(output, MetadataUtil.selectorFor(id)));

            return JobDefinition.newBuilder()
                    .setJobType(JobType.RUN_FLOW)
                    .setRunFlow(runFlow)
                    .build();
        }

        private TagHeader addObject(ObjectType objectType, SchemaDefinition schema, FileType fileType) {

            if (objectType == ObjectType.FILE) {
                return add(ObjectType.FILE, 1, ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.FILE)
                        .setFile(FileDefinition.newBuilder()
                                .setName("scenario_file." + fileType.getExtension())
                                .setExtension(fileType.getExtension())
                                .setMimeType(fileType.getMimeType()))
                        .build());
            }

            return add(ObjectType.DATA, 1, ObjectDefinition.newBuilder()
                    .setObjectType(ObjectType.DATA)
                    .setData(DataDefinition.newBuilder().setSchema(schema))
                    .build());
        }

        private TagHeader add(ObjectType objectType, int version, ObjectDefinition definition) {

            var header = TagHeader.newBuilder()
                    .setObjectType(objectType)
                    .setObjectId(UUID.randomUUID().toString())
                    .build();

            return add(header, version, definition);
        }

        private TagHeader add(TagHeader objectId, int version, ObjectDefinition definition) {

            var header = objectId.toBuilder()
                    .setObjectVersion(version)
                    .setTagVersion(1)
                    .build();

            metadata.getObjects().put(MetadataUtil.objectKey(header), definition);

            return header;
        }
    }
}
