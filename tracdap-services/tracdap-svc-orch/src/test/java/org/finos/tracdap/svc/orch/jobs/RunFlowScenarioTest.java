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

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.metadata.RunFlowJob;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.flows.FlowScenario;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;


/**
 * Run the scenarios marked e2e in the flow scenario catalogue on the platform, through the orchestrator
 * and the local executor, and check each against its expected.yaml.
 */
@Tag("integration")
@Tag("int-e2e")
public class RunFlowScenarioTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";
    private static final String MODEL_REPO = "TRAC_LOCAL_REPO";

    private static final String MODEL_LOG_LINE = "Scenario model [%s] ran";

    private static final Set<JobStatusCode> COMPLETED_STATES = Set.of(
            JobStatusCode.SUCCEEDED, JobStatusCode.FAILED, JobStatusCode.CANCELLED);

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(E2E_CONFIG, List.of(E2E_TENANTS))
            .runDbDeploy(true)
            .runCacheDeploy(true)
            .addTenant(TEST_TENANT)
            .prepareLocalExecutor(true)
            .startService(TracMetadataService.class)
            .startService(TracDataService.class)
            .startService(TracOrchestratorService.class)
            .startService(TracAdminService.class)
            .build();

    private static final Map<String, TagHeader> importedModels = new ConcurrentHashMap<>();

    static List<String> scenarios() {

        var tracRepoDir = FlowScenario.findTracRepoDir();

        return FlowScenario.scenarioNames(tracRepoDir).stream()
                .filter(name -> FlowScenario.load(tracRepoDir, name).e2e())
                .collect(Collectors.toList());
    }

    @ParameterizedTest
    @MethodSource("scenarios")
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void scenario(String scenarioName) throws Exception {

        var scenario = FlowScenario.load(platform.tracRepoDir(), scenarioName);

        var flowId = createFlow(scenario);
        var models = importModels(scenario);
        var inputs = loadInputs(scenario);

        var jobId = runFlow(scenario, flowId, models, inputs, Map.of());
        var result = checkResult(scenario, jobId, 1);

        if (scenario.rerun())
            checkResult(scenario, runFlow(scenario, flowId, models, inputs, result.getOutputsMap()), 2);
    }

    private TagHeader createFlow(FlowScenario scenario) {

        var metaClient = platform.metaClientBlocking();

        var flowId = metaClient.createObject(MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.FLOW)
                        .setFlow(scenario.flow()))
                .build());

        scenario.flowV2().ifPresent(flowV2 -> metaClient.updateObject(MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setPriorVersion(MetadataUtil.selectorFor(flowId))
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.FLOW)
                        .setFlow(flowV2))
                .build()));

        return flowId;
    }

    private Map<String, TagHeader> importModels(FlowScenario scenario) throws Exception {

        var modelVersion = GitHelpers.getCurrentCommit();
        var models = new LinkedHashMap<String, TagHeader>();

        for (var node : scenario.models().keySet()) {

            var entryPoint = scenario.models().get(node);

            if (!importedModels.containsKey(entryPoint)) {
                var modelStub = scenario.modelDefinition(node, MODEL_REPO, modelVersion);
                var modelTag = Helpers.doModelImport(platform, TEST_TENANT, modelStub, List.of(), List.of());
                importedModels.put(entryPoint, modelTag.getHeader());
            }

            models.put(node, importedModels.get(entryPoint));
        }

        return models;
    }

    private Map<String, TagHeader> loadInputs(FlowScenario scenario) throws Exception {

        var dataClient = platform.dataClientBlocking();
        var inputs = new LinkedHashMap<String, TagHeader>();

        for (var input : scenario.inputs().keySet()) {

            var requirement = scenario.inputRequirement(input);
            var dataFile = scenario.dataFile(input);
            var content = ByteString.copyFrom(Files.readAllBytes(dataFile));

            if (requirement.getObjectType() == ObjectType.FILE) {

                inputs.put(input, dataClient.createSmallFile(FileWriteRequest.newBuilder()
                        .setTenant(TEST_TENANT)
                        .setName(dataFile.getFileName().toString())
                        .setMimeType(requirement.getFileType().getMimeType())
                        .setContent(content)
                        .build()));
            }
            else {

                inputs.put(input, dataClient.createSmallDataset(DataWriteRequest.newBuilder()
                        .setTenant(TEST_TENANT)
                        .setSchema(requirement.getSchema())
                        .setFormat("text/csv")
                        .setContent(content)
                        .build()));
            }
        }

        return inputs;
    }

    private TagHeader runFlow(
            FlowScenario scenario, TagHeader flowId,
            Map<String, TagHeader> models, Map<String, TagHeader> inputs,
            Map<String, TagSelector> priorOutputs) {

        var runFlow = RunFlowJob.newBuilder()
                .setFlow(MetadataUtil.selectorFor(flowId))
                .putAllParameters(scenario.parameters())
                .putAllPriorOutputs(priorOutputs);

        models.forEach((node, id) -> runFlow.putModels(node, MetadataUtil.selectorFor(id)));
        inputs.forEach((input, id) -> runFlow.putInputs(input, MetadataUtil.selectorFor(id)));

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_FLOW)
                        .setRunFlow(runFlow))
                .build();

        return Helpers.startJob(platform.orchClientBlocking(), jobRequest).getJobId();
    }

    private ResultDefinition checkResult(FlowScenario scenario, TagHeader jobId, int expectedVersion) {

        var metaClient = platform.metaClientBlocking();
        var jobStatus = waitForCompletion(jobId);

        var jobTag = metaClient.readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorForLatest(jobId))
                .build());

        var result = metaClient.readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(jobTag.getDefinition().getJob().getResultId())
                .build())
                .getDefinition().getResult();

        if (scenario.outcome() == FlowScenario.Outcome.SUCCEEDED) {
            Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode(), jobStatus.getStatusMessage());
        }
        else {
            Assertions.assertEquals(JobStatusCode.FAILED, jobStatus.getStatusCode());
            Assertions.assertTrue(
                    result.getStatusMessage().startsWith(scenario.error()),
                    String.format("Expected an error starting [%s], got [%s]", scenario.error(), result.getStatusMessage()));
        }

        Assertions.assertEquals(scenario.outputs().keySet(), result.getOutputsMap().keySet(), "Saved outputs");

        for (var output : scenario.outputs().entrySet()) {

            var outputTag = metaClient.readObject(MetadataReadRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSelector(result.getOutputsOrThrow(output.getKey()))
                    .build());

            var header = outputTag.getHeader();
            Assertions.assertEquals(output.getValue(), header.getObjectType(), "Output [" + output.getKey() + "]");
            Assertions.assertEquals(expectedVersion, header.getObjectVersion(), "Version of output [" + output.getKey() + "]");

            var outputNode = scenario.flow().getNodesOrThrow(output.getKey());

            for (var nodeAttr : outputNode.getNodeAttrsList())
                Assertions.assertEquals(nodeAttr.getValue(), outputTag.getAttrsOrThrow(nodeAttr.getAttrName()));
        }

        checkModelLogLines(scenario, result);

        return result;
    }

    private void checkModelLogLines(FlowScenario scenario, ResultDefinition result) {

        Assertions.assertTrue(result.hasLogFileId(), "Job has no log file");

        var logFile = platform.dataClientBlocking().readSmallFile(FileReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(result.getLogFileId())
                .build());

        var logText = logFile.getContent().toStringUtf8();

        var classesRun = scenario.models().entrySet().stream()
                .filter(node -> scenario.ran().contains(node.getKey()))
                .map(node -> modelClass(node.getValue()))
                .collect(Collectors.toSet());

        var classesNotRun = scenario.models().values().stream()
                .map(RunFlowScenarioTest::modelClass)
                .filter(modelClass -> !classesRun.contains(modelClass))
                .collect(Collectors.toSet());

        for (var modelClass : classesRun)
            Assertions.assertTrue(logText.contains(String.format(MODEL_LOG_LINE, modelClass)), "Log line for " + modelClass);

        for (var modelClass : classesNotRun)
            Assertions.assertFalse(logText.contains(String.format(MODEL_LOG_LINE, modelClass)), "Unexpected log line for " + modelClass);
    }

    private static String modelClass(String entryPoint) {
        return entryPoint.substring(entryPoint.lastIndexOf('.') + 1);
    }

    private JobStatus waitForCompletion(TagHeader jobId) {

        var statusRequest = JobStatusRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(jobId))
                .build();

        var orchClient = platform.orchClientBlocking();
        var jobStatus = orchClient.checkJob(statusRequest);

        while (!COMPLETED_STATES.contains(jobStatus.getStatusCode())) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            jobStatus = orchClient.checkJob(statusRequest);
        }

        return jobStatus;
    }
}
