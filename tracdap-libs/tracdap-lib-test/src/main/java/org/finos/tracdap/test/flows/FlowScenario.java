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

package org.finos.tracdap.test.flows;

import org.finos.tracdap.common.config.ConfigFormat;
import org.finos.tracdap.common.config.ConfigParser;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.metadata.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.protobuf.Message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


/**
 * A flow configuration from the shared flow scenario catalogue.
 *
 * <p>The catalogue is read by the Java validation tests, the platform end-to-end tests
 * and the Python dev-mode tests, which all check a scenario against the same expected.yaml.</p>
 */
public class FlowScenario {

    public static final String CATALOGUE_PATH = "tracdap-runtime/python/test/tracdap_test/flow_scenarios";
    public static final String MODELS_PATH = "tracdap-runtime/python/test";

    public enum Outcome { SUCCEEDED, FAILED, REJECTED }

    /** An input or output of the strict flow; label, fields and file type are checked only when given */
    public static class ExpectedEntry {

        public final ObjectType objectType;
        public final boolean optional;
        public final String label;
        public final List<String> fields;
        public final String fileType;

        ExpectedEntry(ObjectType objectType, boolean optional, String label, List<String> fields, String fileType) {
            this.objectType = objectType;
            this.optional = optional;
            this.label = label;
            this.fields = fields;
            this.fileType = fileType;
        }
    }

    /** A parameter of the strict flow; a null default value means the parameter has none */
    public static class ExpectedParameter {

        public final BasicType paramType;
        public final Value defaultValue;
        public final String label;

        ExpectedParameter(BasicType paramType, Value defaultValue, String label) {
            this.paramType = paramType;
            this.defaultValue = defaultValue;
            this.label = label;
        }
    }

    private static final YAMLMapper YAML = new YAMLMapper();
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ConfigParser PARSER = new ConfigParser();

    private final String name;
    private final Path dataDir;
    private final Map<String, ModelDefinition> interfaces;

    private final FlowDefinition flow;
    private final FlowDefinition flowV2;
    private final Map<String, String> models;
    private final Map<String, Value> parameters;
    private final Map<String, String> inputs;
    private final boolean rerun;

    private final Outcome outcome;
    private final String error;
    private final Set<String> ran;
    private final Map<String, ExpectedParameter> flowParameters;
    private final Map<String, ExpectedEntry> flowInputs;
    private final Map<String, ExpectedEntry> flowOutputs;
    private final Map<String, ObjectType> outputs;
    private final boolean e2e;

    public static Path findTracRepoDir() {

        var repoDir = Paths.get(".").toAbsolutePath();

        while (!Files.exists(repoDir.resolve("tracdap-api")) && !Files.exists(repoDir.resolve(".git")))
            repoDir = repoDir.getParent();

        return repoDir;
    }

    public static List<String> scenarioNames(Path tracRepoDir) {

        var scenariosDir = tracRepoDir.resolve(CATALOGUE_PATH).resolve("scenarios");

        try (var dirs = Files.list(scenariosDir)) {
            return dirs
                    .filter(dir -> Files.exists(dir.resolve("expected.yaml")))
                    .map(dir -> dir.getFileName().toString())
                    .sorted()
                    .collect(Collectors.toList());
        }
        catch (IOException e) {
            throw new ETracInternal("Failed to list flow scenarios: " + e.getMessage(), e);
        }
    }

    public static Map<String, ModelDefinition> modelInterfaces(Path tracRepoDir) {

        var modelsFile = tracRepoDir.resolve(CATALOGUE_PATH).resolve("models.yaml");
        var interfaces = new LinkedHashMap<String, ModelDefinition>();

        readYaml(modelsFile).properties().forEach(entry ->
                interfaces.put(entry.getKey(), parseProto(entry.getValue(), ModelDefinition.class)));

        return interfaces;
    }

    public static FlowScenario load(Path tracRepoDir, String name) {

        var catalogueDir = tracRepoDir.resolve(CATALOGUE_PATH);
        var scenarioDir = catalogueDir.resolve("scenarios").resolve(name);

        return new FlowScenario(name, catalogueDir, scenarioDir, modelInterfaces(tracRepoDir));
    }

    private FlowScenario(String name, Path catalogueDir, Path scenarioDir, Map<String, ModelDefinition> interfaces) {

        this.name = name;
        this.dataDir = catalogueDir.resolve("data");
        this.interfaces = interfaces;

        this.flow = parseProto(readYaml(scenarioDir.resolve("flow.yaml")), FlowDefinition.class);

        var flowV2File = scenarioDir.resolve("flow_v2.yaml");
        this.flowV2 = Files.exists(flowV2File) ? parseProto(readYaml(flowV2File), FlowDefinition.class) : null;

        var job = readYaml(scenarioDir.resolve("job.yaml"));

        this.models = stringMap(job.path("models"));
        this.inputs = stringMap(job.path("inputs"));
        this.rerun = job.path("rerun").asBoolean(false);
        this.parameters = new LinkedHashMap<>();

        job.path("parameters").properties().forEach(param ->
                parameters.put(param.getKey(), encodeParameter(param.getKey(), param.getValue())));

        var expected = readYaml(scenarioDir.resolve("expected.yaml"));

        this.outcome = Outcome.valueOf(expected.path("outcome").asText());
        this.error = expected.path("error").asText("");
        this.ran = expected.has("ran") ? new LinkedHashSet<>(stringList(expected.path("ran"))) : new LinkedHashSet<>(models.keySet());
        this.flowParameters = expectedParameters(expected.path("flow").path("parameters"));
        this.flowInputs = expectedEntries(expected.path("flow").path("inputs"));
        this.flowOutputs = expectedEntries(expected.path("flow").path("outputs"));
        this.e2e = expected.path("e2e").asBoolean(false);
        this.outputs = new LinkedHashMap<>();

        expected.path("outputs").properties().forEach(output ->
                outputs.put(output.getKey(), ObjectType.valueOf(output.getValue().asText())));

        for (var entryPoint : models.values())
            if (!interfaces.containsKey(entryPoint))
                throw new ETracInternal(String.format("Scenario [%s] uses model [%s], which is not in models.yaml", name, entryPoint));

        for (var node : ran)
            if (!models.containsKey(node))
                throw new ETracInternal(String.format("Scenario [%s] expects node [%s] to run, which is not a model node in job.yaml", name, node));

        checkExpectedError(outcome, error, "expected.yaml");

        if (expected.has("devMode")) {

            var devMode = expected.path("devMode");

            if (!devMode.hasNonNull("reason") && !devMode.hasNonNull("skip"))
                throw new ETracInternal(String.format("Scenario [%s]: the devMode override needs a reason or a skip", name));

            var devOutcome = devMode.has("outcome") ? Outcome.valueOf(devMode.path("outcome").asText()) : outcome;
            checkExpectedError(devOutcome, devMode.path("error").asText(error), "the devMode override");
        }
    }

    private void checkExpectedError(Outcome expectedOutcome, String expectedError, String source) {

        if (expectedOutcome != Outcome.SUCCEEDED && expectedError.isEmpty())
            throw new ETracInternal(String.format("Scenario [%s]: %s expects %s, so it needs an error", name, source, expectedOutcome));
    }

    public String name() { return name; }
    public FlowDefinition flow() { return flow; }
    public Optional<FlowDefinition> flowV2() { return Optional.ofNullable(flowV2); }
    public Map<String, String> models() { return models; }
    public Map<String, Value> parameters() { return parameters; }
    public Map<String, String> inputs() { return inputs; }
    public boolean rerun() { return rerun; }

    public Outcome outcome() { return outcome; }
    public String error() { return error; }
    public Set<String> ran() { return ran; }
    public Map<String, ExpectedParameter> flowParameters() { return flowParameters; }
    public Map<String, ExpectedEntry> flowInputs() { return flowInputs; }
    public Map<String, ExpectedEntry> flowOutputs() { return flowOutputs; }
    public Map<String, ObjectType> outputs() { return outputs; }
    public boolean e2e() { return e2e; }

    public Path dataFile(String inputName) {
        return dataDir.resolve(inputs.get(inputName));
    }

    /** The model's interface from models.yaml, as a model stub that loads it from the given repository */
    public ModelDefinition modelDefinition(String nodeName, String repository, String version) {

        var entryPoint = models.get(nodeName);

        return interfaces.get(entryPoint).toBuilder()
                .setLanguage("python")
                .setRepository(repository)
                .setPath(MODELS_PATH)
                .setEntryPoint(entryPoint)
                .setVersion(version)
                .build();
    }

    /** The requirement of the first model input the flow input feeds */
    public ModelInputSchema inputRequirement(String inputName) {

        var edge = flow.getEdgesList().stream()
                .filter(e -> e.getSource().getNode().equals(inputName))
                .filter(e -> flow.getNodesOrThrow(e.getTarget().getNode()).getNodeType() == FlowNodeType.MODEL_NODE)
                .findFirst()
                .orElseThrow(() -> new ETracInternal(String.format("Scenario [%s]: input [%s] feeds no model", name, inputName)));

        var target = edge.getTarget();
        return interfaces.get(models.get(target.getNode())).getInputsOrThrow(target.getSocket());
    }

    /** The model output that produces the flow output */
    public ModelOutputSchema outputRequirement(String outputName) {

        var edge = flow.getEdgesList().stream()
                .filter(e -> e.getTarget().getNode().equals(outputName))
                .findFirst()
                .orElseThrow(() -> new ETracInternal(String.format("Scenario [%s]: output [%s] has no source", name, outputName)));

        var source = edge.getSource();
        return interfaces.get(models.get(source.getNode())).getOutputsOrThrow(source.getSocket());
    }

    @Override
    public String toString() {
        return name;
    }

    private static JsonNode readYaml(Path path) {

        try {
            return YAML.readTree(Files.readAllBytes(path));
        }
        catch (IOException e) {
            throw new ETracInternal(String.format("Failed to read [%s]: %s", path, e.getMessage()), e);
        }
    }

    private static <T extends Message> T parseProto(JsonNode node, Class<T> protoClass) {

        try {
            var json = JSON.writeValueAsString(node).getBytes(StandardCharsets.UTF_8);
            return PARSER.parseConfig(json, ConfigFormat.JSON, protoClass);
        }
        catch (IOException e) {
            throw new ETracInternal("Failed to parse scenario metadata: " + e.getMessage(), e);
        }
    }

    private static Map<String, String> stringMap(JsonNode node) {

        var map = new LinkedHashMap<String, String>();
        node.properties().forEach(entry -> map.put(entry.getKey(), entry.getValue().asText()));

        return map;
    }

    private static Map<String, ExpectedEntry> expectedEntries(JsonNode node) {

        var entries = new LinkedHashMap<String, ExpectedEntry>();

        node.properties().forEach(entry -> entries.put(entry.getKey(), new ExpectedEntry(
                ObjectType.valueOf(entry.getValue().path("objectType").asText()),
                entry.getValue().path("optional").asBoolean(false),
                entry.getValue().path("label").asText(null),
                entry.getValue().has("fields") ? stringList(entry.getValue().path("fields")) : null,
                entry.getValue().path("fileType").asText(null))));

        return entries;
    }

    private Map<String, ExpectedParameter> expectedParameters(JsonNode node) {

        var params = new LinkedHashMap<String, ExpectedParameter>();

        node.properties().forEach(entry -> params.put(entry.getKey(), new ExpectedParameter(
                BasicType.valueOf(entry.getValue().path("paramType").asText()),
                entry.getValue().has("default") ? encodeParameter(entry.getKey(), entry.getValue().path("default")) : null,
                entry.getValue().path("label").asText(null))));

        return params;
    }

    private static List<String> stringList(JsonNode node) {

        var list = new ArrayList<String>();
        node.forEach(item -> list.add(item.asText()));

        return list;
    }

    private Value encodeParameter(String paramName, JsonNode value) {

        if (value.isBoolean())
            return MetadataCodec.encodeValue(value.booleanValue());
        if (value.isIntegralNumber())
            return MetadataCodec.encodeValue(value.longValue());
        if (value.isFloatingPointNumber())
            return MetadataCodec.encodeValue(value.doubleValue());
        if (value.isTextual())
            return MetadataCodec.encodeValue(value.textValue());

        throw new ETracInternal(String.format("Scenario [%s]: unsupported value for parameter [%s]", name, paramName));
    }
}
