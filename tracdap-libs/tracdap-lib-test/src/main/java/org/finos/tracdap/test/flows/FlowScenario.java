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

    public static class ExpectedEntry {

        public final ObjectType objectType;
        public final boolean optional;

        ExpectedEntry(ObjectType objectType, boolean optional) {
            this.objectType = objectType;
            this.optional = optional;
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
        this.flowInputs = expectedEntries(expected.path("flow").path("inputs"));
        this.flowOutputs = expectedEntries(expected.path("flow").path("outputs"));
        this.e2e = expected.path("e2e").asBoolean(false);
        this.outputs = new LinkedHashMap<>();

        expected.path("outputs").properties().forEach(output ->
                outputs.put(output.getKey(), ObjectType.valueOf(output.getValue().asText())));

        for (var entryPoint : models.values())
            if (!interfaces.containsKey(entryPoint))
                throw new ETracInternal(String.format("Scenario [%s] uses model [%s], which is not in models.yaml", name, entryPoint));
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
                entry.getValue().path("optional").asBoolean(false))));

        return entries;
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
