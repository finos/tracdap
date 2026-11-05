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
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.protobuf.Message;

import java.io.IOException;
import java.nio.ByteBuffer;
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
    private static final CsvMapper CSV = new CsvMapper();

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
    private final Path expectedDir;

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
        this.expectedDir = scenarioDir.resolve("expected");
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

        var expectedFiles = new TreeSet<String>();

        if (Files.isDirectory(expectedDir)) {
            try (var files = Files.list(expectedDir)) {
                files.forEach(file -> expectedFiles.add(file.getFileName().toString().replaceFirst("\\.[^.]+$", "")));
            }
            catch (IOException e) {
                throw new ETracInternal(String.format("Failed to read [%s]: %s", expectedDir, e.getMessage()), e);
            }
        }

        var savedOutputs = new TreeSet<>(outputs.keySet());
        expected.path("devMode").path("outputs").fieldNames().forEachRemaining(savedOutputs::add);

        if (!expectedFiles.equals(savedOutputs))
            throw new ETracInternal(String.format(
                    "Scenario [%s]: every output saved by either runner needs one file in expected/ (outputs %s, files %s)",
                    name, savedOutputs, expectedFiles));

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
    public Path dataFile(String inputName) {
        return dataDir.resolve(inputs.get(inputName));
    }

    /**
     * The data file and schema the platform loads for an input. A dynamic input's file carries its own schema,
     * so the platform loads the CSV file of the same name, with the schema beside it in {@code <name>.schema.yaml}.
     */
    public Map.Entry<Path, SchemaDefinition> platformData(String inputName) {

        var requirement = inputRequirement(inputName);
        var dataFile = dataFile(inputName);

        if (!requirement.getDynamic())
            return Map.entry(dataFile, requirement.getSchema());

        var baseName = dataFile.getFileName().toString().replaceFirst("\\.[^.]+$", "");
        var schema = parseProto(readYaml(dataDir.resolve(baseName + ".schema.yaml")), SchemaDefinition.class);

        return Map.entry(dataDir.resolve(baseName + ".csv"), schema);
    }

    /** The content the platform loads for an input: the data file, with a struct given as JSON */
    public byte[] platformContent(String inputName) {

        var platformData = platformData(inputName);

        try {

            var content = Files.readAllBytes(platformData.getKey());

            return platformData.getValue().getSchemaType() == SchemaType.STRUCT_SCHEMA
                    ? JSON.writeValueAsBytes(YAML.readTree(content))
                    : content;
        }
        catch (IOException e) {
            throw new ETracInternal(String.format("Failed to read [%s]: %s", platformData.getKey(), e.getMessage()), e);
        }
    }

    /** The file in expected/ holding the content a saved output must have */
    public Path expectedOutput(String outputName) {

        try (var files = Files.list(expectedDir)) {
            return files
                    .filter(file -> file.getFileName().toString().replaceFirst("\\.[^.]+$", "").equals(outputName))
                    .findFirst()
                    .orElseThrow();
        }
        catch (IOException e) {
            throw new ETracInternal(String.format("Failed to read [%s]: %s", expectedDir, e.getMessage()), e);
        }
    }

    /** The content a saved output must have, from expected/, in the form {@link #outputContent} gives */
    public Object expectedContent(String outputName) {

        var expectedFile = expectedOutput(outputName);

        try {
            return outputContent(outputName, Files.readAllBytes(expectedFile));
        }
        catch (IOException e) {
            throw new ETracInternal(String.format("Failed to read [%s]: %s", expectedFile, e.getMessage()), e);
        }
    }

    /**
     * A saved output's content in a comparable form: a table (CSV) as its columns and its rows, typed by the
     * output's schema and sorted, a struct (YAML or JSON) as its values, and a file as its bytes.
     */
    public Object outputContent(String outputName, byte[] content) {

        var socket = outputRequirement(outputName);

        if (socket.getObjectType() == ObjectType.FILE)
            return ByteBuffer.wrap(content);

        try {

            if (socket.getSchema().getSchemaType() == SchemaType.STRUCT_SCHEMA)
                return JSON.convertValue(YAML.readTree(content), Object.class);

            var fieldTypes = socket.getSchema().getTable().getFieldsList().stream()
                    .collect(Collectors.toMap(FieldSchema::getFieldName, FieldSchema::getFieldType));

            List<List<String>> csvRows = CSV.readerForListOf(String.class)
                    .with(CsvParser.Feature.WRAP_AS_ARRAY)
                    .<List<String>>readValues(content)
                    .readAll();

            var columns = csvRows.isEmpty() ? List.<String>of() : csvRows.get(0);
            var rows = new ArrayList<List<Object>>();

            for (var csvRow : csvRows.subList(Math.min(1, csvRows.size()), csvRows.size())) {
                var row = new ArrayList<Object>();
                for (var i = 0; i < columns.size(); i++)
                    row.add(csvValue(csvRow.get(i), fieldTypes.get(columns.get(i))));
                rows.add(row);
            }

            rows.sort(Comparator.comparing(Object::toString));

            var table = new LinkedHashMap<String, Object>();
            table.put("columns", columns);
            table.put("rows", rows);

            return table;
        }
        catch (IOException e) {
            throw new ETracInternal(String.format("Scenario [%s]: failed to read output [%s]: %s", name, outputName, e.getMessage()), e);
        }
    }

    /** Where two contents from {@link #outputContent} differ, or null when they match, with floats compared to 1e-9 */
    public static String contentMismatch(Object expected, Object actual) {
        return contentMismatch("", expected, actual);
    }

    private static String contentMismatch(String path, Object expected, Object actual) {

        if (expected == null || actual == null)
            return Objects.equals(expected, actual) ? null : String.format("%s: expected %s, got %s", path, expected, actual);

        if (expected instanceof Number && actual instanceof Number) {

            var floating = expected instanceof Double || expected instanceof Float || actual instanceof Double || actual instanceof Float;
            var e = ((Number) expected).doubleValue();
            var a = ((Number) actual).doubleValue();

            var match = floating
                    ? Math.abs(e - a) <= 1e-9 * Math.max(1.0, Math.abs(e))
                    : ((Number) expected).longValue() == ((Number) actual).longValue();

            return match ? null : String.format("%s: expected %s, got %s", path, expected, actual);
        }

        if (expected instanceof Map && actual instanceof Map) {

            var expectedMap = (Map<?, ?>) expected;
            var actualMap = (Map<?, ?>) actual;

            if (!expectedMap.keySet().equals(actualMap.keySet()))
                return String.format("%s: expected keys %s, got %s", path, expectedMap.keySet(), actualMap.keySet());

            for (var key : expectedMap.keySet()) {
                var mismatch = contentMismatch(path + "[" + key + "]", expectedMap.get(key), actualMap.get(key));
                if (mismatch != null)
                    return mismatch;
            }

            return null;
        }

        if (expected instanceof List && actual instanceof List) {

            var expectedList = (List<?>) expected;
            var actualList = (List<?>) actual;

            if (expectedList.size() != actualList.size())
                return String.format("%s: expected %s, got %s", path, expected, actual);

            for (var i = 0; i < expectedList.size(); i++) {
                var mismatch = contentMismatch(path + "[" + i + "]", expectedList.get(i), actualList.get(i));
                if (mismatch != null)
                    return mismatch;
            }

            return null;
        }

        if (expected instanceof ByteBuffer && actual instanceof ByteBuffer)
            return expected.equals(actual) ? null : String.format("%s: expected [%s], got [%s]", path,
                    StandardCharsets.UTF_8.decode(((ByteBuffer) expected).duplicate()),
                    StandardCharsets.UTF_8.decode(((ByteBuffer) actual).duplicate()));

        return expected.equals(actual) ? null : String.format("%s: expected %s, got %s", path, expected, actual);
    }

    private static Object csvValue(String value, BasicType fieldType) {

        if (value == null || value.isEmpty())
            return null;

        switch (fieldType) {
            case FLOAT: return Double.parseDouble(value);
            case INTEGER: return Long.parseLong(value);
            case BOOLEAN: return Boolean.parseBoolean(value);
            default: return value;
        }
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
