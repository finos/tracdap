/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.orch.api;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.util.ResourceHelpers;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.test.data.SampleData;
import org.finos.tracdap.test.helpers.PlatformTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;
import java.util.UUID;


public class JobValidationTest {

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TEST_TENANT = "ACME_CORP";

    private static final byte[] BASIC_CSV_CONTENT = ResourceHelpers.loadResourceAsBytes(SampleData.BASIC_CSV_DATA_RESOURCE);
    private static final byte[] BASIC_CSV_CONTENT_V2 = ResourceHelpers.loadResourceAsBytes(SampleData.BASIC_CSV_DATA_RESOURCE_V2);
    private static final byte[] ALT_CSV_CONTENT = ResourceHelpers.loadResourceAsBytes(SampleData.ALT_CSV_DATA_RESOURCE);

    protected static TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub metaClient;
    protected static TracDataApiGrpc.TracDataApiBlockingStub dataClient;
    protected static TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClient;

    protected static TagSelector basicDataSelector;
    protected static TagSelector enrichedDataSelector;
    protected static TagSelector altDataSelector;

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
            .runDbDeploy(true)
            .addTenant(TEST_TENANT)
            .startMeta()
            .startData()
            .startOrch()
            .build();

    @BeforeAll
    static void setupClass() {

        metaClient = platform.metaClientTrustedBlocking();
        dataClient = platform.dataClientBlocking();
        orchClient = platform.orchClientBlocking();

        // Prepare some datasets that will be used during the tests

        basicDataSelector = loadCsvData(
                SampleData.BASIC_TABLE_SCHEMA,
                BASIC_CSV_CONTENT,
                List.of(TagUpdate.newBuilder()
                        .setAttrName("data_key")
                        .setValue(MetadataCodec.encodeValue("basic_data"))
                        .build()));

        enrichedDataSelector = loadCsvData(
                SampleData.BASIC_TABLE_SCHEMA_V2,
                BASIC_CSV_CONTENT_V2,
                List.of(TagUpdate.newBuilder()
                        .setAttrName("data_key")
                        .setValue(MetadataCodec.encodeValue("enriched_data"))
                        .build()));

        altDataSelector = loadCsvData(
                SampleData.ALT_TABLE_SCHEMA,
                ALT_CSV_CONTENT,
                List.of(TagUpdate.newBuilder()
                        .setAttrName("data_key")
                        .setValue(MetadataCodec.encodeValue("alt_data"))
                        .build()));
    }

    static private TagSelector loadCsvData(SchemaDefinition schema, byte[] content, List<TagUpdate> tags) {

        var dataWriteRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(schema)
                .setFormat("text/csv")
                // Content is never modified, no need to copy bytes
                .setContent(UnsafeByteOperations.unsafeWrap(content))
                .addAllTagUpdates(tags)
                .build();

        var dataId = dataClient.createSmallDataset(dataWriteRequest);

        return MetadataUtil.selectorFor(dataId);
    }

    @Test
    public void importModel_validateOk() {

        var job = JobDefinition.newBuilder()
            .setJobType(JobType.IMPORT_MODEL)
            .setImportModel(ImportModelJob.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setVersion("v1.0.0")
                .setPath("src/")
                .setEntryPoint("acme.models.test_model.BasicTestModel"));

        var request = JobRequest.newBuilder()
            .setTenant(TEST_TENANT)
            .setJob(job)
            .build();

        var response = orchClient.validateJob(request);

        Assertions.assertEquals(JobStatusCode.VALIDATED, response.getStatusCode());
    }

    @Test
    public void importModel_badInput() {

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_MODEL)
                .setImportModel(ImportModelJob.newBuilder()
                        .clearLanguage()  // Language cannot be null
                        .setRepository("UNIT_TEST_REPO")
                        .setVersion("v1.0.0")
                        .setPath("src/")
                        .setEntryPoint("acme.###.test_model.BasicTestModel"));  // Invalid entry point

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    public void importModel_missingResources() {

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_MODEL)
                .setImportModel(ImportModelJob.newBuilder()
                        .setLanguage("python")
                        .setRepository("REPO_THAT_IS_NOT_CONFIGURED")  // Repo key is not a known resource
                        .setVersion("v1.0.0")
                        .setPath("src/")
                        .setEntryPoint("acme.models.test_model.BasicTestModel"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    private TagSelector createBasicModel(SchemaDefinition inputSchema, SchemaDefinition outputSchema, List<TagUpdate> tags) {

        var modelDef = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setVersion("v1.0.0")
                .setPath("src/")
                .setEntryPoint("acme.models.test_model.BasicTestModel")
                .putParameters("param_1", ModelParameter.newBuilder()
                        .setParamType(TypeSystem.descriptor(BasicType.FLOAT))
                        .build())
                .putInputs("basic_input", ModelInputSchema.newBuilder()
                        .setSchema(inputSchema)
                        .build())
                .putOutputs("enriched_output", ModelOutputSchema.newBuilder()
                        .setSchema(outputSchema)
                        .build())
                .build();

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.MODEL)
                        .setModel(modelDef))
                .addAllTagUpdates(tags)
                .build();

        var modelId = metaClient.createObject(writeRequest);

        return MetadataUtil.selectorFor(modelId);
    }

    @Test
    public void runModel_validateOk() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("basc_test_model"))
                .build());

        var modelSelector = createBasicModel(
                SampleData.BASIC_TABLE_SCHEMA,
                SampleData.BASIC_TABLE_SCHEMA_V2,
                modelTags);

        var job = JobDefinition.newBuilder()
            .setJobType(JobType.RUN_MODEL)
            .setRunModel(RunModelJob.newBuilder()
                .setModel(modelSelector)
                .putParameters("param_1", Value.newBuilder()
                        .setFloatValue(11.0)
                        .build())
                .putInputs("basic_input", basicDataSelector)
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_model_ok"))))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_model_ok")))
                .build();

        var jobStatus = orchClient.validateJob(jobRequest);

        Assertions.assertEquals(JobStatusCode.VALIDATED, jobStatus.getStatusCode());
    }

    @Test
    public void runModel_priorOutputsOk() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("basc_test_model"))
                .build());

        var modelSelector = createBasicModel(
                SampleData.BASIC_TABLE_SCHEMA,
                SampleData.BASIC_TABLE_SCHEMA_V2,
                modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setRunModel(RunModelJob.newBuilder()
                .setModel(modelSelector)
                .putParameters("param_1", Value.newBuilder()
                        .setFloatValue(11.0)
                        .build())
                .putInputs("basic_input", basicDataSelector)
                .putPriorOutputs("enriched_output", enrichedDataSelector)
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_model_ok"))))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_model_ok")))
                .build();

        var jobStatus = orchClient.validateJob(jobRequest);

        Assertions.assertEquals(JobStatusCode.VALIDATED, jobStatus.getStatusCode());
    }

    @Test
    public void runModel_badInput() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("basic_model_bad_input"))
                .build());

        var modelSelector = createBasicModel(
                SampleData.BASIC_TABLE_SCHEMA,
                SampleData.BASIC_TABLE_SCHEMA_V2,
                modelTags);

        var badDataSelector = basicDataSelector.toBuilder()
                .setObjectVersion(-1)
                .build();

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setRunModel(RunModelJob.newBuilder()
                .setModel(modelSelector)
                .putParameters("param_1", Value.newBuilder()
                        .setFloatValue(11.0)
                        .build())
                .putInputs("basic_input", badDataSelector)
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_model_bad_input"))))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_model_bad_input")))
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(jobRequest));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    public void runModel_missingResources() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("basc_test_model"))
                .build());

        var modelSelector = createBasicModel(
                SampleData.BASIC_TABLE_SCHEMA,
                SampleData.BASIC_TABLE_SCHEMA_V2,
                modelTags);

        var missingDataSelector = basicDataSelector.toBuilder()
                .setObjectId(UUID.randomUUID().toString())
                .build();

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setRunModel(RunModelJob.newBuilder()
                .setModel(modelSelector)
                .putParameters("param_1", Value.newBuilder()
                        .setFloatValue(11.0)
                        .build())
                .putInputs("basic_input", missingDataSelector)
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_model_ok"))))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_model_ok")))
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(jobRequest));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void runModel_inconsistent() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("basc_test_model"))
                .build());

        var modelSelector = createBasicModel(
                SampleData.BASIC_TABLE_SCHEMA_V2,  // Expect enriched data as input
                SampleData.BASIC_TABLE_SCHEMA,
                modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setRunModel(RunModelJob.newBuilder()
                .setModel(modelSelector)
                .putParameters("param_1", Value.newBuilder()
                        .setStringValue("Not a real number")  // Supply wrong type for param_1
                        .build())
                .putInputs("basic_input", basicDataSelector)  // Expects enriched data, basic data is missing one field
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_model_ok"))))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_model_ok")))
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(jobRequest));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    private TagSelector createFlowModel(
            String entryPoint,
            Map<String, BasicType> paramTypes,
            Map<String, SchemaDefinition> inputSchemas,
            Map<String, SchemaDefinition> outputSchemas,
            List<TagUpdate> tags) {

        var modelDef = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setVersion("v1.0.0")
                .setPath("src/")
                .setEntryPoint(entryPoint);

        for (var param : paramTypes.entrySet()) {
            modelDef.putParameters(param.getKey(), ModelParameter.newBuilder()
                    .setParamType(TypeSystem.descriptor(param.getValue()))
                    .build());
        }

        for (var input : inputSchemas.entrySet()) {
            modelDef.putInputs(input.getKey(), ModelInputSchema.newBuilder()
                    .setSchema(input.getValue())
                    .build());
        }

        for (var output : outputSchemas.entrySet()) {
            modelDef.putOutputs(output.getKey(), ModelOutputSchema.newBuilder()
                    .setSchema(output.getValue())
                    .build());
        }

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.MODEL)
                        .setModel(modelDef))
                .addAllTagUpdates(tags)
                .build();

        var modelId = metaClient.createObject(writeRequest);

        return MetadataUtil.selectorFor(modelId);
    }

    @Test
    public void runFlow_validateOk() {

        var createFlowRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.FLOW)
                        .setFlow(SampleData.SAMPLE_FLOW))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var flowId = metaClient.createObject(createFlowRequest);
        var flowSelector = MetadataUtil.selectorFor(flowId);

        var model1 = createFlowModel(
                "acme.models.test_model.Model1",
                Map.of("param_1", BasicType.FLOAT),
                Map.of("basic_data_input", SampleData.BASIC_TABLE_SCHEMA),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2),
                List.of());

        var model2 = createFlowModel(
                "acme.models.test_model.Model2",
                Map.of("param_2", BasicType.STRING),
                Map.of("alt_data_input", SampleData.ALT_TABLE_SCHEMA),
                Map.of("enriched_alt_data", SampleData.ALT_TABLE_SCHEMA),
                List.of());

        var model3 = createFlowModel(
                "acme.models.test_model.Model2",
                Map.of("param_1", BasicType.FLOAT, "param_2", BasicType.STRING),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2, "enriched_alt_data", SampleData.ALT_TABLE_SCHEMA),
                Map.of("sample_output_data", SampleData.BASIC_TABLE_SCHEMA_V2),
                List.of());

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_FLOW)
                .setRunFlow(RunFlowJob.newBuilder()
                .setFlow(flowSelector)
                .putModels("model_1", model1)
                .putModels("model_2", model2)
                .putModels("model_3", model3)
                .putParameters("param_1", MetadataCodec.encodeValue(11.0))
                .putParameters("param_2", MetadataCodec.encodeValue("test_value"))
                .putInputs("basic_data_input", basicDataSelector)
                .putInputs("alt_data_input", altDataSelector)
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok"))))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var jobStatus = orchClient.validateJob(jobRequest);

        Assertions.assertEquals(JobStatusCode.VALIDATED, jobStatus.getStatusCode());
    }

    @Test
    public void runFlow_validatePriorOutputsOk() {

        var createFlowRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.FLOW)
                        .setFlow(SampleData.SAMPLE_FLOW))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var flowId = metaClient.createObject(createFlowRequest);
        var flowSelector = MetadataUtil.selectorFor(flowId);

        var model1 = createFlowModel(
                "acme.models.test_model.Model1",
                Map.of("param_1", BasicType.FLOAT),
                Map.of("basic_data_input", SampleData.BASIC_TABLE_SCHEMA),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2),
                List.of());

        var model2 = createFlowModel(
                "acme.models.test_model.Model2",
                Map.of("param_2", BasicType.STRING),
                Map.of("alt_data_input", SampleData.ALT_TABLE_SCHEMA),
                Map.of("enriched_alt_data", SampleData.ALT_TABLE_SCHEMA),
                List.of());

        var model3 = createFlowModel(
                "acme.models.test_model.Model2",
                Map.of("param_1", BasicType.FLOAT, "param_2", BasicType.STRING),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2, "enriched_alt_data", SampleData.ALT_TABLE_SCHEMA),
                Map.of("sample_output_data", SampleData.BASIC_TABLE_SCHEMA_V2),
                List.of());

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_FLOW)
                .setRunFlow(RunFlowJob.newBuilder()
                .setFlow(flowSelector)
                .putModels("model_1", model1)
                .putModels("model_2", model2)
                .putModels("model_3", model3)
                .putParameters("param_1", MetadataCodec.encodeValue(11.0))
                .putParameters("param_2", MetadataCodec.encodeValue("test_value"))
                .putInputs("basic_data_input", basicDataSelector)
                .putInputs("alt_data_input", altDataSelector)
                .putPriorOutputs("sample_output_data", enrichedDataSelector)
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok"))))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var jobStatus = orchClient.validateJob(jobRequest);

        Assertions.assertEquals(JobStatusCode.VALIDATED, jobStatus.getStatusCode());
    }

    @Test
    public void runFlow_badInput() {

        var createFlowRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.FLOW)
                        .setFlow(SampleData.SAMPLE_FLOW))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var flowId = metaClient.createObject(createFlowRequest);
        var flowSelector = MetadataUtil.selectorFor(flowId);

        var model1 = createFlowModel(
                "acme.models.test_model.Model1",
                Map.of("param_1", BasicType.FLOAT),
                Map.of("basic_data_input", SampleData.BASIC_TABLE_SCHEMA),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2),
                List.of());

        // Negative object versions for models 2 and 3 should create a bad request validation failure
        var model2 = model1.toBuilder().setObjectVersion(-1).build();
        var model3 = model1.toBuilder().setObjectVersion(-2).build();

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_FLOW)
                .setRunFlow(RunFlowJob.newBuilder()
                        .setFlow(flowSelector)
                        .putModels("model_1", model1)
                        .putModels("model_2", model2)
                        .putModels("model_3", model3)
                        .putParameters("param_1", MetadataCodec.encodeValue(11.0))
                        .putParameters("param_2", MetadataCodec.encodeValue("test_value"))
                        .putInputs("basic_data_input", basicDataSelector)
                        .putInputs("alt_data_input", altDataSelector)
                        .addOutputAttrs(TagUpdate.newBuilder()
                                .setAttrName("testing_key")
                                .setValue(MetadataCodec.encodeValue("test_flow_ok"))))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(jobRequest));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    public void runFlow_missingResources() {

        var createFlowRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.FLOW)
                        .setFlow(SampleData.SAMPLE_FLOW))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var flowId = metaClient.createObject(createFlowRequest);
        var flowSelector = MetadataUtil.selectorFor(flowId);

        var model1 = createFlowModel(
                "acme.models.test_model.Model1",
                Map.of("param_1", BasicType.FLOAT),
                Map.of("basic_data_input", SampleData.BASIC_TABLE_SCHEMA),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2),
                List.of());

        // Select random (missing) object IDs for two models and one input dataset
        var model2 = model1.toBuilder().setObjectId(UUID.randomUUID().toString()).build();
        var model3 = model1.toBuilder().setObjectId(UUID.randomUUID().toString()).build();
        var basicData = basicDataSelector.toBuilder().setObjectId(UUID.randomUUID().toString()).build();

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_FLOW)
                .setRunFlow(RunFlowJob.newBuilder()
                        .setFlow(flowSelector)
                        .putModels("model_1", model1)
                        .putModels("model_2", model2)
                        .putModels("model_3", model3)
                        .putParameters("param_1", MetadataCodec.encodeValue(11.0))
                        .putParameters("param_2", MetadataCodec.encodeValue("test_value"))
                        .putInputs("basic_data_input", basicData)
                        .putInputs("alt_data_input", altDataSelector)
                        .addOutputAttrs(TagUpdate.newBuilder()
                                .setAttrName("testing_key")
                                .setValue(MetadataCodec.encodeValue("test_flow_ok"))))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(jobRequest));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void runFlow_inconsistent1() {

        // This test provides the wrong schemas and parameter types into the models
        // But the flow wiring is correct

        var createFlowRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.FLOW)
                        .setFlow(SampleData.SAMPLE_FLOW))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var flowId = metaClient.createObject(createFlowRequest);
        var flowSelector = MetadataUtil.selectorFor(flowId);

        var model1 = createFlowModel(
                "acme.models.test_model.Model1",
                Map.of("param_1", BasicType.FLOAT),
                Map.of("basic_data_input", SampleData.BASIC_TABLE_SCHEMA_V2),  // Expect enriched data, basic data will have a missing field
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA),  // Output basic data, will have a missing field for model 3
                List.of());

        var model2 = createFlowModel(
                "acme.models.test_model.Model2",
                Map.of("param_2", BasicType.STRING),
                Map.of("alt_data_input", SampleData.BASIC_TABLE_SCHEMA),  // Expect basic data instead of alt data, wrong schema
                Map.of("enriched_alt_data", SampleData.ALT_TABLE_SCHEMA),
                List.of());

        var model3 = createFlowModel(
                "acme.models.test_model.Model2",
                Map.of("param_1", BasicType.FLOAT, "param_2", BasicType.STRING),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2, "enriched_alt_data", SampleData.ALT_TABLE_SCHEMA),
                Map.of("sample_output_data", SampleData.BASIC_TABLE_SCHEMA_V2),
                List.of());

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_FLOW)
                .setRunFlow(RunFlowJob.newBuilder()
                        .setFlow(flowSelector)
                        .putModels("model_1", model1)
                        .putModels("model_2", model2)
                        .putModels("model_3", model3)
                        // Use the wrong value types for supplied parameters
                        .putParameters("param_1", MetadataCodec.encodeValue("test_value"))
                        .putParameters("param_2", MetadataCodec.encodeValue(11.0))
                        .putInputs("basic_data_input", basicDataSelector)
                        .putInputs("alt_data_input", altDataSelector)
                        .addOutputAttrs(TagUpdate.newBuilder()
                                .setAttrName("testing_key")
                                .setValue(MetadataCodec.encodeValue("test_flow_ok"))))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(jobRequest));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void runFlow_inconsistent2() {

        // This test has broken wiring for the flow

        var createFlowRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.FLOW)
                        .setFlow(SampleData.SAMPLE_FLOW))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var flowId = metaClient.createObject(createFlowRequest);
        var flowSelector = MetadataUtil.selectorFor(flowId);

        var model1 = createFlowModel(
                "acme.models.test_model.Model1",
                Map.of("param_1", BasicType.FLOAT),
                Map.of("basic_data_input", SampleData.BASIC_TABLE_SCHEMA),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2),
                List.of());

        var model2 = createFlowModel(
                "acme.models.test_model.Model2",
                Map.of("param_2", BasicType.STRING),
                Map.of("alt_data_input", SampleData.ALT_TABLE_SCHEMA),
                Map.of("wrong_output_1", SampleData.ALT_TABLE_SCHEMA),  // Wrong output to wire into model 3
                List.of());

        var model3 = createFlowModel(
                "acme.models.test_model.Model2",
                Map.of("param_1", BasicType.FLOAT, "param_2", BasicType.STRING),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2, "enriched_alt_data", SampleData.ALT_TABLE_SCHEMA),
                Map.of("wrong_output_2", SampleData.BASIC_TABLE_SCHEMA_V2),  // Wrong output to wire into flow outputs
                List.of());

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_FLOW)
                .setRunFlow(RunFlowJob.newBuilder()
                        .setFlow(flowSelector)
                        .putModels("model_1", model1)
                        .putModels("model_2", model2)
                        .putModels("model_3", model3)
                        .putParameters("param_1", MetadataCodec.encodeValue(11.0))
                        .putParameters("param_3", MetadataCodec.encodeValue("test_value"))  // Wrong param name, param_2 is missing
                        .putInputs("basic_data_input", basicDataSelector)
                        .putInputs("alt_data_input", altDataSelector)
                        .addOutputAttrs(TagUpdate.newBuilder()
                                .setAttrName("testing_key")
                                .setValue(MetadataCodec.encodeValue("test_flow_ok"))))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("testing_key")
                        .setValue(MetadataCodec.encodeValue("test_flow_ok")))
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(jobRequest));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }
}
