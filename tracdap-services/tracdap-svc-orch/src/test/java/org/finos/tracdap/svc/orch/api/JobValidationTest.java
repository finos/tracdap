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

package org.finos.tracdap.svc.orch.api;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.UuidFactory;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.util.ResourceHelpers;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.data.SampleData;
import org.finos.tracdap.test.helpers.PlatformTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class JobValidationTest {

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TRAC_TENANTS_UNIT = "config/trac-unit-tenants.yaml";
    public static final String TEST_TENANT = "ACME_CORP";

    private static final byte[] BASIC_CSV_CONTENT = ResourceHelpers.loadResourceAsBytes(SampleData.BASIC_CSV_DATA_RESOURCE);
    private static final byte[] BASIC_CSV_CONTENT_V2 = ResourceHelpers.loadResourceAsBytes(SampleData.BASIC_CSV_DATA_RESOURCE_V2);
    private static final byte[] ALT_CSV_CONTENT = ResourceHelpers.loadResourceAsBytes(SampleData.ALT_CSV_DATA_RESOURCE);

    protected static InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient;
    protected static TracDataApiGrpc.TracDataApiBlockingStub dataClient;
    protected static TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClient;

    protected static TagSelector basicDataSelector;
    protected static TagSelector enrichedDataSelector;
    protected static TagSelector altDataSelector;

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT, List.of(TRAC_TENANTS_UNIT))
            .runDbDeploy(true)
            .addTenant(TEST_TENANT)
            .startService(TracAdminService.class)
            .startService(TracMetadataService.class)
            .startService(TracDataService.class)
            .startService(TracOrchestratorService.class)
            .build();

    @BeforeAll
    static void setupClass() {

        metaClient = platform.metaClientInternalBlocking();
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
    public void importModel_priorModelOk() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_model_prior_model_ok"))
                .build());

        var priorModelSelector = createBasicModel(
                SampleData.BASIC_TABLE_SCHEMA,
                SampleData.BASIC_TABLE_SCHEMA_V2,
                modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_MODEL)
                .setImportModel(ImportModelJob.newBuilder()
                        .setLanguage("python")
                        .setRepository("UNIT_TEST_REPO")
                        .setVersion("v2.0.0")
                        .setPath("src/")
                        .setEntryPoint("acme.models.test_model.BasicTestModel")
                        .setPriorModel(priorModelSelector));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var response = orchClient.validateJob(request);

        Assertions.assertEquals(JobStatusCode.VALIDATED, response.getStatusCode());
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
                        .setObjectType(ObjectType.DATA)
                        .setSchema(inputSchema)
                        .build())
                .putOutputs("enriched_output", ModelOutputSchema.newBuilder()
                        .setObjectType(ObjectType.DATA)
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
                .setObjectId(UuidFactory.DEFAULT.allocate().toString())
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
                    .setObjectType(ObjectType.DATA)
                    .setSchema(input.getValue())
                    .build());
        }

        for (var output : outputSchemas.entrySet()) {
            modelDef.putOutputs(output.getKey(), ModelOutputSchema.newBuilder()
                    .setObjectType(ObjectType.DATA)
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
        var model2 = model1.toBuilder().setObjectId(UuidFactory.DEFAULT.allocate().toString()).build();
        var model3 = model1.toBuilder().setObjectId(UuidFactory.DEFAULT.allocate().toString()).build();
        var basicData = basicDataSelector.toBuilder().setObjectId(UuidFactory.DEFAULT.allocate().toString()).build();

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

        // This test has missing parameters and parameters with the wrong type
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
                Map.of("basic_data_input", SampleData.BASIC_TABLE_SCHEMA),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2),
                List.of());

        var model2 = createFlowModel(
                "acme.models.test_model.Model2",
                Map.of("param_2", BasicType.STRING),
                Map.of("alt_data_input", SampleData.ALT_TABLE_SCHEMA),
                Map.of("enriched_alt_data", SampleData.ALT_TABLE_SCHEMA_V2),
                List.of());

        var model3 = createFlowModel(
                "acme.models.test_model.Model2",
                Map.of("param_1", BasicType.INTEGER, "param_2", BasicType.STRING),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2, "enriched_alt_data", SampleData.ALT_TABLE_SCHEMA_V2),
                Map.of("sample_output_data", SampleData.BASIC_TABLE_SCHEMA_V2),
                List.of());

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_FLOW)
                .setRunFlow(RunFlowJob.newBuilder()
                        .setFlow(flowSelector)
                        .putModels("model_1", model1)
                        .putModels("model_2", model2)
                        .putModels("model_3", model3)
                        // Use the wrong value type for param_1
                        .putParameters("param_1", MetadataCodec.encodeValue("test_value"))
                        // Param 2 missing, unexpected param_3
                        .putParameters("param_3", MetadataCodec.encodeValue(11.0))
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

        // This test provides the wrong schemas for some of the datasets
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
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void runFlow_inconsistent3() {

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
                Map.of("sample_output_data", SampleData.ALT_TABLE_SCHEMA),
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

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(jobRequest));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    private TagSelector createDataImportModel(List<TagUpdate> tags) {
        return createDataImportModel(tags, Map.of(), Map.of());
    }

    private TagSelector createDataImportModel(
            List<TagUpdate> tags,
            Map<String, BasicType> paramTypes,
            Map<String, SchemaDefinition> inputSchemas) {

        var modelDef = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setVersion("v1.0.0")
                .setPath("src/")
                .setEntryPoint("acme.models.test_model.BasicDataImportModel")
                .setModelType(ModelType.DATA_IMPORT_MODEL)
                .putOutputs("output_data", ModelOutputSchema.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setSchema(SampleData.BASIC_TABLE_SCHEMA)
                        .build());

        for (var param : paramTypes.entrySet()) {
            modelDef.putParameters(param.getKey(), ModelParameter.newBuilder()
                    .setParamType(TypeSystem.descriptor(param.getValue()))
                    .build());
        }

        for (var input : inputSchemas.entrySet()) {
            modelDef.putInputs(input.getKey(), ModelInputSchema.newBuilder()
                    .setObjectType(ObjectType.DATA)
                    .setSchema(input.getValue())
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

    private TagSelector createDataExportModel(List<TagUpdate> tags) {
        return createDataExportModel(tags, Map.of());
    }

    private TagSelector createDataExportModel(List<TagUpdate> tags, Map<String, BasicType> paramTypes) {

        var modelDef = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setVersion("v1.0.0")
                .setPath("src/")
                .setEntryPoint("acme.models.test_model.BasicDataExportModel")
                .setModelType(ModelType.DATA_EXPORT_MODEL)
                .putInputs("input_data", ModelInputSchema.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setSchema(SampleData.BASIC_TABLE_SCHEMA)
                        .build());

        for (var param : paramTypes.entrySet()) {
            modelDef.putParameters(param.getKey(), ModelParameter.newBuilder()
                    .setParamType(TypeSystem.descriptor(param.getValue()))
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
    public void importData_validateOk() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_data_validate_ok"))
                .build());

        var modelSelector = createDataImportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var response = orchClient.validateJob(request);

        Assertions.assertEquals(JobStatusCode.VALIDATED, response.getStatusCode());
    }

    @Test
    public void importData_badInput() {

        // Model is required and is missing entirely

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    public void importData_outputsMustBeEmpty() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_data_outputs_must_be_empty"))
                .build());

        var modelSelector = createDataImportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE")
                        .putOutputs("output_data", basicDataSelector));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    public void importData_wrongResourceType() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_data_wrong_resource_type"))
                .build());

        var modelSelector = createDataImportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .addStorageAccess("UNIT_TEST_STORAGE"));  // INTERNAL_STORAGE, not EXTERNAL_STORAGE

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void importData_wrongModelType() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_data_wrong_model_type"))
                .build());

        // A standard model, not a data import model
        var modelSelector = createBasicModel(
                SampleData.BASIC_TABLE_SCHEMA,
                SampleData.BASIC_TABLE_SCHEMA_V2,
                modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void importData_missingResources() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_data_missing_resources"))
                .build());

        var modelSelector = createDataImportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .addStorageAccess("STORAGE_THAT_IS_NOT_CONFIGURED"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void importData_missingParameter() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_data_missing_parameter"))
                .build());

        var modelSelector = createDataImportModel(modelTags, Map.of("storage_key", BasicType.STRING), Map.of());

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void importData_wrongParameterType() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_data_wrong_parameter_type"))
                .build());

        var modelSelector = createDataImportModel(modelTags, Map.of("storage_key", BasicType.STRING), Map.of());

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putParameters("storage_key", MetadataCodec.encodeValue(123))  // Wrong type, model expects a string
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void importData_wrongInputSchema() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_data_wrong_input_schema"))
                .build());

        var modelSelector = createDataImportModel(
                modelTags, Map.of(), Map.of("reference_data", SampleData.BASIC_TABLE_SCHEMA));

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putInputs("reference_data", altDataSelector)  // Wrong schema, model expects basic data
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void exportData_validateOk() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("export_data_validate_ok"))
                .build());

        var modelSelector = createDataExportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.EXPORT_DATA)
                .setExportData(ExportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putInputs("input_data", basicDataSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var response = orchClient.validateJob(request);

        Assertions.assertEquals(JobStatusCode.VALIDATED, response.getStatusCode());
    }

    @Test
    public void exportData_badInput() {

        // Model is required and is missing entirely

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.EXPORT_DATA)
                .setExportData(ExportDataJob.newBuilder()
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    public void exportData_outputsMustBeEmpty() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("export_data_outputs_must_be_empty"))
                .build());

        var modelSelector = createDataExportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.EXPORT_DATA)
                .setExportData(ExportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putInputs("input_data", basicDataSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE")
                        .putOutputs("unexpected_output", basicDataSelector));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    public void exportData_wrongResourceType() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("export_data_wrong_resource_type"))
                .build());

        var modelSelector = createDataExportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.EXPORT_DATA)
                .setExportData(ExportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putInputs("input_data", basicDataSelector)
                        .addStorageAccess("UNIT_TEST_REPO"));  // MODEL_REPOSITORY, not EXTERNAL_STORAGE

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void exportData_wrongModelType() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("export_data_wrong_model_type"))
                .build());

        // A data import model, not a data export model
        var modelSelector = createDataImportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.EXPORT_DATA)
                .setExportData(ExportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putInputs("input_data", basicDataSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void exportData_missingResources() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("export_data_missing_resources"))
                .build());

        var modelSelector = createDataExportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.EXPORT_DATA)
                .setExportData(ExportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putInputs("input_data", basicDataSelector)
                        .addStorageAccess("STORAGE_THAT_IS_NOT_CONFIGURED"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void exportData_missingParameter() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("export_data_missing_parameter"))
                .build());

        var modelSelector = createDataExportModel(modelTags, Map.of("storage_key", BasicType.STRING));

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.EXPORT_DATA)
                .setExportData(ExportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putInputs("input_data", basicDataSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void exportData_wrongParameterType() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("export_data_wrong_parameter_type"))
                .build());

        var modelSelector = createDataExportModel(modelTags, Map.of("storage_key", BasicType.STRING));

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.EXPORT_DATA)
                .setExportData(ExportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putInputs("input_data", basicDataSelector)
                        .putParameters("storage_key", MetadataCodec.encodeValue(123))  // Wrong type, model expects a string
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void exportData_wrongInputSchema() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("export_data_wrong_input_schema"))
                .build());

        var modelSelector = createDataExportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.EXPORT_DATA)
                .setExportData(ExportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putInputs("input_data", altDataSelector)  // Wrong schema, model expects basic data
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE"));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    public void importData_reservedOutputAttr() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_data_reserved_output_attr"))
                .build());

        var modelSelector = createDataImportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE")
                        .addOutputAttrs(TagUpdate.newBuilder()
                                .setAttrName("trac_import_location_key")
                                .setValue(MetadataCodec.encodeValue("spoofed_location"))));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    public void importData_importsMustBeEmpty() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_data_imports_must_be_empty"))
                .build());

        var modelSelector = createDataImportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE")
                        .putImports("import_one", basicDataSelector));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    public void importData_importAttrsMustBeEmpty() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("import_data_import_attrs_must_be_empty"))
                .build());

        var modelSelector = createDataImportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_DATA)
                .setImportData(ImportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE")
                        .addImportAttrs(TagUpdate.newBuilder()
                                .setAttrName("business_date")
                                .setValue(MetadataCodec.encodeValue("2024-01-01"))));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    public void exportData_reservedOutputAttr() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("export_data_reserved_output_attr"))
                .build());

        var modelSelector = createDataExportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.EXPORT_DATA)
                .setExportData(ExportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putInputs("input_data", basicDataSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE")
                        .addOutputAttrs(TagUpdate.newBuilder()
                                .setAttrName("trac_job_output")
                                .setValue(MetadataCodec.encodeValue("spoofed"))));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    public void exportData_exportsMustBeEmpty() {

        var modelTags = List.of(TagUpdate.newBuilder()
                .setAttrName("model_key")
                .setValue(MetadataCodec.encodeValue("export_data_exports_must_be_empty"))
                .build());

        var modelSelector = createDataExportModel(modelTags);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.EXPORT_DATA)
                .setExportData(ExportDataJob.newBuilder()
                        .setModel(modelSelector)
                        .putInputs("input_data", basicDataSelector)
                        .addStorageAccess("UNIT_TEST_EXTERNAL_STORAGE")
                        .putExports("export_one", basicDataSelector));

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(request));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    private static FlowEdge.Builder flowEdge(String sourceNode, String sourceSocket, String targetNode, String targetSocket) {

        var source = FlowSocket.newBuilder().setNode(sourceNode);
        var target = FlowSocket.newBuilder().setNode(targetNode);

        if (sourceSocket != null) source.setSocket(sourceSocket);
        if (targetSocket != null) target.setSocket(targetSocket);

        return FlowEdge.newBuilder().setSource(source).setTarget(target);
    }

    private TagSelector createExportFlow() {

        // Export model node is terminal, has no outputs and leaves dataset_2 unconnected

        var flow = FlowDefinition.newBuilder()
                .putNodes("basic_data_input", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("basic_data_input")
                        .addOutputs("enriched_basic_data")
                        .build())
                .putNodes("enriched_basic_data", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())
                .putNodes("export_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .setModelType(ModelType.DATA_EXPORT_MODEL)
                        .addInputs("dataset_1").addInputs("dataset_2")
                        .build())
                .addEdges(flowEdge("basic_data_input", null, "model_1", "basic_data_input"))
                .addEdges(flowEdge("model_1", "enriched_basic_data", "enriched_basic_data", null))
                .addEdges(flowEdge("model_1", "enriched_basic_data", "export_1", "dataset_1"));

        var createFlowRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.FLOW)
                        .setFlow(flow))
                .build();

        var flowId = metaClient.createObject(createFlowRequest);

        return MetadataUtil.selectorFor(flowId);
    }

    private TagSelector createFlowExportModel(boolean dataset2Optional) {

        var modelDef = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setVersion("v1.0.0")
                .setPath("src/")
                .setEntryPoint("acme.models.test_model.FlowDataExportModel")
                .setModelType(ModelType.DATA_EXPORT_MODEL)
                .putInputs("dataset_1", ModelInputSchema.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setSchema(SampleData.BASIC_TABLE_SCHEMA_V2)
                        .setOptional(true)
                        .build())
                .putInputs("dataset_2", ModelInputSchema.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setSchema(SampleData.BASIC_TABLE_SCHEMA_V2)
                        .setOptional(dataset2Optional)
                        .build());

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.MODEL)
                        .setModel(modelDef))
                .build();

        var modelId = metaClient.createObject(writeRequest);

        return MetadataUtil.selectorFor(modelId);
    }

    private RunFlowJob.Builder exportFlowJob() {

        var model1 = createFlowModel(
                "acme.models.test_model.Model1",
                Map.of("param_1", BasicType.FLOAT),
                Map.of("basic_data_input", SampleData.BASIC_TABLE_SCHEMA),
                Map.of("enriched_basic_data", SampleData.BASIC_TABLE_SCHEMA_V2),
                List.of());

        return RunFlowJob.newBuilder()
                .setFlow(createExportFlow())
                .putModels("model_1", model1)
                .putModels("export_1", createFlowExportModel(true))
                .putParameters("param_1", MetadataCodec.encodeValue(11.0))
                .putInputs("basic_data_input", basicDataSelector)
                .addExportStorageAccess("UNIT_TEST_EXTERNAL_STORAGE");
    }

    private JobStatus validateRunFlow(RunFlowJob.Builder runFlow) {

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_FLOW)
                .setRunFlow(runFlow);

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        return orchClient.validateJob(request);
    }

    private static final io.grpc.Metadata.Key<TracErrorDetails> TRAC_ERROR_DETAILS_KEY = io.grpc.Metadata.Key.of(
            "trac-error-details-bin", ProtoUtils.metadataMarshaller(TracErrorDetails.getDefaultInstance()));

    private void expectRunFlowInvalid(RunFlowJob.Builder runFlow, Status.Code expectedCode, String expectedMessage) {

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> validateRunFlow(runFlow));
        Assertions.assertEquals(expectedCode, e.getStatus().getCode());

        var trailers = e.getTrailers();
        var errorDetails = trailers != null ? trailers.get(TRAC_ERROR_DETAILS_KEY) : null;

        var errorMessages = errorDetails != null
                ? errorDetails.getItemsList().stream().map(TracErrorItem::getDetail).collect(Collectors.toList())
                : new ArrayList<String>();

        errorMessages.add(e.getStatus().getDescription());

        Assertions.assertTrue(
                errorMessages.stream().anyMatch(msg -> msg != null && msg.contains(expectedMessage)),
                "Expected error [" + expectedMessage + "], got " + errorMessages);
    }

    @Test
    public void runFlow_exportNode_validateOk() {

        var jobStatus = validateRunFlow(exportFlowJob());

        Assertions.assertEquals(JobStatusCode.VALIDATED, jobStatus.getStatusCode());
    }

    @Test
    public void runFlow_exportNode_requiredInputUnconnected() {

        var runFlow = exportFlowJob()
                .putModels("export_1", createFlowExportModel(false));

        expectRunFlowInvalid(runFlow, Status.Code.FAILED_PRECONDITION, "Input [dataset_2] is not connected");
    }

    @Test
    public void runFlow_exportNode_modelTypeMismatch() {

        // An export model bound to a standard model node, with the same parameters, inputs and outputs as the node

        var modelDef = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setVersion("v1.0.0")
                .setPath("src/")
                .setEntryPoint("acme.models.test_model.Model1Export")
                .setModelType(ModelType.DATA_EXPORT_MODEL)
                .putParameters("param_1", ModelParameter.newBuilder()
                        .setParamType(TypeSystem.descriptor(BasicType.FLOAT))
                        .build())
                .putInputs("basic_data_input", ModelInputSchema.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setSchema(SampleData.BASIC_TABLE_SCHEMA)
                        .build())
                .putOutputs("enriched_basic_data", ModelOutputSchema.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setSchema(SampleData.BASIC_TABLE_SCHEMA_V2)
                        .build());

        var modelId = metaClient.createObject(MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.MODEL)
                        .setModel(modelDef))
                .build());

        var runFlow = exportFlowJob()
                .putModels("model_1", MetadataUtil.selectorFor(modelId));

        expectRunFlowInvalid(runFlow, Status.Code.FAILED_PRECONDITION, "Model type does not match the flow node");
    }

    @Test
    public void runFlow_exportNode_missingExportStorage() {

        var runFlow = exportFlowJob()
                .clearExportStorageAccess();

        expectRunFlowInvalid(runFlow, Status.Code.FAILED_PRECONDITION, "Export storage access is required");
    }

    @Test
    public void runFlow_exportNode_wrongStorageType() {

        var runFlow = exportFlowJob()
                .clearExportStorageAccess()
                .addExportStorageAccess("UNIT_TEST_STORAGE");  // INTERNAL_STORAGE, not EXTERNAL_STORAGE

        expectRunFlowInvalid(runFlow, Status.Code.FAILED_PRECONDITION, "wrong type");
    }

    @Test
    public void runFlow_exportNode_storageNotConfigured() {

        var runFlow = exportFlowJob()
                .clearExportStorageAccess()
                .addExportStorageAccess("STORAGE_THAT_IS_NOT_CONFIGURED");

        expectRunFlowInvalid(runFlow, Status.Code.FAILED_PRECONDITION, "STORAGE_THAT_IS_NOT_CONFIGURED");
    }

    @Test
    public void runFlow_exportStorage_invalidIdentifier() {

        var runFlow = exportFlowJob()
                .clearExportStorageAccess()
                .addExportStorageAccess("not a valid identifier");

        expectRunFlowInvalid(runFlow, Status.Code.INVALID_ARGUMENT, "identifier");
    }

    @Test
    public void runFlow_exportStorageWithoutExportNode() {

        var createFlowRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.FLOW)
                        .setFlow(SampleData.SAMPLE_FLOW))
                .build();

        var flowSelector = MetadataUtil.selectorFor(metaClient.createObject(createFlowRequest));

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

        var runFlow = RunFlowJob.newBuilder()
                .setFlow(flowSelector)
                .putModels("model_1", model1)
                .putModels("model_2", model2)
                .putModels("model_3", model3)
                .putParameters("param_1", MetadataCodec.encodeValue(11.0))
                .putParameters("param_2", MetadataCodec.encodeValue("test_value"))
                .putInputs("basic_data_input", basicDataSelector)
                .putInputs("alt_data_input", altDataSelector)
                .addExportStorageAccess("UNIT_TEST_EXTERNAL_STORAGE");

        expectRunFlowInvalid(runFlow, Status.Code.FAILED_PRECONDITION, "Export storage access is only allowed");
    }


    // -----------------------------------------------------------------------------------------------------------------
    // CAPTURE-ONLY IMPORT DATA JOBS
    // -----------------------------------------------------------------------------------------------------------------

    private static final String CAPTURE_STORAGE = "UNIT_TEST_EXTERNAL_STORAGE";

    private TagSelector createCaptureSchema() {

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.SCHEMA)
                        .setSchema(SampleData.BASIC_TABLE_SCHEMA))
                .build();

        return MetadataUtil.selectorFor(metaClient.createObject(writeRequest));
    }

    private CaptureSource.Builder declaredCapture(String storagePath) {

        return CaptureSource.newBuilder()
                .setLocation(ExternalLocation.newBuilder()
                        .setStorageKey(CAPTURE_STORAGE)
                        .setStoragePath(storagePath))
                .setSchemaSource(CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED)
                .setSchemaId(createCaptureSchema());
    }

    private CaptureSource.Builder fileSchemaCapture(String storagePath) {

        return CaptureSource.newBuilder()
                .setLocation(ExternalLocation.newBuilder()
                        .setStorageKey(CAPTURE_STORAGE)
                        .setStoragePath(storagePath))
                .setSchemaSource(CaptureSchemaSource.CAPTURE_SCHEMA_FILE);
    }

    private JobStatus validateCaptureJob(ImportDataJob.Builder importData) {

        var request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_DATA)
                        .setImportData(importData))
                .build();

        return orchClient.validateJob(request);
    }

    private void expectCaptureInvalid(ImportDataJob.Builder importData, Status.Code expectedCode, String expectedMessage) {

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> validateCaptureJob(importData));
        Assertions.assertEquals(expectedCode, e.getStatus().getCode());

        var trailers = e.getTrailers();
        var errorDetails = trailers != null ? trailers.get(TRAC_ERROR_DETAILS_KEY) : null;

        var errorMessages = errorDetails != null
                ? errorDetails.getItemsList().stream().map(TracErrorItem::getDetail).collect(Collectors.toList())
                : new ArrayList<String>();

        errorMessages.add(e.getStatus().getDescription());

        Assertions.assertTrue(
                errorMessages.stream().anyMatch(msg -> msg != null && msg.contains(expectedMessage)),
                "Expected error [" + expectedMessage + "], got " + errorMessages);
    }

    @Test
    public void captureData_validateOk() {

        var importData = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("2026-09/loans_2026-09-30.csv").build())
                .putCaptures("rates", declaredCapture("rates.PARQUET").build())
                .putCaptures("fx", fileSchemaCapture("fx.parquet").build())
                .putCaptures("prices", fileSchemaCapture("prices.arrow").build())
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("business_segments")
                        .setValue(MetadataCodec.encodeValue("retail")));

        var response = validateCaptureJob(importData);

        Assertions.assertEquals(JobStatusCode.VALIDATED, response.getStatusCode());
    }

    @Test
    public void captureData_modelWithCaptures() {

        var modelSelector = createDataImportModel(List.of());

        var importData = ImportDataJob.newBuilder()
                .setModel(modelSelector)
                .putCaptures("loans", declaredCapture("loans.csv").build());

        expectCaptureInvalid(importData, Status.Code.INVALID_ARGUMENT, "A model cannot be used with captures");
    }

    @Test
    public void captureData_badLocation() {

        var noLocation = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv").clearLocation().build());

        expectCaptureInvalid(noLocation, Status.Code.INVALID_ARGUMENT, "source");

        var badKey = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv")
                        .setLocation(ExternalLocation.newBuilder().setStorageKey("not a key").setStoragePath("loans.csv"))
                        .build());

        expectCaptureInvalid(badKey, Status.Code.INVALID_ARGUMENT, "storageKey");

        for (var path : List.of("../loans.csv", "data/../../loans.csv", "C:\\loans.csv")) {

            var badPath = ImportDataJob.newBuilder()
                    .putCaptures("loans", declaredCapture(path).build());

            expectCaptureInvalid(badPath, Status.Code.INVALID_ARGUMENT, "storagePath");
        }

        var badFileName = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("data/PRN.csv").build());

        expectCaptureInvalid(badFileName, Status.Code.INVALID_ARGUMENT, "reserved filename");
    }

    @Test
    public void captureData_badExtension() {

        for (var path : List.of("loans", "loans.txt", "loans.csv.zip", "data.csv/loans")) {

            var importData = ImportDataJob.newBuilder()
                    .putCaptures("loans", declaredCapture(path).build());

            expectCaptureInvalid(importData, Status.Code.INVALID_ARGUMENT, "is not a supported data file");
        }
    }

    @Test
    public void captureData_clientLocationDetails() {

        var protocol = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv")
                        .setLocation(ExternalLocation.newBuilder()
                                .setStorageKey(CAPTURE_STORAGE)
                                .setStoragePath("loans.csv")
                                .setProtocol("LOCAL"))
                        .build());

        expectCaptureInvalid(protocol, Status.Code.INVALID_ARGUMENT, "protocol of an external location is set by the platform");

        var locationDetails = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv")
                        .setLocation(ExternalLocation.newBuilder()
                                .setStorageKey(CAPTURE_STORAGE)
                                .setStoragePath("loans.csv")
                                .putLocationDetails("rootPath", "/tmp"))
                        .build());

        expectCaptureInvalid(locationDetails, Status.Code.INVALID_ARGUMENT, "location details of an external location are set by the platform");
    }

    @Test
    public void captureData_schemaSourceMismatch() {

        var declaredNoSchema = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv").clearSchemaId().build());

        expectCaptureInvalid(declaredNoSchema, Status.Code.INVALID_ARGUMENT, "schemaSpecifier");

        var inlineSchema = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv").setSchema(SampleData.BASIC_TABLE_SCHEMA).build());

        expectCaptureInvalid(inlineSchema, Status.Code.INVALID_ARGUMENT, "inline schema is not supported");

        var fileWithSchemaId = ImportDataJob.newBuilder()
                .putCaptures("fx", fileSchemaCapture("fx.parquet").setSchemaId(createCaptureSchema()).build());

        expectCaptureInvalid(fileWithSchemaId, Status.Code.INVALID_ARGUMENT, "cannot also declare a schema");

        var fileSchemaCsv = ImportDataJob.newBuilder()
                .putCaptures("loans", fileSchemaCapture("loans.csv").build());

        expectCaptureInvalid(fileSchemaCsv, Status.Code.INVALID_ARGUMENT, "needs a declared schema");

        var wrongSchemaType = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv").setSchemaId(basicDataSelector).build());

        expectCaptureInvalid(wrongSchemaType, Status.Code.INVALID_ARGUMENT, "schemaId");
    }

    @Test
    public void captureData_noSchemaSource() {

        var importData = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv")
                        .setSchemaSource(CaptureSchemaSource.CAPTURE_SCHEMA_SOURCE_NOT_SET)
                        .build());

        expectCaptureInvalid(importData, Status.Code.FAILED_PRECONDITION, "requires a schema source");
    }

    @Test
    public void captureData_schemaNotAvailable() {

        var missingSchema = MetadataUtil.selectorFor(TagHeader.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setObjectId(UuidFactory.DEFAULT.allocate().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build());

        var importData = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv").setSchemaId(missingSchema).build());

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> validateCaptureJob(importData));
        Assertions.assertNotEquals(Status.Code.OK, e.getStatus().getCode());
    }

    @Test
    public void captureData_storageNotExternal() {

        var internalStorage = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv")
                        .setLocation(ExternalLocation.newBuilder().setStorageKey("UNIT_TEST_STORAGE").setStoragePath("loans.csv"))
                        .build());

        expectCaptureInvalid(internalStorage, Status.Code.FAILED_PRECONDITION, "is the wrong type");

        var unknownStorage = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv")
                        .setLocation(ExternalLocation.newBuilder().setStorageKey("NOT_CONFIGURED").setStoragePath("loans.csv"))
                        .build());

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> validateCaptureJob(unknownStorage));
        Assertions.assertNotEquals(Status.Code.OK, e.getStatus().getCode());
    }

    @Test
    public void captureData_outputClash() {

        var importData = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv").build())
                .putCaptures("loans_file", declaredCapture("loans_2.csv").build());

        expectCaptureInvalid(importData, Status.Code.FAILED_PRECONDITION, "clashes with the file output");
    }

    @Test
    public void captureData_badNamesAndAttrs() {

        var badName = ImportDataJob.newBuilder()
                .putCaptures("trac_loans", declaredCapture("loans.csv").build());

        expectCaptureInvalid(badName, Status.Code.INVALID_ARGUMENT, "reserved identifier");

        var reservedDataAttr = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv")
                        .addDataAttrs(TagUpdate.newBuilder()
                                .setAttrName("trac_import_content_hash")
                                .setValue(MetadataCodec.encodeValue("forged")))
                        .build());

        expectCaptureInvalid(reservedDataAttr, Status.Code.INVALID_ARGUMENT, "trac_import_content_hash");

        var reservedFileAttr = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv")
                        .addFileAttrs(TagUpdate.newBuilder()
                                .setAttrName("trac_capture_file")
                                .setValue(MetadataCodec.encodeValue("forged")))
                        .build());

        expectCaptureInvalid(reservedFileAttr, Status.Code.INVALID_ARGUMENT, "trac_capture_file");
    }

    @Test
    public void captureData_fieldsMustBeEmpty() {

        var withParameters = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv").build())
                .putParameters("param", MetadataCodec.encodeValue(1));

        expectCaptureInvalid(withParameters, Status.Code.INVALID_ARGUMENT, "parameters field is not used with captures");

        var withInputs = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv").build())
                .putInputs("input", basicDataSelector);

        expectCaptureInvalid(withInputs, Status.Code.INVALID_ARGUMENT, "inputs field is not used with captures");

        var withOutputs = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv").build())
                .putOutputs("output", basicDataSelector);

        expectCaptureInvalid(withOutputs, Status.Code.INVALID_ARGUMENT, "Outputs must be empty");

        var withPriorOutputs = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv").build())
                .putPriorOutputs("loans", basicDataSelector);

        expectCaptureInvalid(withPriorOutputs, Status.Code.INVALID_ARGUMENT, "priorOutputs field is not used with captures");

        var withStorageAccess = ImportDataJob.newBuilder()
                .putCaptures("loans", declaredCapture("loans.csv").build())
                .addStorageAccess(CAPTURE_STORAGE);

        expectCaptureInvalid(withStorageAccess, Status.Code.INVALID_ARGUMENT, "storageAccess field is not used with captures");
    }
}
