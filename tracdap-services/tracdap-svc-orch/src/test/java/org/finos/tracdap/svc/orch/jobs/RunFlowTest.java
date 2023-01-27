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

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.metadata.ImportModelJob;
import org.finos.tracdap.metadata.RunFlowJob;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.finos.tracdap.svc.orch.jobs.Helpers.runJob;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RunFlowTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";

    private static final String LOANS_INPUT_PATH = "examples/models/python/data/inputs/loan_final313_100_shortform.csv";
    private static final String CURRENCY_INPUT_PATH = "examples/models/python/data/inputs/currency_data_sample.csv";
    public static final String ANALYSIS_TYPE = "analysis_type";

    // Only test E2E run model using the local repo
    // E2E model loading with different repo types is tested in ImportModelTest
    // We don't need to test all combinations of model run from different repo types

    protected String useTracRepo() { return "TRAC_LOCAL_REPO"; }


    @RegisterExtension
    private static final PlatformTest platform = PlatformTest.forConfig(E2E_CONFIG)
            .addTenant(TEST_TENANT)
            .startAll()
            .build();

    private final Logger log = LoggerFactory.getLogger(getClass());

    static TagHeader flowId;
    static TagHeader loansDataId;
    static TagHeader currencyDataId;
    static TagHeader model1Id;
    static TagHeader model2Id;
    static TagHeader outputDataId;

    @Test @Order(1)
    void createFlow() {

        var metaClient = platform.metaClientBlocking();

        var pi_value = Value.newBuilder()
                .setType(
                        TypeDescriptor.newBuilder()
                                .setBasicType(BasicType.FLOAT)
                                .build()
                )
                .setFloatValue(3.14)
                .build();

        var flowDef = FlowDefinition.newBuilder()
                .putNodes("customer_loans", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("currency_data", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder()
                        .setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("customer_loans")
                        .addInputs("currency_data")
                        .addOutputs("preprocessed_data")
                        .build())
                .putNodes("model_2", FlowNode.newBuilder()
                        .setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("preprocessed_data")
                        .addOutputs("profit_by_region")
                        .build())
                .putNodes("profit_by_region", FlowNode.newBuilder()
                        .setNodeType(FlowNodeType.OUTPUT_NODE)
                        .addAllNodeAttrs(List.of(
                                TagUpdate.newBuilder()
                                        .setAttrName(ANALYSIS_TYPE)
                                        .setValue(pi_value)
                                        .build()
                        ))
                        .build())
                .putNodes("preprocessed_data", FlowNode.newBuilder()
                        .setNodeType(FlowNodeType.OUTPUT_NODE)
                        .build())
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("customer_loans"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("customer_loans")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("currency_data"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("currency_data")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("preprocessed_data"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_2").setSocket("preprocessed_data")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_2").setSocket("profit_by_region"))
                        .setTarget(FlowSocket.newBuilder().setNode("profit_by_region")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("preprocessed_data"))
                        .setTarget(FlowSocket.newBuilder().setNode("preprocessed_data")))
                .build();

        var createReq = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder().setObjectType(ObjectType.FLOW).setFlow(flowDef))
                .build();

        flowId = metaClient.createObject(createReq);
    }

    @Test @Order(2)
    void loadInputData() throws Exception {

        log.info("Loading input data...");

        var loansSchema = SchemaDefinition.newBuilder()
                .setSchemaType(SchemaType.TABLE)
                .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("id")
                                .setFieldType(BasicType.STRING)
                                .setBusinessKey(true))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("loan_amount")
                                .setFieldType(BasicType.DECIMAL))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("loan_condition_cat")
                                .setFieldType(BasicType.INTEGER))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("total_pymnt")
                                .setFieldType(BasicType.DECIMAL))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("region")
                                .setFieldType(BasicType.STRING)
                                .setCategorical(true)))
                .build();

        var currencySchema = SchemaDefinition.newBuilder()
                .setSchemaType(SchemaType.TABLE)
                .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("ccy_code")
                                .setFieldType(BasicType.STRING)
                                .setCategorical(true))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("spot_date")
                                .setFieldType(BasicType.DATE))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("dollar_rate")
                                .setFieldType(BasicType.DECIMAL)))
                .build();

        var loansAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_dataset")
                .setValue(MetadataCodec.encodeValue("run_flow:customer_loans"))
                .build());

        var currencyAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_dataset")
                .setValue(MetadataCodec.encodeValue("run_flow:currency_data"))
                .build());

        loansDataId = loadDataset(loansSchema, LOANS_INPUT_PATH, loansAttrs);
        currencyDataId = loadDataset(currencySchema, CURRENCY_INPUT_PATH, currencyAttrs);
    }

    TagHeader loadDataset(SchemaDefinition schema, String dataPath, List<TagUpdate> attrs) throws Exception {

        var dataClient = platform.dataClientBlocking();

        var inputPath = platform.tracRepoDir().resolve(dataPath);
        var inputBytes = Files.readAllBytes(inputPath);

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(schema)
                .setFormat("text/csv")
                .setContent(ByteString.copyFrom(inputBytes))
                .addAllTagUpdates(attrs)
                .build();

        return dataClient.createSmallDataset(writeRequest);
    }

    @Test @Order(3)
    void importModels() {

        log.info("Running IMPORT_MODEL job...");

        var modelsPath = "examples/models/python/src";
        var modelsVersion = "main";

        var model1EntryPoint = "tutorial.model_1.FirstModel";
        var model1Attrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("run_flow:chaining:model_1"))
                .build());
        var model1JobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("run_flow:import_model:model_1"))
                .build());

        var model2EntryPoint = "tutorial.model_2.SecondModel";
        var model2Attrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("run_flow:chaining:model_2"))
                .build());
        var model2JobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("run_flow:import_model:model_2"))
                .build());

        model1Id = importModel(
                modelsPath, model1EntryPoint, modelsVersion,
                model1Attrs, model1JobAttrs);

        model2Id = importModel(
                modelsPath, model2EntryPoint, modelsVersion,
                model2Attrs, model2JobAttrs);
    }

    private TagHeader importModel(
            String path, String entryPoint, String version,
            List<TagUpdate> modelAttrs, List<TagUpdate> jobAttrs) {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var importModel = ImportModelJob.newBuilder()
                .setLanguage("python")
                .setRepository(useTracRepo())
                .setPath(path)
                .setEntryPoint(entryPoint)
                .setVersion(version)
                .addAllModelAttrs(modelAttrs)
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_MODEL)
                        .setImportModel(importModel))
                .addAllJobAttrs(jobAttrs)
                .build();

        var jobStatus = runJob(orchClient, jobRequest);
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        var modelSearch = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName("trac_create_job")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(MetadataCodec.encodeValue(jobKey)))))
                .build();

        var modelSearchResult = metaClient.search(modelSearch);

        Assertions.assertEquals(1, modelSearchResult.getSearchResultCount());

        return modelSearchResult.getSearchResult(0).getHeader();
    }

    @Test @Order(4)
    void runFlow() {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var runFlow = RunFlowJob.newBuilder()
                .setFlow(MetadataUtil.selectorFor(flowId))
                .putParameters("param_1", MetadataCodec.encodeValue(2))
                .putParameters("param_2", MetadataCodec.encodeValue(LocalDate.now()))
                .putParameters("param_3", MetadataCodec.encodeValue(1.2345))
                .putInputs("customer_loans", MetadataUtil.selectorFor(loansDataId))
                .putInputs("currency_data", MetadataUtil.selectorFor(currencyDataId))
                .putModels("model_1", MetadataUtil.selectorFor(model1Id))
                .putModels("model_2", MetadataUtil.selectorFor(model2Id))
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_data")
                        .setValue(MetadataCodec.encodeValue("run_flow:data_output")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_FLOW)
                        .setRunFlow(runFlow))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_flow:run_flow")))
                .build();

        var jobStatus = runJob(orchClient, jobRequest);
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        var dataSearch = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName("trac_create_job")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(MetadataCodec.encodeValue(jobKey)))))
                .build();

        var dataSearchResult = metaClient.search(dataSearch);

        Assertions.assertEquals(2, dataSearchResult.getSearchResultCount());

        var indexedResult = dataSearchResult.getSearchResultList().stream()
                .collect(Collectors.toMap(RunFlowTest::getJobOutput, Function.identity()));

        var profitByRegionAnalysisType = indexedResult.get("profit_by_region")
                .getAttrsOrThrow(ANALYSIS_TYPE)
                .getFloatValue();
        Assertions.assertEquals(3.14, profitByRegionAnalysisType);

        Assertions.assertFalse(
            indexedResult.get("preprocessed_data")
                    .getAttrsMap().containsKey(ANALYSIS_TYPE)
        );

        var searchResult = dataSearchResult.getSearchResult(0);
        var dataReq = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(searchResult.getHeader()))
                .build();

        var dataTag = metaClient.readObject(dataReq);
        var dataDef = dataTag.getDefinition().getData();
        var outputAttr = dataTag.getAttrsOrThrow("e2e_test_data");

        Assertions.assertEquals("run_flow:data_output", MetadataCodec.decodeStringValue(outputAttr));
        Assertions.assertEquals(1, dataDef.getPartsCount());

        outputDataId = dataTag.getHeader();
    }

    private static String getJobOutput(org.finos.tracdap.metadata.Tag t) {
        var attr = t.getAttrsOrThrow("trac_job_output");
        return attr.getStringValue();
    }

    @Test @Order(5)
    void checkOutputData() {

        log.info("Checking output data...");

        var dataClient = platform.dataClientBlocking();

        var EXPECTED_REGIONS = 2;  // based on the chaining example models

        var readRequest = DataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(outputDataId))
                .setFormat("text/csv")
                .build();

        var readResponse = dataClient.readSmallDataset(readRequest);

        var csvText = readResponse.getContent().toString(StandardCharsets.UTF_8);
        var csvLines = csvText.split("\n");

        var csvHeaders = Arrays.stream(csvLines[0].split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        Assertions.assertEquals(List.of("region", "gross_profit"), csvHeaders);
        Assertions.assertEquals(EXPECTED_REGIONS + 1, csvLines.length);
    }
}
