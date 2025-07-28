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
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.metadata.RunModelJob;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.finos.tracdap.svc.orch.jobs.Helpers.runJob;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RunModelTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";
    private static final String INPUT_PATH = "examples/models/python/data/inputs/loan_final313_100_shortform.csv";
    private static final String STRUCT_INPUT_PATH = "examples/models/python/data/inputs/structured_run_config.json";

    // Only test E2E run model using the local repo
    // E2E model loading with different repo types is tested in ImportModelTest
    // We don't need to test all combinations of model run from different repo types

    protected String useTracRepo() { return "TRAC_LOCAL_REPO"; }

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

    private final Logger log = LoggerFactory.getLogger(getClass());

    static TagHeader inputDataId;
    static TagHeader modelId;
    static TagHeader outputDataId;
    static TagHeader optionalIoModelId;
    static TagHeader optionalIoOutputDataId;
    static TagHeader dynamicIoModelId;
    static TagHeader dynamicIoOutputDataId;

    static TagHeader inputFileId;
    static long inputFileSize;
    static TagHeader fileIoModelId;
    static TagHeader outputFileId;
    static TagHeader outputFileIdStream;

    static TagHeader structModelId;
    static SchemaDefinition structModelInputSchema;
    static TagHeader structInputDataId;
    static TagHeader structOutputDataId;

    @Test @Order(1)
    void loadInputData() throws Exception {

        log.info("Loading input data...");

        var metaClient = platform.metaClientBlocking();
        var dataClient = platform.dataClientBlocking();

        var inputSchema = SchemaDefinition.newBuilder()
                .setSchemaType(SchemaType.TABLE)
                .setTable(TableSchema.newBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("id")
                        .setFieldType(BasicType.STRING)
                        .setBusinessKey(true)
                        .setLabel("Customer ID"))
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("loan_amount")
                        .setFieldType(BasicType.DECIMAL)
                        .setLabel("Total loan amount"))
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("loan_condition_cat")
                        .setFieldType(BasicType.INTEGER)
                        .setLabel("Loan condition category code"))
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("total_pymnt")
                        .setFieldType(BasicType.DECIMAL)
                        .setLabel("Total payments to date"))
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("region")
                        .setFieldType(BasicType.STRING)
                        .setCategorical(true)
                        .setLabel("Customer region")))
                .build();

        var inputPath = platform.tracRepoDir().resolve(INPUT_PATH);
        var inputBytes = Files.readAllBytes(inputPath);

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(inputSchema)
                .setFormat("text/csv")
                .setContent(ByteString.copyFrom(inputBytes))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_dataset")
                        .setValue(MetadataCodec.encodeValue("run_model:customer_loans")))
                .build();

        inputDataId = dataClient.createSmallDataset(writeRequest);

        var dataSelector = MetadataUtil.selectorFor(inputDataId);
        var dataRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(dataSelector)
                .build();

        var dataTag = metaClient.readObject(dataRequest);

        var datasetAttr = dataTag.getAttrsOrThrow("e2e_test_dataset");
        var datasetSchema = dataTag.getDefinition().getData().getSchema();

        Assertions.assertEquals("run_model:customer_loans", MetadataCodec.decodeStringValue(datasetAttr));
        Assertions.assertEquals(SchemaType.TABLE, datasetSchema.getSchemaType());
        Assertions.assertEquals(5, datasetSchema.getTable().getFieldsCount());

        log.info("Input data loaded, data ID = [{}]", dataTag.getHeader().getObjectId());
    }

    @Test @Order(2)
    void importModel() throws Exception {

        log.info("Running IMPORT_MODEL job...");

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository(useTracRepo())
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.using_data.UsingDataModel")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_model")
                        .setValue(MetadataCodec.encodeValue("run_model:using_data"))
                        .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_model:import_model"))
                        .build());

        var modelTag = ImportModelTest.doImportModel(platform, TEST_TENANT, modelStub, modelAttrs, jobAttrs);

        var modelDef = modelTag.getDefinition().getModel();
        var modelAttr = modelTag.getAttrsOrThrow("e2e_test_model");

        Assertions.assertEquals("run_model:using_data", MetadataCodec.decodeStringValue(modelAttr));
        Assertions.assertEquals("tutorial.using_data.UsingDataModel", modelDef.getEntryPoint());
        Assertions.assertTrue(modelDef.getParametersMap().containsKey("eur_usd_rate"));
        Assertions.assertTrue(modelDef.getInputsMap().containsKey("customer_loans"));
        Assertions.assertTrue(modelDef.getOutputsMap().containsKey("profit_by_region"));

        modelId = modelTag.getHeader();
    }

    @Test @Order(3)
    void runModel() {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var runModel = RunModelJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(modelId))
                .putParameters("eur_usd_rate", MetadataCodec.encodeValue(1.3785))
                .putParameters("default_weighting", MetadataCodec.encodeValue(1.5))
                .putParameters("filter_defaults", MetadataCodec.encodeValue(true))
                .putInputs("customer_loans", MetadataUtil.selectorFor(inputDataId))
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_data")
                        .setValue(MetadataCodec.encodeValue("run_model:data_output")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setRunModel(runModel))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_model:run_model")))
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

        Assertions.assertEquals(1, dataSearchResult.getSearchResultCount());

        var searchResult = dataSearchResult.getSearchResult(0);
        var dataReq = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(searchResult.getHeader()))
                .build();

        var dataTag = metaClient.readObject(dataReq);
        var dataDef = dataTag.getDefinition().getData();
        var outputAttr = dataTag.getAttrsOrThrow("e2e_test_data");
        var fieldCountAttr = dataTag.getAttrsOrThrow("trac_schema_field_count");
        var rowCountAttr = dataTag.getAttrsOrThrow("trac_data_row_count");

        Assertions.assertEquals("run_model:data_output", MetadataCodec.decodeStringValue(outputAttr));
        Assertions.assertTrue(MetadataCodec.decodeIntegerValue(fieldCountAttr) > 0);
        Assertions.assertTrue(MetadataCodec.decodeIntegerValue(rowCountAttr) > 0);
        Assertions.assertEquals(1, dataDef.getPartsCount());

        outputDataId = dataTag.getHeader();
    }

    @Test @Order(4)
    void checkOutputData() {

        log.info("Checking output data...");

        var dataClient = platform.dataClientBlocking();

        var EXPECTED_REGIONS = 5;  // based on the sample dataset

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

    @Test @Order(5)
    void optionalIO_importModel() throws Exception {

        log.info("Running IMPORT_MODEL job for optional IO...");

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository(useTracRepo())
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.optional_io.OptionalIOModel")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_model")
                        .setValue(MetadataCodec.encodeValue("run_model:optional_io"))
                        .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_model:optional_io_import_model"))
                        .build());

        var modelTag = ImportModelTest.doImportModel(platform, TEST_TENANT, modelStub, modelAttrs, jobAttrs);
        var modelDef = modelTag.getDefinition().getModel();
        var modelAttr = modelTag.getAttrsOrThrow("e2e_test_model");

        Assertions.assertEquals("run_model:optional_io", MetadataCodec.decodeStringValue(modelAttr));
        Assertions.assertEquals("tutorial.optional_io.OptionalIOModel", modelDef.getEntryPoint());
        Assertions.assertTrue(modelDef.getInputsMap().containsKey("account_filter"));
        Assertions.assertTrue(modelDef.getOutputsMap().containsKey("exclusions"));

        optionalIoModelId = modelTag.getHeader();
    }

    @Test @Order(6)
    void optionalIO_runModel() {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        // Do not set the optional input (i.e. inputs look the same as UsingData model
        // Customer loans input is the same dataset as the UsingData model

        var runModel = RunModelJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(optionalIoModelId))
                .putParameters("eur_usd_rate", MetadataCodec.encodeValue(1.3785))
                .putParameters("default_weighting", MetadataCodec.encodeValue(1.5))
                .putParameters("filter_defaults", MetadataCodec.encodeValue(true))
                .putInputs("customer_loans", MetadataUtil.selectorFor(inputDataId))
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_data")
                        .setValue(MetadataCodec.encodeValue("run_model:optional_io_data_output")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_MODEL)
                        .setRunModel(runModel))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_model:optional_io_run_model")))
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

        // Only one output - optional exclusions output is not returned when account_filter is not supplied
        Assertions.assertEquals(1, dataSearchResult.getSearchResultCount());

        var searchResult = dataSearchResult.getSearchResult(0);
        var dataReq = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(searchResult.getHeader()))
                .build();

        var dataTag = metaClient.readObject(dataReq);
        var dataDef = dataTag.getDefinition().getData();
        var outputAttr = dataTag.getAttrsOrThrow("e2e_test_data");

        Assertions.assertEquals("run_model:optional_io_data_output", MetadataCodec.decodeStringValue(outputAttr));
        Assertions.assertEquals(1, dataDef.getPartsCount());

        optionalIoOutputDataId = dataTag.getHeader();
    }

    @Test @Order(7)
    void optionalIO_checkOutputData() {

        log.info("Checking output data for optional IO...");

        var dataClient = platform.dataClientBlocking();

        var EXPECTED_REGIONS = 5;  // based on the sample dataset

        var readRequest = DataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(optionalIoOutputDataId))
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

    @Test @Order(8)
    void dynamicIO_importModel() throws Exception {

        log.info("Running IMPORT_MODEL job for dynamic IO...");

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository(useTracRepo())
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.dynamic_io.DynamicDataFilter")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_model")
                        .setValue(MetadataCodec.encodeValue("run_model:dynamic_io"))
                        .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_model:dynamic_io_import_model"))
                        .build());

        var modelTag = ImportModelTest.doImportModel(platform, TEST_TENANT, modelStub, modelAttrs, jobAttrs);
        var modelDef = modelTag.getDefinition().getModel();
        var modelAttr = modelTag.getAttrsOrThrow("e2e_test_model");

        Assertions.assertEquals("run_model:dynamic_io", MetadataCodec.decodeStringValue(modelAttr));
        Assertions.assertEquals("tutorial.dynamic_io.DynamicDataFilter", modelDef.getEntryPoint());
        Assertions.assertTrue(modelDef.getInputsMap().containsKey("original_data"));
        Assertions.assertTrue(modelDef.getOutputsMap().containsKey("filtered_data"));

        dynamicIoModelId = modelTag.getHeader();
    }

    @Test @Order(9)
    void dynamicIO_runModel() {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        // Do not set the optional input (i.e. inputs look the same as UsingData model
        // Customer loans input is the same dataset as the UsingData model

        var runModel = RunModelJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(dynamicIoModelId))
                .putParameters("filter_column", MetadataCodec.encodeValue("region"))
                .putParameters("filter_value", MetadataCodec.encodeValue("munster"))
                .putInputs("original_data", MetadataUtil.selectorFor(inputDataId))
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_data")
                        .setValue(MetadataCodec.encodeValue("run_model:dynamic_io_data_output")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_MODEL)
                        .setRunModel(runModel))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_model:dynamic_io_run_model")))
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

        // Only one output - optional exclusions output is not returned when account_filter is not supplied
        Assertions.assertEquals(1, dataSearchResult.getSearchResultCount());

        var searchResult = dataSearchResult.getSearchResult(0);
        var dataReq = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(searchResult.getHeader()))
                .build();

        var dataTag = metaClient.readObject(dataReq);
        var dataDef = dataTag.getDefinition().getData();
        var outputAttr = dataTag.getAttrsOrThrow("e2e_test_data");

        Assertions.assertEquals("run_model:dynamic_io_data_output", MetadataCodec.decodeStringValue(outputAttr));
        Assertions.assertEquals(1, dataDef.getPartsCount());

        dynamicIoOutputDataId = dataTag.getHeader();
    }

    @Test @Order(10)
    void dynamicIO_checkOutputData() {

        log.info("Checking output data for dynamic IO...");

        var dataClient = platform.dataClientBlocking();

        var readRequest = DataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(dynamicIoOutputDataId))
                .setFormat("text/csv")
                .build();


        var readResponse = dataClient.readSmallDataset(readRequest);

        var csvText = readResponse.getContent().toString(StandardCharsets.UTF_8);
        var csvLines = csvText.split("\n");

        var csvHeaders = Arrays.stream(csvLines[0].split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        Assertions.assertEquals(List.of("id","loan_amount","loan_condition_cat","total_pymnt","region"), csvHeaders);

        // Check the dynamic filter was applied successfully

        Assertions.assertTrue(csvText.contains("leinster"));
        Assertions.assertFalse(csvText.contains("munster"));
    }

    @Test @Order(11)
    void fileIO_loadInputData() {

        log.info("Loading input file...");

        var sampleData = "Some text in a file\r\n".getBytes(StandardCharsets.UTF_8);
        var sampleDataSize = sampleData.length;

        var metaClient = platform.metaClientBlocking();
        var dataClient = platform.dataClientBlocking();

        var writeRequest = FileWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setName("input_file.txt")
                .setMimeType("text/plain")
                .setContent(ByteString.copyFrom(sampleData))
                .setSize(sampleDataSize)
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_file")
                        .setValue(MetadataCodec.encodeValue("run_model:file_io")))
                .build();

        inputFileId = dataClient.createSmallFile(writeRequest);
        inputFileSize = sampleDataSize;

        var fileSelector = MetadataUtil.selectorFor(inputFileId);
        var fileRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(fileSelector)
                .build();

        var fileTag = metaClient.readObject(fileRequest);

        var fileAttr = fileTag.getAttrsOrThrow("e2e_test_file");
        var fileDef = fileTag.getDefinition().getFile();

        Assertions.assertEquals("run_model:file_io", MetadataCodec.decodeStringValue(fileAttr));
        Assertions.assertEquals(sampleDataSize, fileDef.getSize());

        log.info("Input file loaded, data ID = [{}]", fileTag.getHeader().getObjectId());
    }

    @Test @Order(12)
    void fileIO_importModel() throws Exception {

        log.info("Running IMPORT_MODEL job for file IO...");

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository(useTracRepo())
                .setPath("tracdap-services/tracdap-svc-orch/src/test/resources")
                .setEntryPoint("file_io.FileIOModel")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("run_model:file_io"))
                .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("run_model:file_io_import_model"))
                .build());

        var modelTag = ImportModelTest.doImportModel(platform, TEST_TENANT, modelStub, modelAttrs, jobAttrs);
        var modelDef = modelTag.getDefinition().getModel();
        var modelAttr = modelTag.getAttrsOrThrow("e2e_test_model");

        Assertions.assertEquals("run_model:file_io", MetadataCodec.decodeStringValue(modelAttr));
        Assertions.assertEquals("file_io.FileIOModel", modelDef.getEntryPoint());
        Assertions.assertTrue(modelDef.getInputsMap().containsKey("file_input"));
        Assertions.assertTrue(modelDef.getOutputsMap().containsKey("file_output"));

        fileIoModelId = modelTag.getHeader();
    }

    @ParameterizedTest() @Order(13)
    @ValueSource(booleans= {true, false})
    void fileIO_runModel(boolean useStreams) {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var runModel = RunModelJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(fileIoModelId))
                .putParameters("n_copies", MetadataCodec.encodeValue(3))
                .putParameters("use_streams", MetadataCodec.encodeValue(useStreams))
                .putInputs("file_input", MetadataUtil.selectorFor(inputFileId))
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_data")
                        .setValue(MetadataCodec.encodeValue("run_model:file_io_data_output")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_MODEL)
                        .setRunModel(runModel))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_model:file_io_run_model")))
                .build();

        var jobStatus = runJob(orchClient, jobRequest);
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        // TRAC creates an extra FILE output which is the job log
        // Use a logical search expression to find the actual model output

        var fileSearch = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.FILE)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                .setOperator(LogicalOperator.AND)
                .addExpr(SearchExpression.newBuilder()
                        .setTerm(SearchTerm.newBuilder()
                        .setAttrName("trac_create_job")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(MetadataCodec.encodeValue(jobKey))))
                .addExpr(SearchExpression.newBuilder()
                        .setTerm(SearchTerm.newBuilder()
                        .setAttrName("trac_job_output")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(MetadataCodec.encodeValue("file_output")))))))
                .build();

        var dataSearchResult = metaClient.search(fileSearch);

        Assertions.assertEquals(1, dataSearchResult.getSearchResultCount());

        var searchResult = dataSearchResult.getSearchResult(0);
        var fileReq = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(searchResult.getHeader()))
                .build();

        var fileTag = metaClient.readObject(fileReq);
        var fileDef = fileTag.getDefinition().getFile();
        var outputAttr = fileTag.getAttrsOrThrow("e2e_test_data");

        Assertions.assertEquals("run_model:file_io_data_output", MetadataCodec.decodeStringValue(outputAttr));
        Assertions.assertEquals(inputFileSize * 3, fileDef.getSize());

        if (useStreams)
            outputFileIdStream = fileTag.getHeader();
        else
            outputFileId = fileTag.getHeader();
    }

    @ParameterizedTest @Order(14)
    @ValueSource(booleans= {true, false})
    void fileIO_checkOutputData(boolean useStreams) {

        log.info("Checking output data for file IO...");

        var dataClient = platform.dataClientBlocking();

        var fileId = useStreams ? outputFileIdStream : outputFileId;

        var readRequest = FileReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(fileId))
                .build();

        var readResponse = dataClient.readSmallFile(readRequest);

        var expectedContent = "Some text in a file\r\n".repeat(3);
        var fileContents = readResponse.getContent().toString(StandardCharsets.UTF_8);

        Assertions.assertEquals(expectedContent, fileContents);
    }

    @Test @Order(15)
    void struct_importModel() throws Exception {

        log.info("Running IMPORT_MODEL job for struct...");

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository(useTracRepo())
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.structured_objects.StructModel")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("run_model:struct"))
                .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("run_model:struct_import_model"))
                .build());

        var modelTag = ImportModelTest.doImportModel(platform, TEST_TENANT, modelStub, modelAttrs, jobAttrs);
        var modelDef = modelTag.getDefinition().getModel();
        var modelAttr = modelTag.getAttrsOrThrow("e2e_test_model");

        Assertions.assertEquals("run_model:struct", MetadataCodec.decodeStringValue(modelAttr));
        Assertions.assertEquals("tutorial.structured_objects.StructModel", modelDef.getEntryPoint());
        Assertions.assertTrue(modelDef.getInputsMap().containsKey("run_config"));
        Assertions.assertTrue(modelDef.getOutputsMap().containsKey("modified_config"));

        structModelId = modelTag.getHeader();
        structModelInputSchema = modelDef.getInputsOrThrow("run_config").getSchema();
    }

    @Test @Order(16)
    void struct_loadInputData() throws Exception {

        log.info("Loading struct input data...");

        var metaClient = platform.metaClientBlocking();
        var dataClient = platform.dataClientBlocking();

        var inputPath = platform.tracRepoDir().resolve(STRUCT_INPUT_PATH);
        var inputBytes = Files.readAllBytes(inputPath);

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(structModelInputSchema)
                .setFormat("application/json")
                .setContent(ByteString.copyFrom(inputBytes))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_dataset")
                        .setValue(MetadataCodec.encodeValue("run_model:run_config")))
                .build();

        structInputDataId = dataClient.createSmallDataset(writeRequest);

        var dataSelector = MetadataUtil.selectorFor(structInputDataId);
        var dataRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(dataSelector)
                .build();

        var dataTag = metaClient.readObject(dataRequest);

        var datasetAttr = dataTag.getAttrsOrThrow("e2e_test_dataset");
        var datasetSchema = dataTag.getDefinition().getData().getSchema();

        Assertions.assertEquals("run_model:run_config", MetadataCodec.decodeStringValue(datasetAttr));
        Assertions.assertEquals(SchemaType.STRUCT_SCHEMA, datasetSchema.getSchemaType());
        Assertions.assertEquals(4, datasetSchema.getFieldsCount());

        log.info("Struct input data loaded, data ID = [{}]", dataTag.getHeader().getObjectId());
    }

    @Test @Order(17)
    void struct_runModel() {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var runModel = RunModelJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(structModelId))
                .putParameters("t0_date", MetadataCodec.encodeValue(LocalDate.now()))
                .putParameters("projection_period", MetadataCodec.encodeValue(365))
                .putInputs("run_config", MetadataUtil.selectorFor(structInputDataId))
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_data")
                        .setValue(MetadataCodec.encodeValue("run_model:modified_config")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_MODEL)
                        .setRunModel(runModel))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_model:structured_objects")))
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

        Assertions.assertEquals(1, dataSearchResult.getSearchResultCount());

        var searchResult = dataSearchResult.getSearchResult(0);
        var dataReq = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(searchResult.getHeader()))
                .build();

        var dataTag = metaClient.readObject(dataReq);
        var dataDef = dataTag.getDefinition().getData();
        var outputAttr = dataTag.getAttrsOrThrow("e2e_test_data");

        Assertions.assertEquals("run_model:modified_config", MetadataCodec.decodeStringValue(outputAttr));
        Assertions.assertEquals(1, dataDef.getPartsCount());

        structOutputDataId = dataTag.getHeader();
    }

    @Test @Order(18)
    void struct_checkOutputData() {

        log.info("Checking struct output data...");

        var dataClient = platform.dataClientBlocking();

        var readRequest = DataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(structOutputDataId))
                .setFormat("application/json")
                .build();


        var readResponse = dataClient.readSmallDataset(readRequest);
        var jsonText = readResponse.getContent().toString(StandardCharsets.UTF_8);

        // Model should have added this scenario, just look for its name
        Assertions.assertTrue(jsonText.contains("hpi_shock"));
    }
}
