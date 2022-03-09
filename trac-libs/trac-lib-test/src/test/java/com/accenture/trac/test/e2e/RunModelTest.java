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

package com.accenture.trac.test.e2e;

import com.accenture.trac.api.*;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.MetadataUtil;
import com.accenture.trac.metadata.*;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RunModelTest extends PlatformTestBase {

    private static final String INPUT_PATH = "examples/models/python/data/inputs/loan_final313_100_shortform.csv";

    private static final List<JobStatusCode> COMPLETED_JOB_STATES = List.of(
            JobStatusCode.SUCCEEDED,
            JobStatusCode.FAILED,
            JobStatusCode.CANCELLED);


    static TagHeader inputDataId;
    static TagHeader modelId;
    static TagHeader outputDataId;

    @Test @Order(1)
    void loadInputData() throws Exception {

        log.info("Loading input data...");

        var inputSchema = SchemaDefinition.newBuilder()
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
                        .setFieldType(BasicType.INTEGER)
                        .setCategorical(true))
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("total_pymnt")
                        .setFieldType(BasicType.DECIMAL))
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("region")
                        .setFieldType(BasicType.STRING)
                        .setCategorical(true)))
                .build();

        var inputPath = tracRepoDir.resolve(INPUT_PATH);
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

        var importModel = ImportModelJob.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setPath("examples/models/python/using_data")
                .setEntryPoint("using_data.UsingDataModel")
                .setVersion("main")
                .addModelAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_model")
                        .setValue(MetadataCodec.encodeValue("run_model:using_data")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_MODEL)
                .setImportModel(importModel))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_model:import_model")))
                .build();

        var jobStatus = orchClient.submitJob(jobRequest);
        log.info("Job ID: [{}]", MetadataUtil.objectKey(jobStatus.getJobId()));
        log.info("Job status: [{}] {}", jobStatus.getStatusCode(), jobStatus.getStatusMessage());

        var statusRequest = JobStatusRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(jobStatus.getJobId()))
                .build();

        while (!COMPLETED_JOB_STATES.contains(jobStatus.getStatusCode())) {
            Thread.sleep(1000);
            jobStatus = orchClient.checkJob(statusRequest);
            log.info("Job status: [{}] {}", jobStatus.getStatusCode(), jobStatus.getStatusMessage());
        }

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

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

        var searchResult = modelSearchResult.getSearchResult(0);
        var modelReq = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(searchResult.getHeader()))
                .build();

        var modelTag = metaClient.readObject(modelReq);
        var modelDef = modelTag.getDefinition().getModel();
        var modelAttr = modelTag.getAttrsOrThrow("e2e_test_model");

        Assertions.assertEquals("run_model:using_data", MetadataCodec.decodeStringValue(modelAttr));
        Assertions.assertEquals("using_data.UsingDataModel", modelDef.getEntryPoint());
        Assertions.assertTrue(modelDef.getParametersMap().containsKey("eur_usd_rate"));
        Assertions.assertTrue(modelDef.getInputsMap().containsKey("customer_loans"));
        Assertions.assertTrue(modelDef.getOutputsMap().containsKey("profit_by_region"));

        modelId = modelTag.getHeader();
    }

    @Test @Order(3)
    void runModel() throws Exception {

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

        var jobStatus = orchClient.submitJob(jobRequest);
        log.info("Job ID: [{}]", MetadataUtil.objectKey(jobStatus.getJobId()));
        log.info("Job status: [{}] {}", jobStatus.getStatusCode(), jobStatus.getStatusMessage());

        var statusRequest = JobStatusRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(jobStatus.getJobId()))
                .build();

        while (!COMPLETED_JOB_STATES.contains(jobStatus.getStatusCode())) {
            Thread.sleep(1000);
            jobStatus = orchClient.checkJob(statusRequest);
            log.info("Job status: [{}] {}", jobStatus.getStatusCode(), jobStatus.getStatusMessage());
        }

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

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

        Assertions.assertEquals("run_model:data_output", MetadataCodec.decodeStringValue(outputAttr));
        Assertions.assertEquals(1, dataDef.getPartsCount());

        outputDataId = dataTag.getHeader();
    }

    @Test @Order(4)
    void checkOutputData() {

        log.info("Checking output data...");

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
        Assertions.assertEquals(101, csvLines.length);
    }

}
