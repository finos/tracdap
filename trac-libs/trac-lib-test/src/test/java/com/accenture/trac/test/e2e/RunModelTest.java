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

import java.nio.file.Files;
import java.nio.file.Paths;


@Tag("integration")
@Tag("int-e2e")
public class RunModelTest extends PlatformTestBase {

    static TagHeader inputDataId;
    static TagHeader modelId;

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

        var inputPath = Paths.get("../../examples/models/python/data/inputs/loan_final313_100_shortform.csv");
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
    void importModel() {

        log.info("Running IMPORT_MODEL job...");

        var importModel = ImportModelJob.newBuilder()
                .setLanguage("python")
                .setRepository("trac_git_repo")   // TODO
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

        log.info("Job status: {}", jobStatus.toString());

        var jobId = jobStatus.getJobId();
    }
}
