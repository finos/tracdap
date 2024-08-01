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


public class JobValidationTest {

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TEST_TENANT = "ACME_CORP";

    private static final byte[] BASIC_CSV_CONTENT = ResourceHelpers.loadResourceAsBytes(SampleData.BASIC_CSV_DATA_RESOURCE);
    private static final byte[] BASIC_CSV_CONTENT_V2 = ResourceHelpers.loadResourceAsBytes(SampleData.BASIC_CSV_DATA_RESOURCE_V2);

    protected static TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub metaClient;
    protected static TracDataApiGrpc.TracDataApiBlockingStub dataClient;
    protected static TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClient;

    protected static TagSelector basicDataSelector;
    protected static TagSelector enrichedDataSelector;

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
        Assertions.fail("todo");
    }

    @Test
    public void runModel_validateOk() {

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
                        .setSchema(SampleData.BASIC_TABLE_SCHEMA)
                        .build())
                .putOutputs("enriched_output", ModelOutputSchema.newBuilder()
                        .setSchema(SampleData.BASIC_TABLE_SCHEMA_V2)
                        .build())
                .build();

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setModel(modelDef))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("model_key")
                        .setValue(MetadataCodec.encodeValue("basc_test_model")))
                .build();

        var modelId = metaClient.createObject(writeRequest);
        var modelSelector = MetadataUtil.selectorFor(modelId);

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
                        .setValue(MetadataCodec.encodeValue("test_ model_ok")))
                .build();

        var jobStatus = orchClient.validateJob(jobRequest);

        Assertions.assertEquals(JobStatusCode.VALIDATED, jobStatus.getStatusCode());
    }

    @Test
    public void runModel_badInput() {
        Assertions.fail("todo");
    }

    @Test
    public void runModel_inconsistent() {
        Assertions.fail("todo");
    }

    @Test
    public void runFlow_validateOk() {
        Assertions.fail("todo");
    }

    @Test
    public void runFlow_badInput() {
        Assertions.fail("todo");
    }

    @Test
    public void runFlow_inconsistent() {
        Assertions.fail("todo");
    }
}
