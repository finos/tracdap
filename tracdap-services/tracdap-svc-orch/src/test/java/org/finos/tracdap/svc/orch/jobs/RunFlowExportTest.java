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
import org.finos.tracdap.metadata.RunFlowJob;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RunFlowExportTest {

    // A flow of standard models ending in an export model node, writing to external storage

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";
    private static final String LOANS_INPUT_PATH = "examples/models/python/data/inputs/loan_final313_100_shortform.csv";
    private static final String EXTERNAL_STORAGE_KEY = "TEST_EXTERNAL_STORAGE";

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

    static TagHeader aggregationModelId;
    static TagHeader exportModelId;
    static TagHeader flowId;
    static TagHeader loansDataId;

    static TagHeader jobId_importModel_aggregation;
    static TagHeader jobId_importModel_export;
    static TagHeader jobId_runFlow;

    @Test @Order(101)
    void prepareExternalStorage() throws Exception {

        Files.createDirectories(platform.workingDir().resolve("external_storage"));
    }

    @Test @Order(102)
    void importModels() throws Exception {

        log.info("Running IMPORT_MODEL jobs...");

        var aggregationAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("run_flow_export:pnl_aggregation"))
                .build());

        var exportAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("run_flow_export:data_export"))
                .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("run_flow_export:import_model"))
                .build());

        jobId_importModel_aggregation = Helpers.startModelImport(
                platform, TEST_TENANT, modelStub("tutorial.using_data.PnlAggregation"), aggregationAttrs, jobAttrs);

        jobId_importModel_export = Helpers.startModelImport(
                platform, TEST_TENANT, modelStub("tutorial.data_export.DataExportExample"), exportAttrs, jobAttrs);
    }

    @Test @Order(103)
    void importModels_result() {

        aggregationModelId = Helpers.waitForModelImport(platform, TEST_TENANT, jobId_importModel_aggregation).getHeader();

        var exportModelTag = Helpers.waitForModelImport(platform, TEST_TENANT, jobId_importModel_export);
        Assertions.assertEquals(ModelType.DATA_EXPORT_MODEL, exportModelTag.getDefinition().getModel().getModelType());

        exportModelId = exportModelTag.getHeader();
    }

    private ModelDefinition modelStub(String entryPoint) throws Exception {

        return ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("TRAC_LOCAL_REPO")
                .setPath("examples/models/python/src")
                .setEntryPoint(entryPoint)
                .setVersion(GitHelpers.getCurrentCommit())
                .build();
    }

    @Test @Order(201)
    void createFlow() {

        var metaClient = platform.metaClientBlocking();

        var flowDef = FlowDefinition.newBuilder()
                .putNodes("customer_loans", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("pnl_aggregation", FlowNode.newBuilder()
                        .setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("customer_loans")
                        .addOutputs("profit_by_region")
                        .build())
                .putNodes("profit_by_region", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())
                .putNodes("data_export", FlowNode.newBuilder()
                        .setNodeType(FlowNodeType.MODEL_NODE)
                        .setModelType(ModelType.DATA_EXPORT_MODEL)
                        .addInputs("profit_by_region")
                        .build())
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("customer_loans"))
                        .setTarget(FlowSocket.newBuilder().setNode("pnl_aggregation").setSocket("customer_loans")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("pnl_aggregation").setSocket("profit_by_region"))
                        .setTarget(FlowSocket.newBuilder().setNode("profit_by_region")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("pnl_aggregation").setSocket("profit_by_region"))
                        .setTarget(FlowSocket.newBuilder().setNode("data_export").setSocket("profit_by_region")))
                .build();

        var createReq = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder().setObjectType(ObjectType.FLOW).setFlow(flowDef))
                .build();

        flowId = metaClient.createObject(createReq);
    }

    @Test @Order(202)
    void loadInputData() throws Exception {

        var dataClient = platform.dataClientBlocking();

        var loansSchema = SchemaDefinition.newBuilder()
                .setSchemaType(SchemaType.TABLE)
                .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("id")
                                .setFieldType(BasicType.STRING)
                                .setBusinessKey(true)
                                .setLabel("Customer Id"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("loan_amount")
                                .setFieldType(BasicType.DECIMAL)
                                .setLabel("Loan amount"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("loan_condition_cat")
                                .setFieldType(BasicType.INTEGER)
                                .setLabel("Loan condition category code"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("total_pymnt")
                                .setFieldType(BasicType.DECIMAL)
                                .setLabel("Total payment received to date"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("region")
                                .setFieldType(BasicType.STRING)
                                .setCategorical(true)
                                .setLabel("Customer region")))
                .build();

        var inputBytes = Files.readAllBytes(platform.tracRepoDir().resolve(LOANS_INPUT_PATH));

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(loansSchema)
                .setFormat("text/csv")
                .setContent(ByteString.copyFrom(inputBytes))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_dataset")
                        .setValue(MetadataCodec.encodeValue("run_flow_export:customer_loans")))
                .build();

        loansDataId = dataClient.createSmallDataset(writeRequest);
    }

    @Test @Order(301)
    void runFlow() {

        var orchClient = platform.orchClientBlocking();

        var runFlow = RunFlowJob.newBuilder()
                .setFlow(MetadataUtil.selectorFor(flowId))
                .putParameters("eur_usd_rate", MetadataCodec.encodeValue(1.2071))
                .putParameters("default_weighting", MetadataCodec.encodeValue(1.5))
                .putParameters("filter_defaults", MetadataCodec.encodeValue(false))
                .putParameters("storage_key", MetadataCodec.encodeValue(EXTERNAL_STORAGE_KEY))
                .putParameters("no_overwrite", MetadataCodec.encodeValue(false))
                .putParameters("export_comment", MetadataCodec.encodeValue("run_flow_export e2e test"))
                .putInputs("customer_loans", MetadataUtil.selectorFor(loansDataId))
                .putModels("pnl_aggregation", MetadataUtil.selectorFor(aggregationModelId))
                .putModels("data_export", MetadataUtil.selectorFor(exportModelId))
                .addExportStorageAccess(EXTERNAL_STORAGE_KEY)
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_FLOW)
                        .setRunFlow(runFlow))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("run_flow_export:run_flow")))
                .build();

        jobId_runFlow = Helpers.startJob(orchClient, jobRequest).getJobId();
    }

    @Test @Order(302)
    void runFlow_result() throws Exception {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var jobStatus = Helpers.waitForJob(orchClient, TEST_TENANT, jobId_runFlow);
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        // The job records the export storage it was given

        var jobTag = metaClient.readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(jobStatus.getJobId()))
                .build());

        var runFlowJob = jobTag.getDefinition().getJob().getRunFlow();
        Assertions.assertEquals(List.of(EXTERNAL_STORAGE_KEY), runFlowJob.getExportStorageAccessList());

        // The flow output is saved in TRAC, the export model node creates no TRAC outputs

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

        // The export model node writes the same data to external storage

        var exportedFile = platform.workingDir().resolve("external_storage/data_export_example/profit_by_region.csv");
        Assertions.assertTrue(Files.exists(exportedFile));

        var exportedContent = Files.readString(exportedFile, StandardCharsets.UTF_8);
        var exportedLines = exportedContent.strip().split("\n");

        Assertions.assertTrue(exportedLines.length > 1);
        Assertions.assertTrue(exportedLines[0].contains("region"));
        Assertions.assertTrue(exportedLines[0].contains("gross_profit"));
        Assertions.assertTrue(exportedContent.contains("run_flow_export e2e test"));
    }
}
