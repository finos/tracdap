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
import org.finos.tracdap.metadata.ImportDataJob;
import org.finos.tracdap.metadata.ExportDataJob;
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
public class ImportExportDataTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";
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

    static TagHeader exportModelId;
    static SchemaDefinition exportInputSchema;

    static TagHeader jobId_exportDataModel;

    static TagHeader exportInputDataId;
    static TagHeader jobId_exportData;

    @Test @Order(101)
    void prepareExternalStorage() throws Exception {

        var externalStorageDir = platform.workingDir().resolve("external_storage");
        Files.createDirectories(externalStorageDir);
    }

    @Test @Order(104)
    void exportDataModel() throws Exception {

        log.info("Running IMPORT_MODEL job for DataExportExample...");

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("TRAC_LOCAL_REPO")
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.data_export.DataExportExample")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("import_export_data:data_export_example"))
                .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("import_export_data:import_data_export_example"))
                .build());

        jobId_exportDataModel = Helpers.startModelImport(platform, TEST_TENANT, modelStub, modelAttrs, jobAttrs);
    }

    @Test @Order(105)
    void exportDataModel_result() {

        var modelTag = Helpers.waitForModelImport(platform, TEST_TENANT, jobId_exportDataModel);
        var modelDef = modelTag.getDefinition().getModel();

        Assertions.assertEquals(ModelType.DATA_EXPORT_MODEL, modelDef.getModelType());
        Assertions.assertEquals("tutorial.data_export.DataExportExample", modelDef.getEntryPoint());
        Assertions.assertTrue(modelDef.getInputsMap().containsKey("profit_by_region"));

        exportModelId = modelTag.getHeader();
        exportInputSchema = modelDef.getInputsOrThrow("profit_by_region").getSchema();
    }

    @Test @Order(301)
    void prepareExportInput() {

        log.info("Loading export input data...");

        var dataClient = platform.dataClientBlocking();

        var csvContent = "region,gross_profit\r\nmunster,1000.50\r\nleinster,2000.75\r\n"
                .getBytes(StandardCharsets.UTF_8);

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(exportInputSchema)
                .setFormat("text/csv")
                .setContent(ByteString.copyFrom(csvContent))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_dataset")
                        .setValue(MetadataCodec.encodeValue("import_export_data:profit_by_region")))
                .build();

        exportInputDataId = dataClient.createSmallDataset(writeRequest);
    }

    @Test @Order(302)
    void exportData() {

        var orchClient = platform.orchClientBlocking();

        var exportData = ExportDataJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(exportModelId))
                .putParameters("storage_key", MetadataCodec.encodeValue(EXTERNAL_STORAGE_KEY))
                .putParameters("no_overwrite", MetadataCodec.encodeValue(false))
                .putParameters("export_comment", MetadataCodec.encodeValue("import_export_data e2e test"))
                .putInputs("profit_by_region", MetadataUtil.selectorFor(exportInputDataId))
                .addStorageAccess(EXTERNAL_STORAGE_KEY)
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.EXPORT_DATA)
                        .setExportData(exportData))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("import_export_data:export_data")))
                .build();

        jobId_exportData = Helpers.startJob(orchClient, jobRequest).getJobId();
    }

    @Test @Order(303)
    void exportData_result() throws Exception {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var jobStatus = Helpers.waitForJob(orchClient, TEST_TENANT, jobId_exportData);
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        var jobReq = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(jobStatus.getJobId()))
                .build();

        var jobTag = metaClient.readObject(jobReq);
        var exportDataJob = jobTag.getDefinition().getJob().getExportData();

        Assertions.assertEquals(JobType.EXPORT_DATA, jobTag.getDefinition().getJob().getJobType());
        Assertions.assertEquals(MetadataUtil.objectKey(exportModelId), MetadataUtil.objectKey(exportDataJob.getModel()));

        // Export has no TRAC output - confirm deliberately, not just assumed
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
        Assertions.assertEquals(0, dataSearchResult.getSearchResultCount());

        var exportedFile = platform.workingDir().resolve("external_storage/data_export_example/profit_by_region.csv");
        Assertions.assertTrue(Files.exists(exportedFile));

        var exportedContent = Files.readString(exportedFile, StandardCharsets.UTF_8);
        var exportedLines = exportedContent.strip().split("\n");

        Assertions.assertEquals(3, exportedLines.length);
        Assertions.assertTrue(exportedLines[0].contains("region"));
        Assertions.assertTrue(exportedLines[0].contains("gross_profit"));
        Assertions.assertTrue(exportedContent.contains("munster"));
        Assertions.assertTrue(exportedContent.contains("leinster"));
    }

}
