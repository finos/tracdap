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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ImportExportDataTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";
    private static final String IMPORT_SOURCE_PATH = "examples/models/python/data/inputs/staging/sample_data.parquet";
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

    static TagHeader importModelId;
    static TagHeader exportModelId;
    static SchemaDefinition exportInputSchema;

    static TagHeader jobId_importDataModel;
    static TagHeader jobId_exportDataModel;

    static TagHeader jobId_importData;
    static TagHeader exportInputDataId;
    static TagHeader jobId_exportData;

    @Test @Order(101)
    void prepareExternalStorage() throws Exception {

        var externalStorageDir = platform.workingDir().resolve("external_storage");
        Files.createDirectories(externalStorageDir);

        var sourcePath = platform.tracRepoDir().resolve(IMPORT_SOURCE_PATH);
        Files.copy(sourcePath, externalStorageDir.resolve("sample_data.parquet"), StandardCopyOption.REPLACE_EXISTING);
    }

    @Test @Order(102)
    void importDataModel() throws Exception {

        log.info("Running IMPORT_MODEL job for SimpleDataImport...");

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("TRAC_LOCAL_REPO")
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.data_import.SimpleDataImport")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("import_export_data:simple_data_import"))
                .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("import_export_data:import_simple_data_import"))
                .build());

        jobId_importDataModel = Helpers.startModelImport(platform, TEST_TENANT, modelStub, modelAttrs, jobAttrs);
    }

    @Test @Order(103)
    void importDataModel_result() {

        var modelTag = Helpers.waitForModelImport(platform, TEST_TENANT, jobId_importDataModel);
        var modelDef = modelTag.getDefinition().getModel();

        Assertions.assertEquals(ModelType.DATA_IMPORT_MODEL, modelDef.getModelType());
        Assertions.assertEquals("tutorial.data_import.SimpleDataImport", modelDef.getEntryPoint());
        Assertions.assertTrue(modelDef.getOutputsMap().containsKey("customer_loans"));

        importModelId = modelTag.getHeader();
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

    @Test @Order(201)
    void importData() {

        var orchClient = platform.orchClientBlocking();

        var importData = ImportDataJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(importModelId))
                .putParameters("storage_key", MetadataCodec.encodeValue(EXTERNAL_STORAGE_KEY))
                .putParameters("source_file", MetadataCodec.encodeValue("sample_data.parquet"))
                .addStorageAccess(EXTERNAL_STORAGE_KEY)
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_data")
                        .setValue(MetadataCodec.encodeValue("import_export_data:customer_loans")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_DATA)
                        .setImportData(importData))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("import_export_data:import_data")))
                .build();

        jobId_importData = Helpers.startJob(orchClient, jobRequest).getJobId();
    }

    @Test @Order(202)
    void importData_result() {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var jobStatus = Helpers.waitForJob(orchClient, TEST_TENANT, jobId_importData);
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        var jobReq = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(jobStatus.getJobId()))
                .build();

        var jobTag = metaClient.readObject(jobReq);
        var importDataJob = jobTag.getDefinition().getJob().getImportData();

        Assertions.assertEquals(JobType.IMPORT_DATA, jobTag.getDefinition().getJob().getJobType());
        Assertions.assertEquals(MetadataUtil.objectKey(importModelId), MetadataUtil.objectKey(importDataJob.getModel()));
        Assertions.assertTrue(importDataJob.getStorageAccessList().contains(EXTERNAL_STORAGE_KEY));

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

        Assertions.assertEquals("import_export_data:customer_loans", MetadataCodec.decodeStringValue(outputAttr));
        Assertions.assertEquals(5, MetadataCodec.decodeIntegerValue(fieldCountAttr));
        Assertions.assertTrue(MetadataCodec.decodeIntegerValue(rowCountAttr) > 0);
        Assertions.assertEquals(1, dataDef.getPartsCount());
        Assertions.assertTrue(dataTag.containsAttrs("trac_create_job"));

        var storageReq = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(dataDef.getStorageId())
                .build();

        var storageTag = metaClient.readObject(storageReq);

        Assertions.assertEquals(ObjectType.STORAGE, storageTag.getDefinition().getObjectType());
        Assertions.assertTrue(storageTag.containsAttrs("trac_create_job"));
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

    @Test @Order(401)
    void importData_missingResource() {

        var orchClient = platform.orchClientBlocking();

        var importData = ImportDataJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(importModelId))
                .putParameters("storage_key", MetadataCodec.encodeValue("STORAGE_THAT_IS_NOT_CONFIGURED"))
                .putParameters("source_file", MetadataCodec.encodeValue("sample_data.parquet"))
                .addStorageAccess("STORAGE_THAT_IS_NOT_CONFIGURED")
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_DATA)
                        .setImportData(importData))
                .build();

        var e = Assertions.assertThrows(StatusRuntimeException.class, () -> orchClient.validateJob(jobRequest));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }
}
