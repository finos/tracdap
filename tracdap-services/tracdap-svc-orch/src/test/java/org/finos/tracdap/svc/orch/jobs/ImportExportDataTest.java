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
import org.finos.tracdap.metadata.ExportDataJob;
import org.finos.tracdap.metadata.ImportDataJob;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ImportExportDataTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";
    private static final String EXTERNAL_STORAGE_KEY = "TEST_EXTERNAL_STORAGE";

    private static final SchemaDefinition PROFIT_SCHEMA = SchemaDefinition.newBuilder()
            .setSchemaType(SchemaType.TABLE)
            .setTable(TableSchema.newBuilder()
                    .addFields(FieldSchema.newBuilder().setFieldName("region").setFieldOrder(0).setFieldType(BasicType.STRING))
                    .addFields(FieldSchema.newBuilder().setFieldName("gross_profit").setFieldOrder(1).setFieldType(BasicType.DECIMAL)))
            .build();

    private static final Map<String, String> PLACEMENT_PATHS = Map.of(
            "profit_csv", "placements/profit_by_region.csv",
            "profit_parquet", "placements/profit_by_region.parquet",
            "profit_arrow", "placements/profit_by_region.arrow");

    private static final Map<String, String> PLACEMENT_FORMATS = Map.of(
            "profit_csv", "CSV",
            "profit_parquet", "PARQUET",
            "profit_arrow", "ARROW_FILE");

    private static final Set<JobStatusCode> COMPLETED_STATES = Set.of(
            JobStatusCode.SUCCEEDED, JobStatusCode.FAILED, JobStatusCode.CANCELLED);

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

    static Path externalStorageDir;
    static TagHeader exportInputDataId;

    static TagHeader jobId_placeData;
    static TagHeader jobId_placeAgain;
    static TagHeader jobId_placeFail;

    static ResultDefinition placeDataResult;

    @Test @Order(101)
    void prepareExternalStorage() throws Exception {

        externalStorageDir = platform.workingDir().resolve("external_storage");
        Files.createDirectories(externalStorageDir);
    }

    @Test @Order(102)
    void prepareExportInput() {

        var csvContent = "region,gross_profit\r\nmunster,1000.50\r\nleinster,2000.75\r\n"
                .getBytes(StandardCharsets.UTF_8);

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(PROFIT_SCHEMA)
                .setFormat("text/csv")
                .setContent(ByteString.copyFrom(csvContent))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_dataset")
                        .setValue(MetadataCodec.encodeValue("import_export_data:profit_by_region")))
                .build();

        exportInputDataId = platform.dataClientBlocking().createSmallDataset(writeRequest);
    }

    @Test @Order(201)
    void placeData() {

        jobId_placeData = startPlacementJob(PlacementConflict.PLACEMENT_CONFLICT_NOT_SET, "place_data");
    }

    @Test @Order(202)
    void placeData_result() throws Exception {

        var jobStatus = waitForCompletion(jobId_placeData);
        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode(), jobStatus.getStatusMessage());

        var jobDef = readObject(MetadataUtil.selectorFor(jobStatus.getJobId())).getDefinition().getJob();
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        // The stored job records where the storage key pointed, without credentials

        for (var placement : jobDef.getExportData().getPlacementsMap().values()) {
            Assertions.assertEquals("LOCAL", placement.getLocation().getProtocol());
            Assertions.assertEquals(Set.of("rootPath"), placement.getLocation().getLocationDetailsMap().keySet());
        }

        // The RESULT records each file placed, matching the bytes on disk

        placeDataResult = readObject(jobDef.getResultId()).getDefinition().getResult();
        var records = placeDataResult.getPlacementsMap();

        Assertions.assertEquals(PLACEMENT_PATHS.keySet(), records.keySet());
        Assertions.assertEquals(0, placeDataResult.getOutputsCount());

        for (var placement : PLACEMENT_PATHS.entrySet()) {

            var record = records.get(placement.getKey());
            var placedFile = externalStorageDir.resolve(placement.getValue());

            Assertions.assertTrue(Files.exists(placedFile), placement.getValue());
            Assertions.assertEquals(placement.getValue(), record.getLocation().getStoragePath());
            Assertions.assertEquals(EXTERNAL_STORAGE_KEY, record.getLocation().getStorageKey());
            Assertions.assertEquals("LOCAL", record.getLocation().getProtocol());
            Assertions.assertEquals(PLACEMENT_FORMATS.get(placement.getKey()), record.getFormat());
            Assertions.assertEquals(Files.size(placedFile), record.getSize());
            Assertions.assertEquals(sha256(placedFile), record.getContentHash());
            Assertions.assertEquals(exportInputDataId.getObjectId(), record.getDataId().getObjectId());
            Assertions.assertEquals(exportInputDataId.getObjectVersion(), record.getDataId().getObjectVersion());
        }

        var csvContent = Files.readString(externalStorageDir.resolve(PLACEMENT_PATHS.get("profit_csv")), StandardCharsets.UTF_8);
        Assertions.assertTrue(csvContent.contains("munster"));
        Assertions.assertTrue(csvContent.contains("leinster"));

        // A placement creates no TRAC objects, the only FILE is the job log

        Assertions.assertEquals(List.of(), searchJobOutputs(ObjectType.DATA, jobKey));
        Assertions.assertEquals(List.of(placeDataResult.getLogFileId().getObjectId()), searchJobOutputs(ObjectType.FILE, jobKey));
    }

    @Test @Order(301)
    void placeAgain_suffix() {

        jobId_placeAgain = startPlacementJob(PlacementConflict.PLACEMENT_SUFFIX, "place_again");

        var jobStatus = waitForCompletion(jobId_placeAgain);
        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode(), jobStatus.getStatusMessage());

        var jobDef = readObject(MetadataUtil.selectorFor(jobStatus.getJobId())).getDefinition().getJob();
        var records = readObject(jobDef.getResultId()).getDefinition().getResult().getPlacementsMap();

        Assertions.assertEquals("placements/profit_by_region-2.csv", records.get("profit_csv").getLocation().getStoragePath());
        Assertions.assertTrue(Files.exists(externalStorageDir.resolve("placements/profit_by_region-2.parquet")));
    }

    @Test @Order(302)
    void placeAgain_fail() throws Exception {

        var filesBefore = listPlacedFiles();

        jobId_placeFail = startPlacementJob(PlacementConflict.PLACEMENT_FAIL, "place_fail");

        var jobStatus = waitForCompletion(jobId_placeFail);
        Assertions.assertEquals(JobStatusCode.FAILED, jobStatus.getStatusCode());
        Assertions.assertTrue(jobStatus.getStatusMessage().contains("File already exists"), jobStatus.getStatusMessage());

        Assertions.assertEquals(filesBefore, listPlacedFiles());
    }

    @Test @Order(401)
    void captureBack() {

        var schemaRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.SCHEMA)
                        .setSchema(PROFIT_SCHEMA))
                .build();

        var schemaId = platform.metaClientBlocking().createObject(schemaRequest);
        var placedParquet = placeDataResult.getPlacementsOrThrow("profit_parquet");

        var capture = CaptureSource.newBuilder()
                .setLocation(ExternalLocation.newBuilder()
                        .setStorageKey(EXTERNAL_STORAGE_KEY)
                        .setStoragePath(placedParquet.getLocation().getStoragePath()))
                .setSchemaSource(CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED)
                .setSchemaId(MetadataUtil.selectorFor(schemaId));

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_DATA)
                        .setImportData(ImportDataJob.newBuilder().putCaptures("profit_by_region", capture.build())))
                .build();

        var captureJobId = Helpers.startJob(platform.orchClientBlocking(), jobRequest).getJobId();
        var jobStatus = waitForCompletion(captureJobId);
        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode(), jobStatus.getStatusMessage());

        var captureJob = readObject(MetadataUtil.selectorFor(captureJobId)).getDefinition().getJob();
        var captureResult = readObject(captureJob.getResultId()).getDefinition().getResult();
        var capturedFile = readObject(captureResult.getOutputsOrThrow("profit_by_region_file"));

        var capturedHash = MetadataCodec.decodeStringValue(capturedFile.getAttrsOrThrow("trac_import_content_hash"));
        Assertions.assertEquals(placedParquet.getContentHash(), capturedHash);
    }

    private TagHeader startPlacementJob(PlacementConflict conflict, String jobLabel) {

        var exportData = ExportDataJob.newBuilder().setPlacementConflict(conflict);

        for (var placement : PLACEMENT_PATHS.entrySet()) {
            exportData.putPlacements(placement.getKey(), PlacementTarget.newBuilder()
                    .setDataId(MetadataUtil.selectorFor(exportInputDataId))
                    .setLocation(ExternalLocation.newBuilder()
                            .setStorageKey(EXTERNAL_STORAGE_KEY)
                            .setStoragePath(placement.getValue()))
                    .build());
        }

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.EXPORT_DATA)
                        .setExportData(exportData))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("import_export_data:" + jobLabel)))
                .build();

        return Helpers.startJob(platform.orchClientBlocking(), jobRequest).getJobId();
    }

    private static JobStatus waitForCompletion(TagHeader jobId) {

        var statusRequest = JobStatusRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(jobId))
                .build();

        var jobStatus = platform.orchClientBlocking().checkJob(statusRequest);

        while (!COMPLETED_STATES.contains(jobStatus.getStatusCode())) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            jobStatus = platform.orchClientBlocking().checkJob(statusRequest);
        }

        return jobStatus;
    }

    private static org.finos.tracdap.metadata.Tag readObject(TagSelector selector) {

        return platform.metaClientBlocking().readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selector)
                .build());
    }

    private static List<String> searchJobOutputs(ObjectType objectType, String jobKey) {

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                        .setObjectType(objectType)
                        .setSearch(SearchExpression.newBuilder()
                                .setTerm(SearchTerm.newBuilder()
                                        .setAttrName("trac_create_job")
                                        .setAttrType(BasicType.STRING)
                                        .setOperator(SearchOperator.EQ)
                                        .setSearchValue(MetadataCodec.encodeValue(jobKey)))))
                .build();

        return platform.metaClientBlocking().search(searchRequest).getSearchResultList().stream()
                .map(tag -> tag.getHeader().getObjectId())
                .collect(java.util.stream.Collectors.toList());
    }

    private static List<String> listPlacedFiles() throws Exception {

        try (var files = Files.list(externalStorageDir.resolve("placements"))) {
            return files.map(f -> f.getFileName().toString()).sorted().collect(java.util.stream.Collectors.toList());
        }
    }

    private static String sha256(Path path) throws Exception {

        var digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path));
        return String.format("%064x", new BigInteger(1, digest));
    }
}
