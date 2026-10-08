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
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.helpers.PlatformTest;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CaptureDataTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";
    private static final String EXTERNAL_STORAGE_KEY = "TEST_EXTERNAL_STORAGE";

    private static final String PARQUET_SOURCE = "examples/models/python/data/inputs/staging/sample_data.parquet";
    private static final String CSV_SOURCE = "examples/models/python/data/inputs/capture/currency_data_2026-09-30.csv";

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
    static TagHeader csvSchemaId;
    static TagHeader parquetSchemaId;

    static TagHeader jobId_capture;
    static TagHeader jobId_missingFile;
    static TagHeader jobId_overLimit;

    @Test @Order(101)
    void prepareExternalStorage() throws Exception {

        externalStorageDir = platform.workingDir().resolve("external_storage");
        Files.createDirectories(externalStorageDir.resolve("2026-09"));

        Files.copy(
                platform.tracRepoDir().resolve(CSV_SOURCE),
                externalStorageDir.resolve("2026-09/currency_data_2026-09-30.csv"),
                StandardCopyOption.REPLACE_EXISTING);

        Files.copy(
                platform.tracRepoDir().resolve(PARQUET_SOURCE),
                externalStorageDir.resolve("sample_data.parquet"),
                StandardCopyOption.REPLACE_EXISTING);

        // Larger than the 1 MB captureMaxSize set for the executor in the e2e platform config
        var bigContent = "ccy_code\n" + "EUR\n".repeat(300_000);
        Files.writeString(externalStorageDir.resolve("too_big.csv"), bigContent);
    }

    @Test @Order(102)
    void createSchemas() {

        var csvSchema = tableSchema(
                Map.entry("ccy_code", BasicType.STRING),
                Map.entry("spot_date", BasicType.DATE),
                Map.entry("dollar_rate", BasicType.DECIMAL));

        var parquetSchema = tableSchema(
                Map.entry("id", BasicType.STRING),
                Map.entry("loan_amount", BasicType.DECIMAL),
                Map.entry("region", BasicType.STRING));

        csvSchemaId = createSchema(csvSchema);
        parquetSchemaId = createSchema(parquetSchema);
    }

    @Test @Order(201)
    void captureJob() {

        var importData = ImportDataJob.newBuilder()
                .putCaptures("currency", CaptureSource.newBuilder()
                        .setLocation(location("2026-09/currency_data_2026-09-30.csv"))
                        .setSchemaSource(CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED)
                        .setSchemaId(MetadataUtil.selectorForLatest(csvSchemaId))
                        .addDataAttrs(attr("e2e_test_name", "per capture"))
                        .addFileAttrs(attr("e2e_test_file", "currency file"))
                        .build())
                .putCaptures("loans_declared", CaptureSource.newBuilder()
                        .setLocation(location("sample_data.parquet"))
                        .setSchemaSource(CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED)
                        .setSchemaId(MetadataUtil.selectorFor(parquetSchemaId))
                        .build())
                .putCaptures("loans_file", CaptureSource.newBuilder()
                        .setLocation(location("sample_data.parquet"))
                        .setSchemaSource(CaptureSchemaSource.CAPTURE_SCHEMA_FILE)
                        .build())
                .addOutputAttrs(attr("e2e_test_name", "job level"))
                .addOutputAttrs(attr("e2e_test_data", "capture_data:capture_job"));

        jobId_capture = startJob(importData);
    }

    @Test @Order(202)
    void captureJob_result() throws Exception {

        var jobStatus = Helpers.waitForJob(platform.orchClientBlocking(), TEST_TENANT, jobId_capture);
        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        // The stored job records where each storage key pointed, without credentials

        var jobObj = platform.metaClientBlocking().readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(jobStatus.getJobId()))
                .build());

        for (var capture : jobObj.getDefinition().getJob().getImportData().getCapturesMap().values()) {

            var recordedLocation = capture.getLocation();

            Assertions.assertEquals(EXTERNAL_STORAGE_KEY, recordedLocation.getStorageKey());
            Assertions.assertEquals("LOCAL", recordedLocation.getProtocol());
            Assertions.assertEquals(Set.of("rootPath"), recordedLocation.getLocationDetailsMap().keySet());
            Assertions.assertEquals(
                    externalStorageDir.toAbsolutePath().normalize(),
                    Path.of(recordedLocation.getLocationDetailsOrThrow("rootPath")).toAbsolutePath().normalize());
        }

        var datasets = outputsByName(ObjectType.DATA, jobKey);
        var files = outputsByName(ObjectType.FILE, jobKey);

        Assertions.assertEquals(Set.of("currency", "loans_declared", "loans_file"), datasets.keySet());
        Assertions.assertEquals(Set.of("currency_file", "loans_declared_file", "loans_file_file"), files.keySet());

        // FILE definitions are the source files, unchanged

        var csvPath = externalStorageDir.resolve("2026-09/currency_data_2026-09-30.csv");
        var parquetPath = externalStorageDir.resolve("sample_data.parquet");

        var currencyFile = files.get("currency_file");
        var currencyFileDef = currencyFile.getDefinition().getFile();
        Assertions.assertEquals("currency_data_2026-09-30.csv", currencyFileDef.getName());
        Assertions.assertEquals("csv", currencyFileDef.getExtension());
        Assertions.assertEquals("text/csv", currencyFileDef.getMimeType());
        Assertions.assertEquals(Files.size(csvPath), currencyFileDef.getSize());

        var parquetFileDef = files.get("loans_file_file").getDefinition().getFile();
        Assertions.assertEquals("sample_data.parquet", parquetFileDef.getName());
        Assertions.assertEquals("application/vnd.apache.parquet", parquetFileDef.getMimeType());
        Assertions.assertEquals(Files.size(parquetPath), parquetFileDef.getSize());

        // DATA schema form: pinned schema ID when declared, inline when taken from the file

        var currencyData = datasets.get("currency").getDefinition().getData();
        Assertions.assertTrue(currencyData.hasSchemaId());
        Assertions.assertFalse(currencyData.hasSchema());
        Assertions.assertEquals(csvSchemaId.getObjectId(), currencyData.getSchemaId().getObjectId());
        Assertions.assertEquals(csvSchemaId.getObjectVersion(), currencyData.getSchemaId().getObjectVersion());

        var loansFileData = datasets.get("loans_file").getDefinition().getData();
        Assertions.assertTrue(loansFileData.hasSchema());
        Assertions.assertEquals(5, loansFileData.getSchema().getTable().getFieldsCount());

        // Provenance on both objects

        var currency = datasets.get("currency");
        var csvHash = sha256(csvPath);

        for (var obj : List.of(currency, currencyFile)) {
            Assertions.assertEquals(EXTERNAL_STORAGE_KEY, stringAttr(obj, "trac_import_location_key"));
            Assertions.assertEquals("2026-09/currency_data_2026-09-30.csv", stringAttr(obj, "trac_import_location_path"));
            Assertions.assertEquals("currency_data_2026-09-30.csv", stringAttr(obj, "trac_import_file_name"));
            Assertions.assertEquals(Files.size(csvPath), MetadataCodec.decodeIntegerValue(obj.getAttrsOrThrow("trac_import_file_size")));
            Assertions.assertEquals(csvHash, stringAttr(obj, "trac_import_content_hash"));
            Assertions.assertTrue(obj.containsAttrs("trac_import_file_modified"));
            Assertions.assertEquals(JobType.IMPORT_DATA.name(), stringAttr(obj, "trac_job_type"));
        }

        Assertions.assertEquals(currencyFile.getHeader().getObjectId(), stringAttr(currency, "trac_capture_file"));
        Assertions.assertEquals("DECLARED", stringAttr(currency, "trac_import_schema_source"));
        Assertions.assertEquals(4, MetadataCodec.decodeIntegerValue(currency.getAttrsOrThrow("trac_import_source_columns")));
        Assertions.assertEquals(3, MetadataCodec.decodeIntegerValue(currency.getAttrsOrThrow("trac_schema_field_count")));
        Assertions.assertEquals("FILE", stringAttr(datasets.get("loans_file"), "trac_import_schema_source"));

        // User attributes: per-capture attributes override job output attributes

        Assertions.assertEquals("per capture", stringAttr(currency, "e2e_test_name"));
        Assertions.assertEquals("job level", stringAttr(currencyFile, "e2e_test_name"));
        Assertions.assertEquals("currency file", stringAttr(currencyFile, "e2e_test_file"));
        Assertions.assertFalse(currency.containsAttrs("e2e_test_file"));
        Assertions.assertEquals("capture_data:capture_job", stringAttr(datasets.get("loans_declared"), "e2e_test_data"));
        Assertions.assertEquals("currency", stringAttr(currency, "trac_job_output"));
        Assertions.assertEquals("currency_file", stringAttr(currencyFile, "trac_job_output"));

        // STORAGE objects carry only storage and job attributes

        var storageIds = new ArrayList<TagSelector>();
        datasets.values().forEach(obj -> storageIds.add(obj.getDefinition().getData().getStorageId()));
        files.values().forEach(obj -> storageIds.add(obj.getDefinition().getFile().getStorageId()));

        for (var storageId : storageIds) {

            var storageObj = platform.metaClientBlocking().readObject(MetadataReadRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSelector(storageId)
                    .build());

            Assertions.assertTrue(storageObj.containsAttrs("trac_storage_object"));
            Assertions.assertTrue(storageObj.containsAttrs("trac_create_job"));
            Assertions.assertFalse(storageObj.containsAttrs("trac_import_location_key"));
            Assertions.assertFalse(storageObj.containsAttrs("trac_import_content_hash"));
            Assertions.assertFalse(storageObj.containsAttrs("e2e_test_name"));
        }
    }

    @Test @Order(301)
    void missingFile() {

        var importData = ImportDataJob.newBuilder()
                .putCaptures("missing", CaptureSource.newBuilder()
                        .setLocation(location("not_there.parquet"))
                        .setSchemaSource(CaptureSchemaSource.CAPTURE_SCHEMA_FILE)
                        .build());

        jobId_missingFile = startJob(importData);
    }

    @Test @Order(302)
    void missingFile_result() {

        var jobStatus = waitForCompletion(jobId_missingFile);
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        Assertions.assertEquals(JobStatusCode.FAILED, jobStatus.getStatusCode());
        Assertions.assertTrue(jobStatus.getStatusMessage().contains("not_there.parquet"), jobStatus.getStatusMessage());

        Assertions.assertEquals(0, searchJobOutputs(ObjectType.DATA, jobKey).size());
        Assertions.assertEquals(0, outputsByName(ObjectType.FILE, jobKey).size());
    }

    @Test @Order(303)
    void overSizeLimit() {

        var importData = ImportDataJob.newBuilder()
                .putCaptures("too_big", CaptureSource.newBuilder()
                        .setLocation(location("too_big.csv"))
                        .setSchemaSource(CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED)
                        .setSchemaId(MetadataUtil.selectorFor(csvSchemaId))
                        .build());

        jobId_overLimit = startJob(importData);
    }

    @Test @Order(304)
    void overSizeLimit_result() {

        var jobStatus = waitForCompletion(jobId_overLimit);
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        Assertions.assertEquals(JobStatusCode.FAILED, jobStatus.getStatusCode());
        Assertions.assertTrue(jobStatus.getStatusMessage().contains("capture size limit"), jobStatus.getStatusMessage());
        Assertions.assertTrue(jobStatus.getStatusMessage().contains("[captureMaxSize]"), jobStatus.getStatusMessage());

        Assertions.assertEquals(0, searchJobOutputs(ObjectType.DATA, jobKey).size());
        Assertions.assertEquals(0, outputsByName(ObjectType.FILE, jobKey).size());
    }

    private static SchemaDefinition tableSchema(Map.Entry<String, BasicType>... fields) {

        var table = TableSchema.newBuilder();

        for (var i = 0; i < fields.length; i++) {
            table.addFields(FieldSchema.newBuilder()
                    .setFieldName(fields[i].getKey())
                    .setFieldOrder(i)
                    .setFieldType(fields[i].getValue()));
        }

        return SchemaDefinition.newBuilder()
                .setSchemaType(SchemaType.TABLE)
                .setPartType(PartType.PART_ROOT)
                .setTable(table)
                .build();
    }

    private static TagHeader createSchema(SchemaDefinition schema) {

        var request = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.SCHEMA)
                        .setSchema(schema))
                .build();

        return platform.metaClientBlocking().createObject(request);
    }

    private static ExternalLocation location(String storagePath) {

        return ExternalLocation.newBuilder()
                .setStorageKey(EXTERNAL_STORAGE_KEY)
                .setStoragePath(storagePath)
                .build();
    }

    private static TagUpdate attr(String attrName, String value) {

        return TagUpdate.newBuilder()
                .setAttrName(attrName)
                .setValue(MetadataCodec.encodeValue(value))
                .build();
    }

    private static TagHeader startJob(ImportDataJob.Builder importData) {

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_DATA)
                        .setImportData(importData))
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

    private static List<org.finos.tracdap.metadata.Tag> searchJobOutputs(ObjectType objectType, String jobKey) {

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

        var metaClient = platform.metaClientBlocking();

        return metaClient.search(searchRequest).getSearchResultList().stream()
                .map(result -> metaClient.readObject(MetadataReadRequest.newBuilder()
                        .setTenant(TEST_TENANT)
                        .setSelector(MetadataUtil.selectorFor(result.getHeader()))
                        .build()))
                .collect(Collectors.toList());
    }

    private static Map<String, org.finos.tracdap.metadata.Tag> outputsByName(ObjectType objectType, String jobKey) {

        return searchJobOutputs(objectType, jobKey).stream()
                .filter(tag -> tag.containsAttrs("trac_job_output"))
                .collect(Collectors.toMap(tag -> stringAttr(tag, "trac_job_output"), tag -> tag));
    }

    private static String stringAttr(org.finos.tracdap.metadata.Tag tag, String attrName) {
        return MetadataCodec.decodeStringValue(tag.getAttrsOrThrow(attrName));
    }

    private static String sha256(Path path) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path));
        return String.format("%064x", new BigInteger(1, digest));
    }
}
