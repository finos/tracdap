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
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.metadata.ExportDataJob;
import org.finos.tracdap.metadata.ImportDataJob;
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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;


/**
 * Proves {@code JobProcessorHelpers.translateResourceSecrets} against real cloud storage, for all
 * three cloud storage plugins (S3, Azure Blob, GCS). {@code SimpleCsvExport}/{@code SimpleCsvImport}
 * ({@code tutorial.data_export}/{@code tutorial.data_import}) are a genuine matched round-trip pair -
 * same schema ({@code profit_by_region.csv}), same CSV format on both sides - unlike
 * {@code DataExportExample}/{@code SimpleDataImport}, which share neither.
 * <p>
 * Two scenarios per cloud: a full export/import round trip using {@code default} credentials, and
 * (S3/Azure only - GCS has no credential-mode property, see {@code GcsObjectStorage}) an export
 * using a deliberately incorrect {@code static}/{@code account_key} secret, asserting the job fails.
 * The second scenario is an interim proof that secret aliasing and presentation to the runtime work
 * correctly without depending on a real static credential existing anywhere; a real-secret pass is
 * left to manual UAT.
 * <p>
 * Requires real cloud credentials via environment variables; each cloud's tests are skipped if its
 * variables aren't set (see {@code Assumptions.assumeTrue} below for the exact variable names).
 * <p>
 * Tagged {@code all-platforms} (not a single {@code aws-platform}/{@code azure-platform}/
 * {@code gcp-platform}) because one class covers all three clouds - each CI matrix row
 * (aws/azure/gcp) runs the whole class once, and only that row's own scenarios execute for real;
 * the other clouds' {@code Assumptions.assumeTrue} calls skip, since only one cloud is
 * authenticated per job.
 */
@Tag("integration")
@Tag("int-cloud-storage")
@Tag("all-platforms")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ImportExportCloudStorageTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";

    private static final String TRAC_TEST_ID = System.getenv().getOrDefault("TRAC_TEST_ID", "local");

    private static final String INPUT_CSV = "region,gross_profit\r\nmunster,1000.50\r\nleinster,2000.75\r\n";

    private static final List<String[]> INPUT_ROWS = List.of(
            new String[] {"munster", "1000.50"},
            new String[] {"leinster", "2000.75"});

    private static final List<JobStatusCode> COMPLETED_JOB_STATES = List.of(
            JobStatusCode.SUCCEEDED, JobStatusCode.FAILED, JobStatusCode.CANCELLED);

    // Not real - used only to prove the secret-alias/auth-failure path. Deliberately not shaped
    // like a real AWS access key (which always has a recognised 4-letter prefix, e.g. AKIA/ASIA,
    // followed by 16 more characters), and the Azure key is computed rather than a literal -
    // GitHub's push protection flags both an AKIA-prefixed string and a bare base64 blob of this
    // length as likely-leaked credentials regardless of whether they're genuine.
    private static final String FAKE_AWS_ACCESS_KEY_ID = "NOTAREALACCESSKEYID0";
    private static final String FAKE_AWS_SECRET_ACCESS_KEY = "fakeFAKEfakeFAKEfakeFAKEfakeFAKEfakeFAKE";
    private static final String FAKE_AZURE_ACCOUNT_KEY = java.util.Base64.getEncoder().encodeToString(
            "not-a-real-azure-account-key-value".getBytes(StandardCharsets.UTF_8));

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

    static TagHeader exportModelId;
    static TagHeader importModelId;
    static SchemaDefinition exportInputSchema;
    static TagHeader exportInputDataId;

    @Test @Order(101)
    void exportModelImport() throws Exception {

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("TRAC_LOCAL_REPO")
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.data_export.SimpleCsvExport")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("import_export_cloud_storage:simple_csv_export"))
                .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("import_export_cloud_storage:import_simple_csv_export"))
                .build());

        var jobId = Helpers.startModelImport(platform, TEST_TENANT, modelStub, modelAttrs, jobAttrs);
        var modelTag = Helpers.waitForModelImport(platform, TEST_TENANT, jobId);
        var modelDef = modelTag.getDefinition().getModel();

        Assertions.assertEquals(ModelType.DATA_EXPORT_MODEL, modelDef.getModelType());

        exportModelId = modelTag.getHeader();
        exportInputSchema = modelDef.getInputsOrThrow("dataset").getSchema();
    }

    @Test @Order(102)
    void importModelImport() throws Exception {

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("TRAC_LOCAL_REPO")
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.data_import.SimpleCsvImport")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("import_export_cloud_storage:simple_csv_import"))
                .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("import_export_cloud_storage:import_simple_csv_import"))
                .build());

        var jobId = Helpers.startModelImport(platform, TEST_TENANT, modelStub, modelAttrs, jobAttrs);
        var modelTag = Helpers.waitForModelImport(platform, TEST_TENANT, jobId);
        var modelDef = modelTag.getDefinition().getModel();

        Assertions.assertEquals(ModelType.DATA_IMPORT_MODEL, modelDef.getModelType());

        importModelId = modelTag.getHeader();
    }

    @Test @Order(103)
    void prepareExportInput() {

        var dataClient = platform.dataClientBlocking();

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(exportInputSchema)
                .setFormat("text/csv")
                .setContent(ByteString.copyFrom(INPUT_CSV.getBytes(StandardCharsets.UTF_8)))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_dataset")
                        .setValue(MetadataCodec.encodeValue("import_export_cloud_storage:profit_by_region")))
                .build();

        exportInputDataId = dataClient.createSmallDataset(writeRequest);
    }

    // --- AWS -------------------------------------------------------------------------------

    @Test @Order(201)
    void awsDefaultRoundTrip() {

        var region = System.getenv("TRAC_AWS_REGION");
        var bucket = System.getenv("TRAC_AWS_BUCKET");

        Assumptions.assumeTrue(
                region != null && bucket != null,
                "TRAC_AWS_REGION / TRAC_AWS_BUCKET are not set");

        var resource = ResourceDefinition.newBuilder()
                .setResourceType(ResourceType.EXTERNAL_STORAGE)
                .setProtocol("S3")
                .putProperties("bucket", bucket)
                .putProperties("region", region)
                .putProperties("prefix", "int_cloud_storage_" + TRAC_TEST_ID + "/")
                .putProperties("credentials", "default")
                .build();

        roundTrip("AWS_DEFAULT", resource, "aws_default");
    }

    @Test @Order(202)
    void awsStaticFakeSecret() {

        var region = System.getenv("TRAC_AWS_REGION");
        var bucket = System.getenv("TRAC_AWS_BUCKET");

        Assumptions.assumeTrue(
                region != null && bucket != null,
                "TRAC_AWS_REGION / TRAC_AWS_BUCKET are not set");

        var resource = ResourceDefinition.newBuilder()
                .setResourceType(ResourceType.EXTERNAL_STORAGE)
                .setProtocol("S3")
                .putProperties("bucket", bucket)
                .putProperties("region", region)
                .putProperties("prefix", "int_cloud_storage_" + TRAC_TEST_ID + "/")
                .putProperties("credentials", "static")
                .putSecrets("accessKeyId", FAKE_AWS_ACCESS_KEY_ID)
                .putSecrets("secretAccessKey", FAKE_AWS_SECRET_ACCESS_KEY)
                .build();

        expectAuthFailure("AWS_STATIC_FAKE", resource, "aws_static_fake");
    }

    // --- Azure -----------------------------------------------------------------------------

    @Test @Order(301)
    void azureDefaultRoundTrip() {

        var storageAccount = System.getenv("TRAC_AZURE_STORAGE_ACCOUNT");
        var container = System.getenv("TRAC_AZURE_CONTAINER");

        Assumptions.assumeTrue(
                storageAccount != null && container != null,
                "TRAC_AZURE_STORAGE_ACCOUNT / TRAC_AZURE_CONTAINER are not set");

        var resource = ResourceDefinition.newBuilder()
                .setResourceType(ResourceType.EXTERNAL_STORAGE)
                .setProtocol("BLOB")
                .putProperties("storageAccount", storageAccount)
                .putProperties("container", container)
                .putProperties("prefix", "int_cloud_storage_" + TRAC_TEST_ID + "/")
                .putProperties("credentials", "default")
                .build();

        roundTrip("AZURE_DEFAULT", resource, "azure_default");
    }

    @Test @Order(302)
    void azureStaticFakeSecret() {

        var storageAccount = System.getenv("TRAC_AZURE_STORAGE_ACCOUNT");
        var container = System.getenv("TRAC_AZURE_CONTAINER");

        Assumptions.assumeTrue(
                storageAccount != null && container != null,
                "TRAC_AZURE_STORAGE_ACCOUNT / TRAC_AZURE_CONTAINER are not set");

        var resource = ResourceDefinition.newBuilder()
                .setResourceType(ResourceType.EXTERNAL_STORAGE)
                .setProtocol("BLOB")
                .putProperties("storageAccount", storageAccount)
                .putProperties("container", container)
                .putProperties("prefix", "int_cloud_storage_" + TRAC_TEST_ID + "/")
                .putProperties("credentials", "account_key")
                .putSecrets("accountKey", FAKE_AZURE_ACCOUNT_KEY)
                .build();

        expectAuthFailure("AZURE_STATIC_FAKE", resource, "azure_static_fake");
    }

    // --- GCP ---------------------------------------------------------------------------------
    // Default only - the GCS plugin has no credentials property at all (GcsObjectStorage only
    // takes region/project/bucket/prefix), so there is no static-credential scenario to run.

    @Test @Order(401)
    void gcpDefaultRoundTrip() {

        // Region is optional in both the Java and Python GCS plugins (GcsObjectStorage never reads
        // REGION_PROPERTY; storage_gcp.py only sets it if present) - finos/tracdap's own CI does not
        // set TRAC_GCP_REGION, only TRAC_GCP_PROJECT/TRAC_GCP_BUCKET, so this doesn't gate on it.
        var region = System.getenv("TRAC_GCP_REGION");
        var project = System.getenv("TRAC_GCP_PROJECT");
        var bucket = System.getenv("TRAC_GCP_BUCKET");

        Assumptions.assumeTrue(
                project != null && bucket != null,
                "TRAC_GCP_PROJECT / TRAC_GCP_BUCKET are not set");

        var resourceBuilder = ResourceDefinition.newBuilder()
                .setResourceType(ResourceType.EXTERNAL_STORAGE)
                .setProtocol("GCS")
                .putProperties("bucket", bucket)
                .putProperties("project", project)
                .putProperties("prefix", "int_cloud_storage_" + TRAC_TEST_ID + "/");

        if (region != null)
            resourceBuilder.putProperties("region", region);

        roundTrip("GCP_DEFAULT", resourceBuilder.build(), "gcp_default");
    }

    // --- Shared mechanics --------------------------------------------------------------------

    private void roundTrip(String storageKey, ResourceDefinition resource, String label) {

        registerStorageResource(storageKey, resource);

        var exportFile = label + "/profit_by_region.csv";

        var exportJobId = submitExportJob(storageKey, exportFile, "import_export_cloud_storage:" + label + "_export");
        var exportStatus = Helpers.waitForJob(platform.orchClientBlocking(), TEST_TENANT, exportJobId);
        Assertions.assertEquals(JobStatusCode.SUCCEEDED, exportStatus.getStatusCode());

        var importJobId = submitImportJob(storageKey, exportFile, "import_export_cloud_storage:" + label + "_import");
        var importStatus = Helpers.waitForJob(platform.orchClientBlocking(), TEST_TENANT, importJobId);
        Assertions.assertEquals(JobStatusCode.SUCCEEDED, importStatus.getStatusCode());

        var outputDataId = findOutputDataset(importJobId);
        assertDatasetMatchesInput(outputDataId);
    }

    private void expectAuthFailure(String storageKey, ResourceDefinition resource, String label) {

        registerStorageResource(storageKey, resource);

        var exportFile = label + "/profit_by_region.csv";
        var exportJobId = submitExportJob(storageKey, exportFile, "import_export_cloud_storage:" + label + "_export");

        var jobStatus = pollJobStatus(exportJobId);

        Assertions.assertEquals(JobStatusCode.FAILED, jobStatus.getStatusCode());

        // Assert on the failure being an auth rejection specifically (EStorageAccess, raised from
        // CommonFileStorage's generic PermissionError handler), not just any failure - a config
        // typo would also produce JobStatusCode.FAILED, and would not prove secret resolution works.
        var statusMessage = jobStatus.getStatusMessage().toLowerCase();
        Assertions.assertTrue(
                statusMessage.contains("access denied") || statusMessage.contains("permission"),
                () -> "Expected an access-denied failure, got: " + jobStatus.getStatusMessage());
    }

    private void registerStorageResource(String storageKey, ResourceDefinition resource) {

        var definition = ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.RESOURCE)
                .setResource(resource)
                .build();

        var writeRequest = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass(ConfigKeys.TRAC_RESOURCES)
                .setConfigKey(storageKey)
                .setDefinition(definition)
                .build();

        platform.adminClientBlocking().createConfigObject(writeRequest);
    }

    private TagHeader submitExportJob(String storageKey, String exportFile, String jobAttrValue) {

        var orchClient = platform.orchClientBlocking();

        var exportData = ExportDataJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(exportModelId))
                .putParameters("storage_key", MetadataCodec.encodeValue(storageKey))
                .putParameters("export_file", MetadataCodec.encodeValue(exportFile))
                .putInputs("dataset", MetadataUtil.selectorFor(exportInputDataId))
                .addStorageAccess(storageKey)
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.EXPORT_DATA)
                        .setExportData(exportData))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue(jobAttrValue)))
                .build();

        return Helpers.startJob(orchClient, jobRequest).getJobId();
    }

    private TagHeader submitImportJob(String storageKey, String sourceFile, String jobAttrValue) {

        var orchClient = platform.orchClientBlocking();

        var importData = ImportDataJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(importModelId))
                .putParameters("storage_key", MetadataCodec.encodeValue(storageKey))
                .putParameters("source_file", MetadataCodec.encodeValue(sourceFile))
                .addStorageAccess(storageKey)
                .addOutputAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_data")
                        .setValue(MetadataCodec.encodeValue(jobAttrValue)))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_DATA)
                        .setImportData(importData))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue(jobAttrValue)))
                .build();

        return Helpers.startJob(orchClient, jobRequest).getJobId();
    }

    private JobStatus pollJobStatus(TagHeader jobId) {

        var orchClient = platform.orchClientBlocking();
        var statusRequest = JobStatusRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(jobId))
                .build();

        var jobStatus = orchClient.checkJob(statusRequest);

        while (!COMPLETED_JOB_STATES.contains(jobStatus.getStatusCode())) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            jobStatus = orchClient.checkJob(statusRequest);
        }

        return jobStatus;
    }

    private TagHeader findOutputDataset(TagHeader jobId) {

        var metaClient = platform.metaClientBlocking();
        var jobKey = MetadataUtil.objectKey(jobId);

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

        return dataSearchResult.getSearchResult(0).getHeader();
    }

    private void assertDatasetMatchesInput(TagHeader dataId) {

        var dataClient = platform.dataClientBlocking();

        var readRequest = DataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(dataId))
                .setFormat("text/csv")
                .build();

        var readResponse = dataClient.readSmallDataset(readRequest);
        var rows = parseCsvRows(readResponse.getContent().toStringUtf8());

        Assertions.assertEquals(INPUT_ROWS.size(), rows.size());

        for (var i = 0; i < INPUT_ROWS.size(); i++) {
            Assertions.assertEquals(INPUT_ROWS.get(i)[0], rows.get(i)[0]);
            Assertions.assertEquals(new BigDecimal(INPUT_ROWS.get(i)[1]), new BigDecimal(rows.get(i)[1]));
        }
    }

    private static List<String[]> parseCsvRows(String csvContent) {

        var lines = csvContent.strip().split("\r?\n");

        return Arrays.stream(lines)
                .skip(1)  // header row
                .map(line -> line.split(","))
                .collect(Collectors.toList());
    }
}
