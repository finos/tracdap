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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
 * three cloud storage plugins (S3, Azure Blob, GCS), using {@code default} credentials.
 * {@code SimpleCsvExport}/{@code SimpleCsvImport} ({@code tutorial.data_export}/
 * {@code tutorial.data_import}) are a genuine matched round-trip pair - same schema
 * ({@code profit_by_region.csv}), same CSV format on both sides - unlike
 * {@code DataExportExample}/{@code SimpleDataImport}, which share neither.
 * <p>
 * Deliberately does not attempt a {@code static}/{@code account_key} scenario with a made-up
 * secret: PyArrow's native S3/Azure filesystems always raise a plain {@code OSError} for an auth
 * rejection (confirmed directly against both real endpoints), never Python's {@code PermissionError}
 * subclass, so the runtime's own error mapping can't distinguish "wrong secret" from any other
 * storage-layer failure - a fake-secret job failing proves nothing about *why* it failed. Proving
 * secret resolution itself (the alias reaching the runtime as plaintext) doesn't need a real cloud
 * call at all; proving a real static credential authenticates needs one that doesn't exist yet.
 * Both are left for a follow-up once either exists, rather than landing a test whose pass/fail
 * doesn't mean what it looks like it means.
 * <p>
 * Requires real cloud credentials via environment variables; each cloud's tests are skipped if its
 * variables aren't set (see {@code Assumptions.assumeTrue} below for the exact variable names).
 * <p>
 * Tagged {@code all-platforms} (not a single {@code aws-platform}/{@code azure-platform}/
 * {@code gcp-platform}) because one class covers all three clouds - each CI matrix row
 * (aws/azure/gcp) runs the whole class once, and only that row's own scenario executes for real;
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

    private static final int RESOURCE_PROPAGATION_RETRIES = 10;
    private static final long RESOURCE_PROPAGATION_RETRY_DELAY_MS = 200;

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
                targetMatches("aws") && region != null && bucket != null,
                "TRAC_AWS_REGION / TRAC_AWS_BUCKET are not set, or TRAC_CLOUD_TARGET is a different cloud");

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

    // --- Azure -----------------------------------------------------------------------------

    @Test @Order(301)
    void azureDefaultRoundTrip() {

        var storageAccount = System.getenv("TRAC_AZURE_STORAGE_ACCOUNT");
        var container = System.getenv("TRAC_AZURE_CONTAINER");

        Assumptions.assumeTrue(
                targetMatches("azure") && storageAccount != null && container != null,
                "TRAC_AZURE_STORAGE_ACCOUNT / TRAC_AZURE_CONTAINER are not set, or TRAC_CLOUD_TARGET is a different cloud");

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

    // --- GCP ---------------------------------------------------------------------------------
    // Default only - no static-credential scenario for any cloud here (see class doc comment).

    @Test @Order(401)
    void gcpDefaultRoundTrip() {

        // Region is optional in both the Java and Python GCS plugins (GcsObjectStorage never reads
        // REGION_PROPERTY; storage_gcp.py only sets it if present) - finos/tracdap's own CI does not
        // set TRAC_GCP_REGION, only TRAC_GCP_PROJECT/TRAC_GCP_BUCKET, so this doesn't gate on it.
        var region = System.getenv("TRAC_GCP_REGION");
        var project = System.getenv("TRAC_GCP_PROJECT");
        var bucket = System.getenv("TRAC_GCP_BUCKET");

        Assumptions.assumeTrue(
                targetMatches("gcp") && project != null && bucket != null,
                "TRAC_GCP_PROJECT / TRAC_GCP_BUCKET are not set, or TRAC_CLOUD_TARGET is a different cloud");

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

        return submitJobAwaitingResource(orchClient, jobRequest, storageKey);
    }

    // registerStorageResource() always precedes this call, but NotifierService.configUpdate()
    // (tracdap-svc-admin) pushes the new resource to the orchestrator asynchronously, fire-and-
    // forget - retry briefly on FAILED_PRECONDITION for the resource just registered, rather than
    // assume it's already visible.
    private TagHeader submitJobAwaitingResource(
            TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClient,
            JobRequest jobRequest, String storageKey) {

        var notYetVisible = "Required resource [" + storageKey + "] not available";

        for (var attempt = 1; ; attempt++) {

            try {
                return Helpers.startJob(orchClient, jobRequest).getJobId();
            }
            catch (StatusRuntimeException e) {

                var description = e.getStatus().getDescription();
                var isResourcePropagationDelay = e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION
                        && description != null && description.contains(notYetVisible);

                if (!isResourcePropagationDelay || attempt >= RESOURCE_PROPAGATION_RETRIES)
                    throw e;

                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(RESOURCE_PROPAGATION_RETRY_DELAY_MS));
            }
        }
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

            // Value equality, not BigDecimal.equals() - DECIMAL fields round-trip through Arrow's
            // decimal128(38, 12) (DataMapping.DEFAULT_DECIMAL_SCALE), so 1000.50 legitimately comes
            // back as 1000.500000000000: same number, different scale.
            var expected = new BigDecimal(INPUT_ROWS.get(i)[1]);
            var actual = new BigDecimal(rows.get(i)[1]);
            Assertions.assertEquals(0, expected.compareTo(actual), () -> "Expected " + expected + " but was " + actual);
        }
    }

    private static List<String[]> parseCsvRows(String csvContent) {

        var lines = csvContent.strip().split("\r?\n");

        return Arrays.stream(lines)
                .skip(1)  // header row
                .map(line -> line.split(","))
                .collect(Collectors.toList());
    }

    // CI sets TRAC_CLOUD_TARGET so each end-to-end-cloud-* job only attempts its own cloud - the
    // bucket/region env vars alone are not enough to tell, since env: ${{ vars }} exposes every
    // repo variable to every job regardless of matrix target. Unset locally, so a developer can
    // still run a single cloud's tests just by setting that cloud's own env vars.
    private static boolean targetMatches(String target) {

        var configuredTarget = System.getenv("TRAC_CLOUD_TARGET");
        return configuredTarget == null || configuredTarget.equals(target);
    }
}
