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
import org.finos.tracdap.metadata.ImportModelJob;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


@Tag("integration")
@Tag("int-e2e")
public abstract class ImportModelTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";

    // Test model import using different repo types
    // This will test the E2E model loading mechanism
    // The same mechanism is used for model import and model run
    // So we don't need to test all combinations of model run from different repo types

    protected abstract String useTracRepo();

    public static class LocalRepoTest extends ImportModelTest {
        protected String useTracRepo() { return "TRAC_LOCAL_REPO"; }
    }

    public static class GitRepoTest extends ImportModelTest {
        protected String useTracRepo() { return "TRAC_GIT_REPO"; }
    }

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

    private static final Logger log = LoggerFactory.getLogger(ImportModelTest.class);

    @Test
    void importModel() throws Exception {

        log.info("Running IMPORT_MODEL job...");

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository(useTracRepo())
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.schema_files.PnlAggregationSchemas")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_model")
                        .setValue(MetadataCodec.encodeValue("import_model:schema_files"))
                        .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("import_model:schema_files"))
                .build());

        var modelTag = Helpers.doModelImport(platform, TEST_TENANT, modelStub, modelAttrs, jobAttrs);

        var modelDef = modelTag.getDefinition().getModel();
        var modelAttr = modelTag.getAttrsOrThrow("e2e_test_model");

        Assertions.assertEquals("import_model:schema_files", MetadataCodec.decodeStringValue(modelAttr));
        Assertions.assertEquals("tutorial.schema_files.PnlAggregationSchemas", modelDef.getEntryPoint());
        Assertions.assertTrue(modelDef.getParametersMap().containsKey("eur_usd_rate"));
        Assertions.assertTrue(modelDef.getInputsMap().containsKey("customer_loans"));
        Assertions.assertTrue(modelDef.getOutputsMap().containsKey("profit_by_region"));
    }

    @Test
    void importModelAsNewVersion() throws Exception {

        log.info("Running IMPORT_MODEL job as new version...");

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository(useTracRepo())
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.schema_files.PnlAggregationSchemas")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("import_model_as_new_version:schema_files"))
                .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("import_model_as_new_version:schema_files"))
                .build());

        // Import version 1 — use the prior commit so v1 and v2 have distinct commit SHAs
        var priorCommit = GitHelpers.getPriorCommit();
        var modelV1Stub = modelStub.toBuilder().setVersion(priorCommit).build();
        var modelV1Tag = Helpers.doModelImport(platform, TEST_TENANT, modelV1Stub, modelAttrs, jobAttrs);
        var modelV1Header = modelV1Tag.getHeader();
        Assertions.assertEquals(1, modelV1Header.getObjectVersion());

        // Import version 2 — same entry point but current commit, priorModel set to v1
        var orchClient = platform.orchClientBlocking();
        var metaClient = platform.metaClientBlocking();

        var importModelV2 = ImportModelJob.newBuilder()
                .setLanguage(modelStub.getLanguage())
                .setRepository(modelStub.getRepository())
                .setPath(modelStub.getPath())
                .setEntryPoint(modelStub.getEntryPoint())
                .setVersion(modelStub.getVersion())  // current commit — differs from v1
                .setPriorModel(MetadataUtil.selectorFor(modelV1Header))
                .addAllModelAttrs(modelAttrs)
                .build();

        var jobV2Request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_MODEL)
                        .setImportModel(importModelV2))
                .addAllJobAttrs(jobAttrs)
                .build();

        var jobV2Id = Helpers.startJob(orchClient, jobV2Request).getJobId();
        Helpers.waitForJob(orchClient, TEST_TENANT, jobV2Id);

        // Read version 2 directly and verify it is a new version of the same object
        var v2Selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(modelV1Header.getObjectId())
                .setObjectVersion(2)
                .setLatestTag(true)
                .build();

        var modelV2Tag = metaClient.readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(v2Selector)
                .build());

        Assertions.assertEquals(modelV1Header.getObjectId(), modelV2Tag.getHeader().getObjectId());
        Assertions.assertEquals(2, modelV2Tag.getHeader().getObjectVersion());
        Assertions.assertEquals(modelStub.getEntryPoint(), modelV2Tag.getDefinition().getModel().getEntryPoint());
    }

    @Test
    void importModelSameCommitRejected() throws Exception {

        log.info("Running IMPORT_MODEL new-version job with same commit — expecting failure...");

        var modelVersion = GitHelpers.getCurrentCommit();
        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository(useTracRepo())
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.schema_files.PnlAggregationSchemas")
                .setVersion(modelVersion)
                .build();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("import_model_same_commit_rejected:schema_files"))
                .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("import_model_same_commit_rejected:schema_files"))
                .build());

        // Import version 1
        var modelV1Tag = Helpers.doModelImport(platform, TEST_TENANT, modelStub, modelAttrs, jobAttrs);
        var modelV1Header = modelV1Tag.getHeader();

        // Attempt version 2 with the same commit SHA — must be rejected
        var orchClient = platform.orchClientBlocking();

        var importModelV2 = ImportModelJob.newBuilder()
                .setLanguage(modelStub.getLanguage())
                .setRepository(modelStub.getRepository())
                .setPath(modelStub.getPath())
                .setEntryPoint(modelStub.getEntryPoint())
                .setVersion(modelStub.getVersion())  // same commit as v1
                .setPriorModel(MetadataUtil.selectorFor(modelV1Header))
                .addAllModelAttrs(modelAttrs)
                .build();

        var jobV2Request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_MODEL)
                        .setImportModel(importModelV2))
                .addAllJobAttrs(jobAttrs)
                .build();

        var jobV2Id = Helpers.startJob(orchClient, jobV2Request).getJobId();

        Executable waitForV2 = () -> Helpers.waitForJob(orchClient, TEST_TENANT, jobV2Id);
        Assertions.assertThrows(RuntimeException.class, waitForV2);
    }

    @Test
    void importModelNonLatestPriorRejected() throws Exception {

        log.info("Running IMPORT_MODEL new-version job with non-latest prior — expecting failure...");

        var modelVersion = GitHelpers.getCurrentCommit();
        var priorCommit = GitHelpers.getPriorCommit();

        var modelAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_model")
                .setValue(MetadataCodec.encodeValue("import_model_non_latest_prior_rejected:schema_files"))
                .build());

        var jobAttrs = List.of(TagUpdate.newBuilder()
                .setAttrName("e2e_test_job")
                .setValue(MetadataCodec.encodeValue("import_model_non_latest_prior_rejected:schema_files"))
                .build());

        // Import version 1
        var modelV1Stub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository(useTracRepo())
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.schema_files.PnlAggregationSchemas")
                .setVersion(priorCommit)
                .build();

        var modelV1Tag = Helpers.doModelImport(platform, TEST_TENANT, modelV1Stub, modelAttrs, jobAttrs);
        var modelV1Header = modelV1Tag.getHeader();

        // Import version 2 successfully (priorModel = v1)
        var orchClient = platform.orchClientBlocking();

        var importModelV2 = ImportModelJob.newBuilder()
                .setLanguage(modelV1Stub.getLanguage())
                .setRepository(modelV1Stub.getRepository())
                .setPath(modelV1Stub.getPath())
                .setEntryPoint(modelV1Stub.getEntryPoint())
                .setVersion(modelVersion)
                .setPriorModel(MetadataUtil.selectorFor(modelV1Header))
                .addAllModelAttrs(modelAttrs)
                .build();

        var jobV2Request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_MODEL)
                        .setImportModel(importModelV2))
                .addAllJobAttrs(jobAttrs)
                .build();

        var jobV2Id = Helpers.startJob(orchClient, jobV2Request).getJobId();
        Helpers.waitForJob(orchClient, TEST_TENANT, jobV2Id);

        // Now attempt version 3 using v1 (not v2, the latest) as priorModel — must be rejected
        var importModelV3FromV1 = ImportModelJob.newBuilder()
                .setLanguage(modelV1Stub.getLanguage())
                .setRepository(modelV1Stub.getRepository())
                .setPath(modelV1Stub.getPath())
                .setEntryPoint(modelV1Stub.getEntryPoint())
                .setVersion(priorCommit + "-alt")  // distinct version string
                .setPriorModel(MetadataUtil.selectorFor(modelV1Header))  // v1, not the latest v2
                .addAllModelAttrs(modelAttrs)
                .build();

        var jobV3Request = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_MODEL)
                        .setImportModel(importModelV3FromV1))
                .addAllJobAttrs(jobAttrs)
                .build();

        var jobV3Id = Helpers.startJob(orchClient, jobV3Request).getJobId();

        Executable waitForV3 = () -> Helpers.waitForJob(orchClient, TEST_TENANT, jobV3Id);
        Assertions.assertThrows(RuntimeException.class, waitForV3);
    }

    // Let other tests in this suite use this to import models


}
