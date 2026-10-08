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
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RefusedModelTypesTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";

    private static final Set<JobStatusCode> COMPLETED_STATES = Set.of(
            JobStatusCode.SUCCEEDED, JobStatusCode.FAILED, JobStatusCode.CANCELLED);

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(E2E_CONFIG, List.of(E2E_TENANTS))
            .runDbDeploy(true)
            .runCacheDeploy(true)
            .addTenant(TEST_TENANT)
            .prepareLocalExecutor(true)
            .preStartAction(RefusedModelTypesTest::refuseImportModels)
            .startService(TracMetadataService.class)
            .startService(TracDataService.class)
            .startService(TracOrchestratorService.class)
            .startService(TracAdminService.class)
            .build();

    static TagHeader jobId_importModel;
    static TagHeader jobId_standardModel;

    private static void refuseImportModels(PlatformTest platform) {

        try {

            var configFile = Path.of(platform.platformConfigUrl().toURI());
            var config = Files.readString(configFile);

            var refusedEntry = String.format("config:\n  %s: %s\n", ConfigKeys.MODEL_IMPORT_REFUSED_TYPES, ModelType.DATA_IMPORT_MODEL);
            var updatedConfig = config.replaceFirst("config:\n", refusedEntry);

            Assertions.assertNotEquals(config, updatedConfig);
            Files.writeString(configFile, updatedConfig);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test @Order(101)
    void importModel_refused() {

        var importModel = modelStub("tutorial.data_import.SimpleDataImport");

        jobId_importModel = Helpers.startModelImport(platform, TEST_TENANT, importModel, List.of(), List.of());
    }

    @Test @Order(102)
    void importModel_refused_result() {

        var jobStatus = waitForCompletion(jobId_importModel);

        Assertions.assertEquals(JobStatusCode.FAILED, jobStatus.getStatusCode());
        Assertions.assertTrue(jobStatus.getStatusMessage().contains(ModelType.DATA_IMPORT_MODEL.name()), jobStatus.getStatusMessage());
        Assertions.assertTrue(jobStatus.getStatusMessage().contains(ConfigKeys.MODEL_IMPORT_REFUSED_TYPES), jobStatus.getStatusMessage());

        Assertions.assertEquals(0, searchModelsCreatedBy(jobId_importModel));
    }

    @Test @Order(201)
    void standardModel_accepted() {

        var standardModel = modelStub("tutorial.using_data.PnlAggregation");

        jobId_standardModel = Helpers.startModelImport(platform, TEST_TENANT, standardModel, List.of(), List.of());
    }

    @Test @Order(202)
    void standardModel_accepted_result() {

        var modelTag = Helpers.waitForModelImport(platform, TEST_TENANT, jobId_standardModel);

        Assertions.assertEquals(ModelType.STANDARD_MODEL, modelTag.getDefinition().getModel().getModelType());
    }

    private static ModelDefinition modelStub(String entryPoint) {

        try {

            return ModelDefinition.newBuilder()
                    .setLanguage("python")
                    .setRepository("TRAC_LOCAL_REPO")
                    .setPath("examples/models/python/src")
                    .setEntryPoint(entryPoint)
                    .setVersion(GitHelpers.getCurrentCommit())
                    .build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    private static int searchModelsCreatedBy(TagHeader jobId) {

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                        .setObjectType(ObjectType.MODEL)
                        .setSearch(SearchExpression.newBuilder()
                                .setTerm(SearchTerm.newBuilder()
                                        .setAttrName("trac_create_job")
                                        .setAttrType(BasicType.STRING)
                                        .setOperator(SearchOperator.EQ)
                                        .setSearchValue(MetadataCodec.encodeValue(MetadataUtil.objectKey(jobId))))))
                .build();

        return platform.metaClientBlocking().search(searchRequest).getSearchResultCount();
    }
}
