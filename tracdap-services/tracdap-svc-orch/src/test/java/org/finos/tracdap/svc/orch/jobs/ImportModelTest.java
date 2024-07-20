/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.orch.jobs;


import org.finos.tracdap.api.JobRequest;
import org.finos.tracdap.api.MetadataReadRequest;
import org.finos.tracdap.api.MetadataSearchRequest;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.metadata.ImportModelJob;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.finos.tracdap.svc.orch.jobs.Helpers.runJob;


@Tag("integration")
@Tag("int-e2e")
public abstract class ImportModelTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";

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
    public static final PlatformTest platform = PlatformTest.forConfig(E2E_CONFIG)
            .addTenant(TEST_TENANT)
            .prepareLocalExecutor(true)
            .startAll()
            .build();

    private final Logger log = LoggerFactory.getLogger(getClass());


    @Test
    void importModel() throws Exception {

        log.info("Running IMPORT_MODEL job...");

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var modelVersion = GitHelpers.getCurrentCommit();

        var importModel = ImportModelJob.newBuilder()
                .setLanguage("python")
                .setRepository(useTracRepo())
                .setPath("examples/models/python/src")
                .setEntryPoint("tutorial.schema_files.SchemaFilesModel")
                .setVersion(modelVersion)
                .addModelAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_model")
                        .setValue(MetadataCodec.encodeValue("import_model:schema_files")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_MODEL)
                        .setImportModel(importModel))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("e2e_test_job")
                        .setValue(MetadataCodec.encodeValue("import_model:schema_files")))
                .build();

        // Developer note: This test will fail running locally if the latest commit is not pushed to GitHub
        // It doesn't need to be merged, but the commit must exist on your origin / fork

        var jobStatus = runJob(orchClient, jobRequest);
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        var modelSearch = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                        .setObjectType(ObjectType.MODEL)
                        .setSearch(SearchExpression.newBuilder()
                                .setTerm(SearchTerm.newBuilder()
                                        .setAttrName("trac_create_job")
                                        .setAttrType(BasicType.STRING)
                                        .setOperator(SearchOperator.EQ)
                                        .setSearchValue(MetadataCodec.encodeValue(jobKey)))))
                .build();

        var modelSearchResult = metaClient.search(modelSearch);

        Assertions.assertEquals(1, modelSearchResult.getSearchResultCount());

        var searchResult = modelSearchResult.getSearchResult(0);
        var modelReq = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(searchResult.getHeader()))
                .build();

        var modelTag = metaClient.readObject(modelReq);
        var modelDef = modelTag.getDefinition().getModel();
        var modelAttr = modelTag.getAttrsOrThrow("e2e_test_model");

        Assertions.assertEquals("import_model:schema_files", MetadataCodec.decodeStringValue(modelAttr));
        Assertions.assertEquals("tutorial.schema_files.SchemaFilesModel", modelDef.getEntryPoint());
        Assertions.assertTrue(modelDef.getParametersMap().containsKey("eur_usd_rate"));
        Assertions.assertTrue(modelDef.getInputsMap().containsKey("customer_loans"));
        Assertions.assertTrue(modelDef.getOutputsMap().containsKey("profit_by_region"));

        var descriptionAttr = modelTag.getAttrsOrThrow("model_description");
        var segmentAttr = modelTag.getAttrsOrThrow("business_segment");
        var classifiersAttr = modelTag.getAttrsOrThrow("classifiers");

        Assertions.assertInstanceOf(String.class, MetadataCodec.decodeValue(descriptionAttr));
        Assertions.assertEquals("retail_products", MetadataCodec.decodeValue(segmentAttr));
        Assertions.assertEquals(List.of("loans", "uk", "examples"), MetadataCodec.decodeArrayValue(classifiersAttr));
    }
}
