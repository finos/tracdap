/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.orch.api;

import org.finos.tracdap.api.JobRequest;
import org.finos.tracdap.api.TracOrchestratorApiGrpc;
import org.finos.tracdap.metadata.ImportModelJob;
import org.finos.tracdap.metadata.JobDefinition;
import org.finos.tracdap.metadata.JobStatusCode;
import org.finos.tracdap.metadata.JobType;
import org.finos.tracdap.test.helpers.PlatformTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;


public class JobValidationTest {

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TEST_TENANT = "ACME_CORP";

    protected static TracOrchestratorApiGrpc.TracOrchestratorApiBlockingStub orchClient;

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
            .runDbDeploy(true)
            .addTenant(TEST_TENANT)
            .startMeta()
            .startOrch()
            .build();

    @BeforeAll
    static void setupClass() {
        orchClient = platform.orchClientBlocking();
    }

    @Test
    public void test1() {

        var job = JobDefinition.newBuilder()
            .setJobType(JobType.IMPORT_MODEL)
            .setImportModel(ImportModelJob.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setVersion("v1.0.0")
                .setPath("src/")
                .setEntryPoint("acme.models.test_model.CoyoteBlaster"));

        var request = JobRequest.newBuilder()
            .setTenant(TEST_TENANT)
            .setJob(job)
            .build();

        var response = orchClient.validateJob(request);

        Assertions.assertEquals(JobStatusCode.VALIDATED, response.getStatusCode());
    }
}
