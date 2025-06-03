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

package org.finos.tracdap.svc.meta.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.util.VersionInfo;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;


abstract class InfoAndResourcesTest {

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TRAC_TENANTS_UNIT = "config/trac-unit-tenants.yaml";
    public static final String TRAC_CONFIG_ENV_VAR = "TRAC_CONFIG_FILE";
    public static final String TRAC_TENANTS_ENV_VAR = "TRAC_TENANTS_FILE";

    protected TracMetadataApiGrpc.TracMetadataApiBlockingStub readApi;

    // Include this test case as a unit test
    static class UnitTest extends InfoAndResourcesTest {

        @RegisterExtension
        private static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT, List.of(TRAC_TENANTS_UNIT))
                .runDbDeploy(true)
                .addTenant(TEST_TENANT)
                .startService(TracMetadataService.class)
                .startService(TracAdminService.class)
                .build();

        @BeforeEach
        void setup() {
            readApi = platform.metaClientBlocking();
        }
    }

    // For now, do not run this test for integration against the metadb backend databases
    // Currently resources come from config, so there is no integration point
    // When resources move to being managed as metadata objects, this will need to be enabled
    @Disabled
    @Tag("integration")
    @Tag("int-metadb")
    static class IntegrationTest extends InfoAndResourcesTest {

        private static final String TRAC_CONFIG_ENV_FILE = System.getenv(TRAC_CONFIG_ENV_VAR);
        private static final String TRAC_TENANTS_ENV_FILE = System.getenv(TRAC_TENANTS_ENV_VAR);

        @RegisterExtension
        private static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_ENV_FILE, List.of(TRAC_TENANTS_ENV_FILE))
                .runDbDeploy(false)
                .addTenant(TEST_TENANT)
                .startService(TracMetadataService.class)
                .startService(TracAdminService.class)
                .build();

        @BeforeEach
        void setup() {
            readApi = platform.metaClientBlocking();
        }
    }

    @Test
    void platformInfo() {

        var platformInfo = readApi.platformInfo(PlatformInfoRequest.newBuilder().build());

        System.out.println("Running TRAC D.A.P. version " + platformInfo.getTracVersion());

        var expectedVersion = VersionInfo.getComponentVersion(TracMetadataService.class);
        Assertions.assertEquals(expectedVersion, platformInfo.getTracVersion());

        // Environment settings are set up for this test in test-unit.yaml in the -list-test resources folder
        // For integration tests, config files are in the .github folder under config

        Assertions.assertEquals("TEST_ENVIRONMENT", platformInfo.getEnvironment());
        Assertions.assertFalse(platformInfo.getProduction());
        Assertions.assertTrue(platformInfo.containsDeploymentInfo("region"));
        Assertions.assertEquals("UK", platformInfo.getDeploymentInfoOrThrow("region"));
    }

    @Test
    void listTenants() {

        var tenantsResponse = readApi.listTenants(ListTenantsRequest.newBuilder().build());
        var tenants = tenantsResponse.getTenantsList();

        Assertions.assertEquals(1, tenants.size());
        Assertions.assertEquals(TEST_TENANT, tenants.get(0).getTenantCode());

        // Default description set up in org.finos.tracdap.test.helpers.PlatformTest
        // Also for integration tests, in the integration.yml workflow for GitHub actions
        var expectedDescription = "Test tenant [" + TEST_TENANT + "]";
        Assertions.assertEquals(expectedDescription, tenants.get(0).getDescription());
    }
}
