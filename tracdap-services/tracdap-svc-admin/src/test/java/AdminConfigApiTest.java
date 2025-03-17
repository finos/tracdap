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

import org.finos.tracdap.api.TracAdminApiGrpc;
import org.finos.tracdap.api.TracMetadataApiGrpc;
import org.finos.tracdap.api.internal.TrustedMetadataApiGrpc;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.helpers.PlatformTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;


abstract class AdminConfigApiTest {

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TRAC_CONFIG_ENV_VAR = "TRAC_CONFIG_FILE";

    protected TracMetadataApiGrpc.TracMetadataApiBlockingStub metaApi;
    protected TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub trustedApi;
    protected TracAdminApiGrpc.TracAdminApiBlockingStub adminApi;

    // Include this test case as a unit test
    static class UnitTest extends AdminConfigApiTest {

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
                .runDbDeploy(true)
                .addTenant(TEST_TENANT)
                .startService(TracMetadataService.class)
                .startService(TracAdminService.class)
                .build();

        @BeforeEach
        void setup() {
            metaApi = platform.metaClientBlocking();
            trustedApi = platform.metaClientTrustedBlocking();
            adminApi = platform.adminClientBlocking();
        }
    }

    // Include this test case for integration against different database backends
    @org.junit.jupiter.api.Tag("integration")
    @org.junit.jupiter.api.Tag("int-metadb")
    static class IntegrationTest extends AdminConfigApiTest {

        private static final String TRAC_CONFIG_ENV_FILE = System.getenv(TRAC_CONFIG_ENV_VAR);

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_ENV_FILE)
                .runDbDeploy(false)
                .addTenant(TEST_TENANT)
                .startService(TracAdminService.class)
                .build();

        @BeforeEach
        void setup() {
            metaApi = platform.metaClientBlocking();
            trustedApi = platform.metaClientTrustedBlocking();
            adminApi = platform.adminClientBlocking();
        }
    }

    @Test
    void test1() {

    }
}
