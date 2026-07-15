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

package org.finos.tracdap.svc.orch.api;

import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.helpers.PlatformTest;

import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;


/**
 * Verify the orchestrator service reports SERVING on the standard gRPC health service
 * (grpc.health.v1.Health) once started. The probe is a bare, unauthenticated health
 * check, matching how the gateway /availablez endpoint checks service readiness.
 */
class OrchestratorHealthCheckTest {

    private static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    private static final String TRAC_TENANTS_UNIT = "config/trac-unit-tenants.yaml";
    private static final String TEST_TENANT = "ACME_CORP";

    @RegisterExtension
    private static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT, List.of(TRAC_TENANTS_UNIT))
            .runDbDeploy(true)
            .addTenant(TEST_TENANT)
            .startService(TracAdminService.class)
            .startService(TracMetadataService.class)
            .startService(TracDataService.class)
            .startService(TracOrchestratorService.class)
            .build();

    @Test
    void healthCheckReportsServing() {

        var healthClient = platform.healthClientBlocking(ConfigKeys.ORCHESTRATOR_SERVICE_KEY);
        var request = HealthCheckRequest.newBuilder().setService("").build();

        var response = healthClient.check(request);

        Assertions.assertEquals(ServingStatus.SERVING, response.getStatus());
    }
}
