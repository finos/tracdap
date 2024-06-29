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

package org.finos.tracdap.common.util;

import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.exception.EConfig;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.RoutingTarget;

import java.util.Map;

public class RoutingUtils {

    // This is a quick solution for routing between services based on the deployment layout

    private static final Map<String, String> STANDARD_ALIASES = Map.ofEntries(
            Map.entry(ConfigKeys.METADATA_SERVICE_KEY, "tracdap-svc-meta"),
            Map.entry(ConfigKeys.DATA_SERVICE_KEY, "tracdap-svc-data"),
            Map.entry(ConfigKeys.ORCHESTRATOR_SERVICE_KEY, "tracdap-svc-orch"),
            Map.entry(ConfigKeys.WEB_SERVER_SERVICE_KEY, "tracdap-webserver"),
            Map.entry(ConfigKeys.GATEWAY_SERVICE_KEY, "tracdap-gateway"));

    public static RoutingTarget serviceTarget(PlatformConfig platformConfig, String serviceKey) {

        if (!platformConfig.containsServices(serviceKey))
            throw new EConfig(String.format("Missing or invalid config: services.%s", serviceKey));

        var serviceConfig = platformConfig.getServicesOrThrow(serviceKey);

        switch (platformConfig.getDeployment().getLayout()) {

            case LAYOUT_NOT_SET:
                throw new EConfig("Missing or invalid config: [deployment.layout]");

            case SANDBOX:

                return RoutingTarget.newBuilder()
                        .setHost("localhost")
                        .setPort(serviceConfig.getPort())
                        .build();

            case HOSTED:

                var hostedAlias = serviceConfig.getAlias();
                var serviceAlias = hostedAlias.isEmpty() ? STANDARD_ALIASES.get(serviceKey) : hostedAlias;

                return RoutingTarget.newBuilder()
                        .setHost(serviceAlias)
                        .setPort(serviceConfig.getPort())
                        .build();

            case CUSTOM:

                var customAlias = serviceConfig.getAlias();

                if (customAlias.isEmpty())
                    throw new EConfig(String.format("Missing or invalid config: services.%s.alias", serviceKey));

                return RoutingTarget.newBuilder()
                        .setHost(customAlias)
                        .setPort(serviceConfig.getPort())
                        .build();

            // Should never happen
            default:
                throw new EUnexpected();
        }

    }
}
