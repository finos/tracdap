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

package org.finos.tracdap.common.util;

import org.finos.tracdap.common.config.ConfigHelpers;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.exception.EConfig;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.RoutingTarget;
import org.finos.tracdap.config.ServiceConfig;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class RoutingUtils {

    // This is a quick solution for routing between services based on the deployment layout

    private static final Map<String, String> STANDARD_ALIASES = Map.ofEntries(
            Map.entry(ConfigKeys.ADMIN_SERVICE_KEY, "tracdap-svc-admin"),
            Map.entry(ConfigKeys.METADATA_SERVICE_KEY, "tracdap-svc-meta"),
            Map.entry(ConfigKeys.DATA_SERVICE_KEY, "tracdap-svc-data"),
            Map.entry(ConfigKeys.ORCHESTRATOR_SERVICE_KEY, "tracdap-svc-orch"),
            Map.entry(ConfigKeys.GATEWAY_SERVICE_KEY, "tracdap-gateway"));

    public static RoutingTarget serviceTarget(PlatformConfig platformConfig, String serviceKey) {

        if (!platformConfig.containsServices(serviceKey))
            throw new EConfig(String.format("Missing or invalid config: services.%s", serviceKey));

        var serviceConfig = platformConfig.getServicesOrThrow(serviceKey);

        switch (platformConfig.getDeployment().getLayout()) {

            case LAYOUT_NOT_SET:
                throw new EConfig("Missing or invalid config: [deployment.layout]");

            case LOCALHOST:

                return RoutingTarget.newBuilder()
                        .setHost("localhost")
                        .setPort(serviceConfig.getPort())
                        .build();

            case SERVICE_KEY:

                var optionalAlias = serviceConfig.getAlias();
                var serviceKeyOrAlias = optionalAlias.isEmpty() ? STANDARD_ALIASES.get(serviceKey) : optionalAlias;

                if (serviceKeyOrAlias.isEmpty())
                    throw new EConfig(String.format("Missing required config: services.%s.alias", serviceKey));

                return RoutingTarget.newBuilder()
                        .setHost(serviceKeyOrAlias)
                        .setPort(serviceConfig.getPort())
                        .build();

            case SERVICE_ALIAS:

                var serviceAlias = serviceConfig.getAlias();

                if (serviceAlias.isEmpty())
                    throw new EConfig(String.format("Missing required config: services.%s.alias", serviceKey));

                return RoutingTarget.newBuilder()
                        .setHost(serviceAlias)
                        .setPort(serviceConfig.getPort())
                        .build();

            // Should never happen
            default:
                throw new EUnexpected();
        }

    }

    /**
     * Resolve the optional local bind host configured for a service via {@link ConfigKeys#NETWORK_BIND_ADDRESS}.
     *
     * @return trimmed bind host/IP, or {@code null} when unset/blank (meaning wildcard bind)
     */
    public static String serviceBindHost(ServiceConfig serviceConfig) {

        var bindHost = ConfigHelpers.readString(
                "service",
                serviceConfig.getPropertiesMap(),
                ConfigKeys.NETWORK_BIND_ADDRESS,
                false);

        if (bindHost == null || bindHost.isBlank())
            return null;

        return bindHost;
    }

    /**
     * Resolve the local {@link InetSocketAddress} a service should bind to.
     * Falls back to a wildcard bind on the configured port when {@link ConfigKeys#NETWORK_BIND_ADDRESS} is not set.
     */
    public static InetSocketAddress serviceBindAddress(ServiceConfig serviceConfig) {

        var bindHost = serviceBindHost(serviceConfig);
        var port = serviceConfig.getPort();

        if (bindHost != null) {
            try {
                return new InetSocketAddress(InetAddress.getByName(bindHost), port);
            }
            catch (UnknownHostException e) {
                throw new EConfig(String.format("Invalid bind address [%s]: %s", bindHost, e.getMessage()), e);
            }
        }

        return new InetSocketAddress(port);
    }
}
