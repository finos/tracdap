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

package org.finos.tracdap.gateway.config.helpers;

import org.finos.tracdap.config.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class ConfigTranslator {

    public static GatewayConfig translateServiceRoutes(org.finos.tracdap.config.GatewayConfig originalConfig) {

        var serviceRoutes = createRoutesForServices(originalConfig.getServices());

        return originalConfig.toBuilder()
                .addAllRoutes(serviceRoutes)
                .build();
    }

    public static List<GwRoute> createRoutesForServices(GwServiceMap services) {

        if (services == null)
            return List.of();

        var serviceRoutes = new ArrayList<GwRoute>();

        if (services.hasMeta()) {

            var metaApiRoutes = createRoutesForService(
                    "TRAC Metadata Service",
                    services.getMeta(),
                    "/tracdap.api.TracMetadataApi/",
                    "/trac-meta/",
                    GwRestMapping.TRAC_META);

            serviceRoutes.addAll(metaApiRoutes);
        }

        if (services.hasData()) {

            var dataApiRoutes = createRoutesForService(
                    "TRAC Data Service",
                    services.getData(),
                    "/tracdap.api.TracDataApi/",
                    "/trac-data/",
                    null);

            serviceRoutes.addAll(dataApiRoutes);
        }

        if (services.hasOrch()) {

            var orchApiRoutes = createRoutesForService(
                    "TRAC Orchestrator Service",
                    services.getOrch(),
                    "/tracdap.api.TracOrchestratorApi/",
                    "/trac-orch/",
                    GwRestMapping.TRAC_ORCH);

            serviceRoutes.addAll(orchApiRoutes);
        }

        return serviceRoutes;
    }

    public static List<GwRoute> createRoutesForService(
            String routeName, GwService service,
            String grpcPath, String restPath, GwRestMapping restMapping) {

        var routes = new ArrayList<GwRoute>();

        if (service.getProtocolsList().contains(GwProtocol.GRPC) || service.getProtocolsList().contains(GwProtocol.GRPC_WEB))
            routes.add(createGrpcRoute(routeName, service, grpcPath));

        if (service.getProtocolsList().contains(GwProtocol.REST) && restMapping != null)
            routes.add(createRestRoute(routeName, service, restPath, restMapping));

        return routes;
    }

    private static GwRoute createGrpcRoute(
            String routeName, GwService service, String grpcPath) {

        var protocols = service.getProtocolsList().stream()
                .filter(p -> List.of(GwProtocol.GRPC, GwProtocol.GRPC_WEB).contains(p))
                .collect(Collectors.toList());

        var grpcRoute = GwRoute.newBuilder();
        grpcRoute.setRouteName(routeName);
        grpcRoute.setRouteType(GwProtocol.GRPC);
        grpcRoute.addAllProtocols(protocols);

        createMatchAndTarget(grpcRoute, service, grpcPath);

        return grpcRoute.build();
    }

    private static GwRoute createRestRoute(
            String routeName, GwService service,
            String restPath, GwRestMapping restMapping) {

        var restRoute = GwRoute.newBuilder();
        restRoute.setRouteName(routeName);
        restRoute.setRouteType(GwProtocol.REST);
        restRoute.addProtocols(GwProtocol.REST);
        restRoute.setRestMapping(restMapping);

        createMatchAndTarget(restRoute, service, restPath);

        return restRoute.build();
    }

    private static void createMatchAndTarget(GwRoute.Builder route, GwService service, String path) {

        var match = GwMatch.newBuilder();
        match.setPath(path);

        var target = GwTarget.newBuilder();
        target.setScheme(service.getTarget().getScheme());
        target.setHost(service.getTarget().getHost());
        target.setPort(service.getTarget().getPort());
        target.setPath(path);

        route.setMatch(match);
        route.setTarget(target);
    }
}
