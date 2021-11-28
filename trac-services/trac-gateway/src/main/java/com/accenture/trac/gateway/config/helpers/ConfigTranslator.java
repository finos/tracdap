/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.gateway.config.helpers;

import com.accenture.trac.gateway.config.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ConfigTranslator {

    public static RootConfig translateServiceRoutes(RootConfig originalConfig) {

        var originalServices = originalConfig.getTrac().getGateway().getServices();
        var originalRoutes = originalConfig.getTrac().getGateway().getRoutes();

        // There may be no additional routes, just the main services
        if (originalRoutes == null) originalRoutes = List.of();

        var serviceRoutes = createRoutesForServices(originalServices);

        var allRoutes = Stream.concat(
                serviceRoutes.stream(),
                originalRoutes.stream())
                .collect(Collectors.toList());

        var gatewayConfig = new GatewayConfig();
        gatewayConfig.setProxy(originalConfig.getTrac().getGateway().getProxy());
        gatewayConfig.setServices(originalServices);
        gatewayConfig.setRoutes(allRoutes);

        var tracConfig = new TracConfig();
        tracConfig.setGateway(gatewayConfig);

        var translatedConfig = new RootConfig();
        translatedConfig.setConfig(originalConfig.getConfig());
        translatedConfig.setTrac(tracConfig);

        return translatedConfig;
    }

    public static List<RouteConfig> createRoutesForServices(ServicesConfig services) {

        if (services == null)
            return List.of();

        var serviceRoutes = new ArrayList<RouteConfig>();

        if (services.getMeta() != null) {

            var metaApiRoutes = createRoutesForService(
                    "TRAC Metadata Service",
                    services.getMeta(),
                    "/trac.api.TracMetadataApi/",
                    "/trac-meta/",
                    RestMapping.TRAC_META);

            serviceRoutes.addAll(metaApiRoutes);
        }

        if (services.getData() != null) {

            var metaApiRoutes = createRoutesForService(
                    "TRAC Data Service",
                    services.getData(),
                    "/trac.api.TracDataApi/",
                    "/trac-data/",
                    null);

            serviceRoutes.addAll(metaApiRoutes);
        }

        return serviceRoutes;
    }

    public static List<RouteConfig> createRoutesForService(
            String routeName, ServiceConfig service,
            String grpcPath, String restPath, RestMapping restMapping) {

        var routes = new ArrayList<RouteConfig>();

        if (service.getProtocols().contains(RouteProtocol.GRPC) || service.getProtocols().contains(RouteProtocol.GRPC_WEB))
            routes.add(createGrpcRoute(routeName, service, grpcPath));

        if (service.getProtocols().contains(RouteProtocol.REST) && restMapping != null)
            routes.add(createRestRoute(routeName, service, restPath, restMapping));

        return routes;
    }

    private static RouteConfig createGrpcRoute(
            String routeName, ServiceConfig service, String grpcPath) {

        var protocols = service.getProtocols().stream()
                .filter(p -> List.of(RouteProtocol.GRPC, RouteProtocol.GRPC_WEB).contains(p))
                .collect(Collectors.toList());

        var grpcRoute = new RouteConfig();
        grpcRoute.setRouteName(routeName);
        grpcRoute.setRouteType(RouteType.GRPC);
        grpcRoute.setProtocols(protocols);

        createMatchAndTarget(grpcRoute, service, grpcPath);

        return grpcRoute;
    }

    private static RouteConfig createRestRoute(
            String routeName, ServiceConfig service,
            String restPath, RestMapping restMapping) {

        var restRoute = new RouteConfig();
        restRoute.setRouteName(routeName);
        restRoute.setRouteType(RouteType.REST);
        restRoute.setProtocols(List.of(RouteProtocol.REST));
        restRoute.setRestMapping(restMapping);

        createMatchAndTarget(restRoute, service, restPath);

        return restRoute;
    }

    private static void createMatchAndTarget(RouteConfig route, ServiceConfig service, String path) {

        var match = new MatchConfig();
        match.setPath(path);

        var target = new TargetConfig();
        target.setScheme(service.getTarget().getScheme());
        target.setHost(service.getTarget().getHost());
        target.setPort(service.getTarget().getPort());
        target.setPath(path);

        route.setMatch(match);
        route.setTarget(target);
    }
}
