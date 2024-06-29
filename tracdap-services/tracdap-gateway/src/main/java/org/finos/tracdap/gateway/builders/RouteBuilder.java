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

package org.finos.tracdap.gateway.builders;

import org.finos.tracdap.config.*;
import org.finos.tracdap.gateway.exec.IRouteMatcher;
import org.finos.tracdap.gateway.exec.Route;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class RouteBuilder {

    private static final ClassLoader API_CLASSLOADER = RouteBuilder.class.getClassLoader();
    private static final Logger log = LoggerFactory.getLogger(RouteBuilder.class);

    private int nextRouteIndex;

    public RouteBuilder() {
        nextRouteIndex = 0;
    }

    public List<Route> buildRoutes(PlatformConfig platformConfig) {

        log.info("Building the route table...");

        var services = ServiceInfo.buildServiceInfo(platformConfig);
        var customRoutes = platformConfig.getGateway().getRoutesList();

        var nRoutes = services.size() * 2 + customRoutes.size();
        var routes = new ArrayList<Route>(nRoutes);

        for (var serviceInfo : services) {
            var grpcRoute = buildGrpcRoute(serviceInfo);
            routes.add(grpcRoute);
        }

        for (var serviceInfo : services) {
            var restApiRoute = buildRestApiRoute(serviceInfo);
            routes.add(restApiRoute);
        }

        for (var routeConfig : customRoutes) {
            var route = buildCustomRoute(routeConfig);
            routes.add(route);
        }

        for (var route : routes) {

            log.info("[{}] {} -> {} ({})",
                    route.getIndex(),
                    route.getConfig().getMatch().getPath(),
                    route.getConfig().getRouteName(),
                    route.getConfig().getRouteType());
        }

        return routes;
    }

    private Route buildGrpcRoute(ServiceInfo serviceInfo) {

        var routeIndex = nextRouteIndex++;
        var routeName = serviceInfo.serviceName;
        var routeType = RouteConfig.Protocol.GRPC;

        var grpcPath = '/' + serviceInfo.descriptor.getFullName() + "/";
        var matcher = (IRouteMatcher) (method, url) -> url.getPath().startsWith(grpcPath);
        var protocols = List.of(RouteConfig.Protocol.GRPC, RouteConfig.Protocol.GRPC_WEB);

        var match = RouteConfig.Match.newBuilder()
                .setPath(grpcPath);

        var target = RouteConfig.Target.newBuilder()
                .setHost(serviceInfo.config.getAlias())  // TODO: Target for different deployment layouts
                .setPort(serviceInfo.config.getPort())
                .setPath(grpcPath);

        var routeConfig = RouteConfig.newBuilder()
                .setRouteName(routeName)
                .setRouteType(routeType)
                .addAllProtocols(protocols)
                .setMatch(match)
                .setTarget(target)
                .build();

        return new Route(routeIndex, routeConfig, matcher);
    }

    private Route buildRestApiRoute(ServiceInfo serviceInfo) {

        var routeIndex = nextRouteIndex++;
        var routeName = serviceInfo.serviceName;
        var routeType = RouteConfig.Protocol.REST;

        var restPath = serviceInfo.restPrefix + "/";
        var matcher = (IRouteMatcher) (method, url) -> url.getPath().startsWith(restPath);
        var protocols = List.of(RouteConfig.Protocol.REST);
        var restMethods = RestApiBuilder.buildAllMethods(serviceInfo.descriptor, serviceInfo.restPrefix, API_CLASSLOADER);

        var match = RouteConfig.Match.newBuilder()
                .setPath(restPath);

        var target = RouteConfig.Target.newBuilder()
                .setHost(serviceInfo.config.getAlias())  // TODO: Target for different deployment layouts
                .setPort(serviceInfo.config.getPort())
                .setPath(restPath);

        var routeConfig = RouteConfig.newBuilder()
                .setRouteName(routeName)
                .setRouteType(routeType)
                .addAllProtocols(protocols)
                .setMatch(match)
                .setTarget(target)
                .build();

        return new Route(routeIndex, routeConfig, matcher, restMethods);
    }

    private Route buildCustomRoute(RouteConfig routeConfig) {

        var routeIndex = nextRouteIndex++;

        var customPath = routeConfig.getMatch().getPath();
        var matcher = (IRouteMatcher) (method, uri) -> uri.getPath().startsWith(customPath);

        return new Route(routeIndex, routeConfig, matcher);
    }
}
