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

package com.accenture.trac.gateway.exec;

import org.finos.tracdap.config.GwRoute;
import com.accenture.trac.gateway.config.rest.OrchApiRestMapping;
import com.accenture.trac.gateway.config.rest.MetaApiRestMapping;
import com.accenture.trac.gateway.proxy.rest.RestApiMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RouteBuilder {

    private static final Logger log = LoggerFactory.getLogger(RouteBuilder.class);

    public static List<Route> buildAll(List<GwRoute> routeConfigs) {

        log.info("Pre-building the route map...");

        return IntStream.range(0, routeConfigs.size())
                .mapToObj(i -> Map.entry(i, routeConfigs.get(i)))
                .map(kv -> build(kv.getValue(), kv.getKey()))
                .collect(Collectors.toList());
    }

    public static Route build(GwRoute config, int routeIndex) {

        log.info("Building route: {} ({}) {}",
                config.getRouteName(), config.getRouteType(),
                config.getMatch().getPath());

        var matchPath = config.getMatch().getPath();
        var matcher = (IRouteMatcher)
                (uri, method, headers) -> uri
                .getPath()
                .startsWith(matchPath);

        var restMethods = lookupRestMethods(config);

        return new Route(routeIndex, config, matcher, restMethods);
    }

    private static List<RestApiMethod<?, ?, ?>> lookupRestMethods(GwRoute config) {

        switch (config.getRestMapping()) {

            case TRAC_META: return MetaApiRestMapping.metaApiRoutes();

            case TRAC_ORCH: return OrchApiRestMapping.orchApiRoutes();

            default:
                return null;
        }
    }
}
