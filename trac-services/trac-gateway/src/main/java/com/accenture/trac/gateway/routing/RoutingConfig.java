/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.gateway.routing;

import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;


public class RoutingConfig {

    private final List<Route> routes;

    public RoutingConfig() {
        this.routes = new ArrayList<>();
    }

    public static RoutingConfig newBlankConfig() {
        return new RoutingConfig();
    }

    public RoutingConfig addRoute(IRouteMatcher matcher, Supplier<ChannelInboundHandler> handler) {

        var newRoute = new Route(matcher, handler);

        this.routes.add(newRoute);

        return this;
    }

    public ChannelInboundHandler matchRequest(URI uri, HttpMethod method, HttpHeaders headers) {

        for (var route : routes) {
            if (route.matcher.matches(uri, method, headers))
                return route.handler.get();
        }

        return null;
    }

    private static class Route {

        final IRouteMatcher matcher;
        final Supplier<ChannelInboundHandler> handler;

        Route(IRouteMatcher matcher, Supplier<ChannelInboundHandler> handler) {
            this.matcher = matcher;
            this.handler = handler;
        }

    }
}

