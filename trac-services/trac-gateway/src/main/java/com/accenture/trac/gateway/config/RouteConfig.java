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

package com.accenture.trac.gateway.config;


import java.util.List;

public class RouteConfig {

    private String routeName;
    private RouteType routeType;
    private List<RouteProtocol> protocols = List.of();
    private RestMapping restMapping;

    private MatchConfig match;
    private TargetConfig target;



    public String getRouteName() {
        return routeName;
    }

    public void setRouteName(String routeName) {
        this.routeName = routeName;
    }

    public RouteType getRouteType() {
        return routeType;
    }

    public void setRouteType(RouteType routeType) {
        this.routeType = routeType;
    }

    public List<RouteProtocol> getProtocols() {
        return protocols;
    }

    public void setProtocols(List<RouteProtocol> protocols) {
        this.protocols = protocols;
    }

    public RestMapping getRestMapping() {
        return restMapping;
    }

    public void setRestMapping(RestMapping restMapping) {
        this.restMapping = restMapping;
    }

    public MatchConfig getMatch() {
        return match;
    }

    public void setMatch(MatchConfig match) {
        this.match = match;
    }

    public TargetConfig getTarget() {
        return target;
    }

    public void setTarget(TargetConfig target) {
        this.target = target;
    }
}
