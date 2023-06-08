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

package org.finos.tracdap.gateway.exec;

import org.finos.tracdap.config.GwRoute;
import org.finos.tracdap.gateway.proxy.rest.RestApiMethod;

import java.util.List;


public class Route {

    private final int index;
    private final GwRoute config;

    private final IRouteMatcher matcher;

    private final List<RestApiMethod<?, ?>> restMethods;

    public Route(int index, GwRoute config, IRouteMatcher matcher) {
        this(index, config, matcher, null);
    }

    public Route(
            int index, GwRoute config,
            IRouteMatcher matcher,
            List<RestApiMethod<?, ?>> restMethods) {

        this.config = config;
        this.index = index;
        this.matcher = matcher;
        this.restMethods = restMethods;
    }

    public int getIndex() {
        return index;
    }

    public GwRoute getConfig() {
        return config;
    }

    public IRouteMatcher getMatcher() {
        return matcher;
    }

    public List<RestApiMethod<?, ?>> getRestMethods() {
        return restMethods;
    }
}
