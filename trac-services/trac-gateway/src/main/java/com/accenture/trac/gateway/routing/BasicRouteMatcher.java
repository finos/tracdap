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

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

import java.net.URI;


/**
 * Route matcher that matches the first section of the URI path.
 */
public class BasicRouteMatcher implements IRouteMatcher{

    private final String pathPrefix;

    public BasicRouteMatcher(String pathPrefix) {
        this.pathPrefix = pathPrefix;
    }

    @Override
    public boolean matches(URI uri, HttpMethod method, HttpHeaders headers) {

        var pathParts = uri.getPath().split("/");

        // Part 0 will be an empty string because uri starts with /
        return pathParts.length > 1 && pathParts[1].equals(pathPrefix);
    }
}
