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

package org.finos.tracdap.gateway.proxy.rest.match;

import org.finos.tracdap.gateway.proxy.rest.RestApiRequest;

import java.util.List;


public class RequestMatcher implements Matcher<RestApiRequest> {

    private final Matcher<String> methodMatcher;
    private final Matcher<List<String>> pathMatcher;

    public RequestMatcher(Matcher<String> methodMatcher, Matcher<List<String>> pathMatcher) {
        this.methodMatcher = methodMatcher;
        this.pathMatcher = pathMatcher;
    }

    @Override
    public boolean matches(RestApiRequest request) {

        return methodMatcher.matches(request.method()) &&
                pathMatcher.matches(request.pathSegments());
    }
}
