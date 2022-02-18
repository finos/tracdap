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

package com.accenture.trac.gateway.config.rest;

import com.accenture.trac.api.JobRequest;
import com.accenture.trac.api.JobStatusRequest;
import com.accenture.trac.api.TracOrchestratorApiGrpc;
import com.accenture.trac.gateway.proxy.rest.RestApiMethod;
import com.accenture.trac.metadata.TagSelector;
import io.netty.handler.codec.http.HttpMethod;

import java.util.ArrayList;
import java.util.List;


public class OrchApiRestMapping {

    public static List<RestApiMethod<?, ?, ?>> orchApiRoutes() {

        var apiMethods = new ArrayList<RestApiMethod<?, ?, ?>>();

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/trac-orch/api/v1/{tenant}/validate-job",
                TracOrchestratorApiGrpc.getValidateJobMethod(),
                JobRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/trac-orch/api/v1/{tenant}/submit-job",
                TracOrchestratorApiGrpc.getSubmitJobMethod(),
                JobRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/trac-orch/api/v1/{tenant}/check-job",
                TracOrchestratorApiGrpc.getCheckJobMethod(),
                JobStatusRequest.getDefaultInstance(),
                "selector", TagSelector.getDefaultInstance()));

        return apiMethods;
    }
}
