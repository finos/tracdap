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

package org.finos.tracdap.gateway.config.rest;

import org.finos.tracdap.api.*;
import org.finos.tracdap.gateway.proxy.rest.RestApiMethod;
import org.finos.tracdap.metadata.TagSelector;
import org.finos.tracdap.metadata.SearchParameters;

import io.netty.handler.codec.http.HttpMethod;

import java.util.ArrayList;
import java.util.List;


public class MetaApiRestMapping {

    /**
     * This config is expressed using Google's HTTP API annotations in the API proto files
     * To use the proto files authoritatively, we need to write a generator that understands those annotations
     * As a quick work-around, the REST mappings are duplicated here in code
     */

    public static List<RestApiMethod<?, ?, ?>> metaApiRoutes() {

        var apiMethods = new ArrayList<RestApiMethod<?, ?, ?>>();

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta/api/v1/{tenant}/create-object",
                TracMetadataApiGrpc.getCreateObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta/api/v1/{tenant}/update-object",
                TracMetadataApiGrpc.getUpdateObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta/api/v1/{tenant}/update-tag",
                TracMetadataApiGrpc.getUpdateTagMethod(),
                MetadataWriteRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta/api/v1/{tenant}/read-object",
                TracMetadataApiGrpc.getReadObjectMethod(),
                MetadataReadRequest.getDefaultInstance(),
                "selector", TagSelector.getDefaultInstance()));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta/api/v1/{tenant}/read-batch",
                TracMetadataApiGrpc.getReadBatchMethod(),
                MetadataBatchRequest.getDefaultInstance(),
                "selector", TagSelector.getDefaultInstance()));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta/api/v1/{tenant}/search",
                TracMetadataApiGrpc.getSearchMethod(),
                MetadataSearchRequest.getDefaultInstance(),
                "searchParams", SearchParameters.getDefaultInstance()));

        apiMethods.add(RestApiMethod.create(HttpMethod.GET,
                "/tracdap-meta/api/v1/{tenant}/{objectType}/{objectId}/versions/{objectVersion}/tags/{tagVersion}",
                TracMetadataApiGrpc.getGetObjectMethod(),
                MetadataGetRequest.getDefaultInstance()));

        apiMethods.add(RestApiMethod.create(HttpMethod.GET,
                "/tracdap-meta/api/v1/{tenant}/{objectType}/{objectId}/versions/latest/tags/latest",
                TracMetadataApiGrpc.getGetLatestObjectMethod(),
                MetadataGetRequest.getDefaultInstance()));

        apiMethods.add(RestApiMethod.create(HttpMethod.GET,
                "/tracdap-meta/api/v1/{tenant}/{objectType}/{objectId}/versions/{objectVersion}/tags/latest",
                TracMetadataApiGrpc.getGetLatestTagMethod(),
                MetadataGetRequest.getDefaultInstance()));

        return apiMethods;
    }

    public static List<RestApiMethod<?, ?, ?>> metaApiTrustedRoutes() {

        var apiMethods = new ArrayList<RestApiMethod<?, ?, ?>>();

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta-trusted/api/v1/{tenant}/trusted/create-object",
                TrustedMetadataApiGrpc.getCreateObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta-trusted/api/v1/{tenant}/trusted/update-object",
                TrustedMetadataApiGrpc.getUpdateObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta-trusted/api/v1/{tenant}/trusted/update-tag",
                TrustedMetadataApiGrpc.getUpdateTagMethod(),
                MetadataWriteRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta-trusted/api/v1/{tenant}/trusted/preallocate",
                TrustedMetadataApiGrpc.getPreallocateIdMethod(),
                MetadataWriteRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta-trusted/api/v1/{tenant}/trusted/create-preallocated",
                TrustedMetadataApiGrpc.getCreatePreallocatedObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta-trusted/api/v1/{tenant}/trusted/read-object",
                TracMetadataApiGrpc.getReadObjectMethod(),
                MetadataReadRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta-trusted/api/v1/{tenant}/trusted/read-batch",
                TracMetadataApiGrpc.getReadBatchMethod(),
                MetadataBatchRequest.getDefaultInstance(), true));

        apiMethods.add(RestApiMethod.create(HttpMethod.POST,
                "/tracdap-meta-trusted/api/v1/{tenant}/trusted/search",
                TracMetadataApiGrpc.getSearchMethod(),
                MetadataSearchRequest.getDefaultInstance(),
                "searchParams", SearchParameters.getDefaultInstance()));

        return apiMethods;
    }
}
