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

package com.accenture.trac.gateway;

import com.accenture.trac.common.api.*;
import com.accenture.trac.common.metadata.TagSelector;
import com.accenture.trac.common.metadata.search.SearchExpression;
import com.accenture.trac.common.metadata.search.SearchParameters;
import com.accenture.trac.gateway.proxy.RestApiRequestBuilder;
import com.accenture.trac.gateway.proxy.RestApiRouteMatcher;
import com.accenture.trac.gateway.proxy.RestApiUnaryHandler;
import com.accenture.trac.gateway.routing.RoutingConfig;

import com.google.protobuf.Message;
import io.grpc.MethodDescriptor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;

import java.util.function.Supplier;


public class TracApiConfig {

    public static RoutingConfig metaApiRoutes(String serviceHost, int servicePort) {

        var apiRoutes = new RoutingConfig();

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta/api/v1/{tenant}/create-object",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getCreateObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(), true);

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta/api/v1/{tenant}/update-object",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getUpdateObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(), true);

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta/api/v1/{tenant}/update-tag",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getUpdateTagMethod(),
                MetadataWriteRequest.getDefaultInstance(), true);

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta/api/v1/{tenant}/read-object",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getReadObjectMethod(),
                MetadataReadRequest.getDefaultInstance(),
                "selector", TagSelector.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta/api/v1/{tenant}/read-batch",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getReadBatchMethod(),
                MetadataBatchRequest.getDefaultInstance(),
                "selector", TagSelector.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta/api/v1/{tenant}/search",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getSearchMethod(),
                MetadataSearchRequest.getDefaultInstance(),
                "searchParams", SearchParameters.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.GET,
                "/trac-meta/api/v1/{tenant}/{objectType}/{objectId}/versions/{objectVersion}/tags/{tagVersion}",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getGetObjectMethod(),
                MetadataGetRequest.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.GET,
                "/trac-meta/api/v1/{tenant}/{objectType}/{objectId}/versions/latest/tags/latest",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getGetLatestObjectMethod(),
                MetadataGetRequest.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.GET,
                "/trac-meta/api/v1/{tenant}/{objectType}/{objectId}/versions/{objectVersion}/tags/latest",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getGetLatestTagMethod(),
                MetadataGetRequest.getDefaultInstance());

        return apiRoutes;
    }

    public static RoutingConfig metaApiTrustedRoutes(String serviceHost, int servicePort) {

        var apiRoutes = new RoutingConfig();

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/create-object",
                serviceHost, servicePort,
                TrustedMetadataApiGrpc.getCreateObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(), true);

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/update-object",
                serviceHost, servicePort,
                TrustedMetadataApiGrpc.getUpdateObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(), true);

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/update-tag",
                serviceHost, servicePort,
                TrustedMetadataApiGrpc.getUpdateTagMethod(),
                MetadataWriteRequest.getDefaultInstance(), true);

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/preallocate",
                serviceHost, servicePort,
                TrustedMetadataApiGrpc.getPreallocateIdMethod(),
                MetadataWriteRequest.getDefaultInstance(), true);

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/create-preallocated",
                serviceHost, servicePort,
                TrustedMetadataApiGrpc.getCreatePreallocatedObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(), true);

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/read-object",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getReadObjectMethod(),
                MetadataReadRequest.getDefaultInstance(), true);

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/read-batch",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getReadBatchMethod(),
                MetadataBatchRequest.getDefaultInstance(), true);

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/search",
                serviceHost, servicePort,
                TracMetadataApiGrpc.getSearchMethod(),
                MetadataSearchRequest.getDefaultInstance(),
                "searchParams", SearchParameters.getDefaultInstance());

        return apiRoutes;
    }

    public static <TRequest extends Message, TRequestBody extends Message, TResponse extends Message>
    void addApiCall(
            RoutingConfig routes, HttpMethod method, String urlPattern,
            String serviceHost, int servicePort,
            MethodDescriptor<TRequest, TResponse> grpcMethod, TRequest blankRequest,
            String bodyElement, TRequestBody bodyTemplate) {

        // Matcher and builder created once and reused for all matching requests
        var requestMatcher = new RestApiRouteMatcher(method, urlPattern, blankRequest);
        var requestBuilder = new RestApiRequestBuilder<>(urlPattern, blankRequest, bodyElement);

        // Handler is supplied at runtime when there is a route match for the API call
        var requestHandler = wrapUnaryHandler(() ->
                new RestApiUnaryHandler<>(serviceHost, servicePort, grpcMethod, requestBuilder, bodyTemplate));

        routes.addRoute(requestMatcher, requestHandler);
    }

    public static <TRequest extends Message, TResponse extends Message>
    void addApiCall(
                    RoutingConfig routes, HttpMethod method, String urlPattern,
                    String serviceHost, int servicePort,
                    MethodDescriptor<TRequest, TResponse> grpcMethod, TRequest blankRequest,
                    boolean hasBody) {

        // Matcher and builder created once and reused for all matching requests
        var requestMatcher = new RestApiRouteMatcher(method, urlPattern, blankRequest);
        var requestBuilder = new RestApiRequestBuilder<>(urlPattern, blankRequest, hasBody);

        // Handler is supplied at runtime when there is a route match for the API call
        var requestHandler = hasBody
            ? wrapUnaryHandler(() -> new RestApiUnaryHandler<>(serviceHost, servicePort, grpcMethod, requestBuilder, blankRequest))
            : wrapUnaryHandler(() -> new RestApiUnaryHandler<>(serviceHost, servicePort, grpcMethod, requestBuilder));

        routes.addRoute(requestMatcher, requestHandler);
    }

    public static <TRequest extends Message, TResponse extends Message>
    void addApiCall(
            RoutingConfig routes, HttpMethod method, String urlPattern,
            String serviceHost, int servicePort,
            MethodDescriptor<TRequest, TResponse> grpcMethod, TRequest blankRequest) {

        addApiCall(routes, method, urlPattern, serviceHost, servicePort, grpcMethod, blankRequest, false);
    }

    private static Supplier<ChannelInboundHandler> wrapUnaryHandler(Supplier<ChannelInboundHandler> unaryHandler) {

        return () -> new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel channel) {

                var pipeline = channel.pipeline();

                pipeline.remove(this);
                pipeline.addLast(new HttpObjectAggregator(10 * 1024 * 1024));
                pipeline.addLast(unaryHandler.get());
            }
        };
    }

}
