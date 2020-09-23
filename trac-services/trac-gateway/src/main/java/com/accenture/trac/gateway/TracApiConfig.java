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

import com.accenture.trac.common.api.meta.*;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.common.metadata.search.SearchExpression;
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
                "/trac-meta/api/v1/{tenant}/{objectType}/new-object",
                serviceHost, servicePort,
                MetadataPublicWriteApiGrpc.getSaveNewObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(),
                "tag", Tag.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta/api/v1/{tenant}/{objectType}/new-version",
                serviceHost, servicePort,
                MetadataPublicWriteApiGrpc.getSaveNewVersionMethod(),
                MetadataWriteRequest.getDefaultInstance(),
                "tag", Tag.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta/api/v1/{tenant}/{objectType}/new-tag",
                serviceHost, servicePort,
                MetadataPublicWriteApiGrpc.getSaveNewTagMethod(),
                MetadataWriteRequest.getDefaultInstance(),
                "tag", Tag.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.GET,
                "/trac-meta/api/v1/{tenant}/{objectType}/{objectId}/versions/{objectVersion}/tags/{tagVersion}",
                serviceHost, servicePort,
                MetadataReadApiGrpc.getLoadTagMethod(),
                MetadataReadRequest.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.GET,
                "/trac-meta/api/v1/{tenant}/{objectType}/{objectId}/versions/{objectVersion}/tags/latest",
                serviceHost, servicePort,
                MetadataReadApiGrpc.getLoadLatestTagMethod(),
                MetadataReadRequest.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.GET,
                "/trac-meta/api/v1/{tenant}/{objectType}/{objectId}/versions/latest/tags/latest",
                serviceHost, servicePort,
                MetadataReadApiGrpc.getLoadLatestObjectMethod(),
                MetadataReadRequest.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta/api/v1/{tenant}/{searchParams.objectType}/search?priorVersions={searchParams.priorVersions}",
                serviceHost, servicePort,
                MetadataSearchApiGrpc.getSearchMethod(),
                MetadataSearchRequest.getDefaultInstance(),
                "searchParams.search", SearchExpression.getDefaultInstance());

        return apiRoutes;
    }

    public static RoutingConfig metaApiTrustedRoutes(String serviceHost, int servicePort) {

        var apiRoutes = new RoutingConfig();

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/{objectType}/new-object",
                serviceHost, servicePort,
                MetadataTrustedWriteApiGrpc.getSaveNewObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(),
                "tag", Tag.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/{objectType}/new-version",
                serviceHost, servicePort,
                MetadataTrustedWriteApiGrpc.getSaveNewVersionMethod(),
                MetadataWriteRequest.getDefaultInstance(),
                "tag", Tag.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/{objectType}/new-tag",
                serviceHost, servicePort,
                MetadataTrustedWriteApiGrpc.getSaveNewTagMethod(),
                MetadataWriteRequest.getDefaultInstance(),
                "tag", Tag.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/{objectType}/preallocate",
                serviceHost, servicePort,
                MetadataTrustedWriteApiGrpc.getPreallocateIdMethod(),
                MetadataWriteRequest.getDefaultInstance(),
                "tag", Tag.getDefaultInstance());

        TracApiConfig.addApiCall(apiRoutes, HttpMethod.POST,
                "/trac-meta-trusted/api/v1/{tenant}/trusted/{objectType}/save-preallocated",
                serviceHost, servicePort,
                MetadataTrustedWriteApiGrpc.getSavePreallocatedObjectMethod(),
                MetadataWriteRequest.getDefaultInstance(),
                "tag", Tag.getDefaultInstance());

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
            MethodDescriptor<TRequest, TResponse> grpcMethod, TRequest blankRequest) {

        // Matcher and builder created once and reused for all matching requests
        var requestMatcher = new RestApiRouteMatcher(method, urlPattern, blankRequest);
        var requestBuilder = new RestApiRequestBuilder<>(urlPattern, blankRequest);

        // Handler is supplied at runtime when there is a route match for the API call
        var requestHandler = wrapUnaryHandler(() ->
                new RestApiUnaryHandler<>(serviceHost, servicePort, grpcMethod, requestBuilder));

        routes.addRoute(requestMatcher, requestHandler);
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
