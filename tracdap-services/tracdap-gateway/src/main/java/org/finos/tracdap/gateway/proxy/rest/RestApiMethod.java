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

package org.finos.tracdap.gateway.proxy.rest;

import com.google.protobuf.Message;
import io.grpc.MethodDescriptor;
import io.netty.handler.codec.http.HttpMethod;


public class RestApiMethod <TRequest extends Message, TResponse extends Message> {

    final MethodDescriptor<TRequest, TResponse> grpcMethod;
    final RestApiTranslator<TRequest, TResponse> translator;
    final RestApiMatcher matcher;
    final boolean hasBody;

    private RestApiMethod(
            MethodDescriptor<TRequest, TResponse> grpcMethod,
            RestApiTranslator<TRequest, TResponse> translator,
            RestApiMatcher matcher,
            boolean hasBody) {

        this.grpcMethod = grpcMethod;
        this.translator = translator;
        this.matcher = matcher;
        this.hasBody = hasBody;
    }

    public static <TRequest extends Message, TResponse extends Message>
    RestApiMethod<TRequest, TResponse> create(
            HttpMethod httpMethod, String urlTemplate,
            MethodDescriptor<TRequest, TResponse> grpcMethod,
            TRequest blankRequest,
            String bodyField) {

        // Matcher and builder created once and reused for all matching requests
        var matcher = new RestApiMatcher(httpMethod, urlTemplate, blankRequest);
        var translator = new RestApiTranslator<TRequest, TResponse>(urlTemplate, blankRequest, bodyField);

        return new RestApiMethod<>(grpcMethod, translator, matcher, true);
    }

    public static <TRequest extends Message, TResponse extends Message>
    RestApiMethod<TRequest, TResponse> create(
            HttpMethod httpMethod, String urlTemplate,
            MethodDescriptor<TRequest, TResponse> grpcMethod,
            TRequest blankRequest,
            boolean hasBody) {

        // Matcher and builder created once and reused for all matching requests
        var matcher = new RestApiMatcher(httpMethod, urlTemplate, blankRequest);
        var translator = new RestApiTranslator<TRequest, TResponse>(urlTemplate, blankRequest, hasBody);

        return new RestApiMethod<>(grpcMethod, translator, matcher, hasBody);
    }

    public static <TRequest extends Message, TResponse extends Message>
    RestApiMethod<TRequest, TResponse> create(
            HttpMethod method, String urlPattern,
            MethodDescriptor<TRequest, TResponse> grpcMethod, TRequest blankRequest) {

        return create(method, urlPattern, grpcMethod, blankRequest, false);
    }
}
