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


public class RestApiMethod <
        TRequest extends Message,
        TRequestBody extends Message,
        TResponse extends Message> {

    final boolean hasBody;
    final MethodDescriptor<TRequest, TResponse> grpcMethod;

    final RestApiMatcher matcher;
    final RestApiTranslator<TRequest, TRequestBody> translator;

    private RestApiMethod(
            boolean hasBody,
            MethodDescriptor<TRequest, TResponse> grpcMethod,
            RestApiMatcher matcher,
            RestApiTranslator<TRequest, TRequestBody> translator) {

        this.hasBody = hasBody;
        this.grpcMethod = grpcMethod;

        this.matcher = matcher;
        this.translator = translator;
    }

    public static <TRequest extends Message, TRequestBody extends Message, TResponse extends Message>
    RestApiMethod<TRequest, TRequestBody, TResponse> create(
            HttpMethod httpMethod, String urlTemplate,
            MethodDescriptor<TRequest, TResponse> grpcMethod, TRequest blankRequest,
            String bodyField, TRequestBody bodyTemplate) {

        // Matcher and builder created once and reused for all matching requests
        var matcher = new RestApiMatcher(httpMethod, urlTemplate, blankRequest);
        var translator = new RestApiTranslator<>(urlTemplate, blankRequest, bodyField, bodyTemplate);

        return new RestApiMethod<>(true, grpcMethod, matcher, translator);
    }

    public static <TRequest extends Message, TResponse extends Message>
    RestApiMethod<TRequest, Message, TResponse> create(
            HttpMethod httpMethod, String urlTemplate,
            MethodDescriptor<TRequest, TResponse> grpcMethod, TRequest blankRequest,
            boolean hasBody) {

        // Matcher and builder created once and reused for all matching requests
        var matcher = new RestApiMatcher(httpMethod, urlTemplate, blankRequest);
        var translator = new RestApiTranslator<>(urlTemplate, blankRequest, hasBody);

        return new RestApiMethod<>(hasBody, grpcMethod, matcher, translator);
    }

    public static <TRequest extends Message, TResponse extends Message>
    RestApiMethod<TRequest, Message, TResponse> create(
            HttpMethod method, String urlPattern,
            MethodDescriptor<TRequest, TResponse> grpcMethod, TRequest blankRequest) {

        return create(method, urlPattern, grpcMethod, blankRequest, false);
    }
}
