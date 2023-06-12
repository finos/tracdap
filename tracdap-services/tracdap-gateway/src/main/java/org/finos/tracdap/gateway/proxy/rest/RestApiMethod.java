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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.netty.handler.codec.http.HttpMethod;


public class RestApiMethod <TRequest extends Message, TResponse extends Message> {

    final Descriptors.MethodDescriptor grpcMethod;
    final RestApiTranslator<TRequest, TResponse> translator;
    final RestApiMatcher matcher;
    final boolean hasBody;

    private RestApiMethod(
            Descriptors.MethodDescriptor grpcMethod,
            RestApiTranslator<TRequest, TResponse> translator,
            RestApiMatcher matcher,
            boolean hasBody) {

        this.grpcMethod = grpcMethod;
        this.translator = translator;
        this.matcher = matcher;
        this.hasBody = hasBody;
    }

    public static <TRequest extends Message, TResponse extends Message>
    RestApiMethod<TRequest, TResponse> POST(
            Descriptors.MethodDescriptor grpcMethod,
            TRequest blankRequest, TResponse blankResponse,
            String urlTemplate, String requestBody, String responseBody) {

        // Matcher and builder created once and reused for all matching requests
        var matcher = new RestApiMatcher(HttpMethod.POST, urlTemplate, blankRequest);
        var translator = new RestApiTranslator<>(blankRequest, blankResponse, urlTemplate, requestBody, responseBody);

        return new RestApiMethod<>(grpcMethod, translator, matcher, true);
    }

    public static <TRequest extends Message, TResponse extends Message>
    RestApiMethod<TRequest, TResponse> GET(
            Descriptors.MethodDescriptor grpcMethod,
            TRequest blankRequest, TResponse blankResponse,
            String urlTemplate, String responseBody) {

        // Matcher and builder created once and reused for all matching requests
        var matcher = new RestApiMatcher(HttpMethod.GET, urlTemplate, blankRequest);
        var translator = new RestApiTranslator<>(blankRequest, blankResponse, urlTemplate, null, responseBody);

        return new RestApiMethod<>(grpcMethod, translator, matcher, false);
    }
}
