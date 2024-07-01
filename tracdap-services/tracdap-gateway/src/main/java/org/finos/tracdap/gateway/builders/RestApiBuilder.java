/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.gateway.builders;

import org.finos.tracdap.common.exception.EConfig;

import com.google.api.HttpRule;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.finos.tracdap.gateway.proxy.rest.RestApiMethod;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;


public class RestApiBuilder {

    static List<RestApiMethod<?, ?>> buildAllMethods(
            Descriptors.ServiceDescriptor protoService,
            String apiPrefix, ClassLoader classLoader) {

        var methodList = new ArrayList<RestApiMethod<?, ?>>();

        var protoMethods = protoService.getMethods();

        for (var protoMethod : protoMethods) {

            var options = protoMethod.getOptions().getAllFields();
            var httpOption = options.entrySet().stream()
                    .filter(option -> option.getKey().getMessageType().equals(HttpRule.getDescriptor()))
                    .findFirst();

            if (httpOption.isPresent()) {
                var httpRule = (HttpRule) httpOption.get().getValue();
                var proxyMethod = buildMethod(protoMethod, apiPrefix, httpRule, classLoader);
                methodList.add(proxyMethod);
            }
        }

        return methodList;
    }

    static RestApiMethod<?, ?> buildMethod(
            Descriptors.MethodDescriptor protoMethod,
            String apiPrefix, HttpRule httpRule,
            ClassLoader classLoader) {

        try {

            var requestProtoType = protoMethod.getInputType();
            var requestJavaPackage = requestProtoType.getFile().getOptions().getJavaPackage();
            var requestJavaTypeName = requestJavaPackage + "." + requestProtoType.getName();
            var requestJavaType = classLoader.loadClass(requestJavaTypeName);
            var blankRequest = (Message) requestJavaType.getMethod("getDefaultInstance").invoke(null);

            var responseProtoType = protoMethod.getOutputType();
            var responseJavaPackage = responseProtoType.getFile().getOptions().getJavaPackage();
            var responseJavaTypeName= responseJavaPackage + "." + responseProtoType.getName();
            var responseJavaType = classLoader.loadClass(responseJavaTypeName);
            var blankResponse = (Message) responseJavaType.getMethod("getDefaultInstance").invoke(null);

            if (httpRule.hasGet()) {

                var urlPattern = apiPrefix + httpRule.getGet();
                var responseBody = httpRule.getResponseBody().isEmpty() ? "*" : httpRule.getResponseBody();

                return RestApiMethod.GET(
                        protoMethod, blankRequest, blankResponse,
                        urlPattern, responseBody);
            }

            if (httpRule.hasPost()) {

                var urlPattern = apiPrefix + httpRule.getPost();
                var requestBody = httpRule.getBody().isEmpty() ? "*" : httpRule.getBody();
                var responseBody = httpRule.getResponseBody().isEmpty() ? "*" : httpRule.getResponseBody();

                return RestApiMethod.POST(
                        protoMethod, blankRequest, blankResponse,
                        urlPattern, requestBody, responseBody);
            }

            throw new EConfig("REST mapping for " + protoMethod.getName() + " uses an unsupported HTTP rule type");
        }
        catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {

            throw new EConfig("REST mapping for " + protoMethod.getName() + " could not be created", e);
        }
    }
}
