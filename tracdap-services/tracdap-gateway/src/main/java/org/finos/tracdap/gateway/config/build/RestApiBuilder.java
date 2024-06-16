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

package org.finos.tracdap.gateway.config.build;

import org.finos.tracdap.api.Data;
import org.finos.tracdap.api.Metadata;
import org.finos.tracdap.api.Orchestrator;
import org.finos.tracdap.common.exception.ETracInternal;

import com.google.api.HttpRule;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.finos.tracdap.gateway.proxy.rest.RestApiMethod;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;


public class RestApiBuilder {

    public static List<RestApiMethod<?, ?>> metaApiRoutes() {

        return registerService(Metadata.getDescriptor(), "TracMetadataApi", "/trac-meta/api/v1");
    }

    public static List<RestApiMethod<?, ?>> dataApiRoutes() {

        return registerService(Data.getDescriptor(), "TracDataApi", "/trac-data/api/v1");
    }

    public static List<RestApiMethod<?, ?>> orchApiRoutes() {

        return registerService(Orchestrator.getDescriptor(), "TracOrchestratorApi", "/trac-orch/api/v1");
    }

    static List<RestApiMethod<?, ?>> registerService(Descriptors.FileDescriptor protoFile, String serviceName, String apiPrefix) {

        var methodList = new ArrayList<RestApiMethod<?, ?>>();

        var protoService = protoFile.findServiceByName(serviceName);
        var protoMethods = protoService.getMethods();

        for (var protoMethod : protoMethods) {

            var options = protoMethod.getOptions().getAllFields();
            var httpOption = options.entrySet().stream()
                    .filter(option -> option.getKey().getMessageType().equals(HttpRule.getDescriptor()))
                    .findFirst();

            if (httpOption.isPresent()) {
                var httpRule = (HttpRule) httpOption.get().getValue();
                var proxyMethod = registerMethod(protoMethod, apiPrefix, httpRule);
                methodList.add(proxyMethod);
            }
        }

        return methodList;
    }

    static RestApiMethod<?, ?> registerMethod(Descriptors.MethodDescriptor protoMethod, String apiPrefix, HttpRule httpRule) {

        try {

            var requestProtoType = protoMethod.getInputType();
            var requestJavaPackage = requestProtoType.getFile().getOptions().getJavaPackage();
            var requestJavaTypeName = requestJavaPackage + "." + requestProtoType.getName();
            var requestJavaType = Metadata.class.getClassLoader().loadClass(requestJavaTypeName);
            var blankRequest = (Message) requestJavaType.getMethod("getDefaultInstance").invoke(null);

            var responseProtoType = protoMethod.getOutputType();
            var responseJavaPackage = responseProtoType.getFile().getOptions().getJavaPackage();
            var responseJavaTypeName= responseJavaPackage + "." + responseProtoType.getName();
            var responseJavaType = Metadata.class.getClassLoader().loadClass(responseJavaTypeName);
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

            throw new ETracInternal("REST mapping for " + protoMethod.getName() + " uses an unsupported HTTP rule type");
        }
        catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {

            throw new ETracInternal("REST mapping for " + protoMethod.getName() + " could not be created", e);
        }
    }
}
