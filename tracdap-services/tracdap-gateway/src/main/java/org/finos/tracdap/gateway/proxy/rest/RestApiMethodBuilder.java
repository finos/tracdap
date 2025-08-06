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

package org.finos.tracdap.gateway.proxy.rest;

import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.gateway.proxy.rest.match.*;
import org.finos.tracdap.gateway.proxy.rest.translate.*;
import org.finos.tracdap.api.DownloadResponse;

import com.google.api.HttpRule;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class RestApiMethodBuilder {

    public static final String SEPARATOR = "/";
    public static final String PERIOD = ".";

    public static final String DOUBLE_WILDCARD = "**";
    public static final String WILDCARD = "*";
    public static final String LEFT_BRACE = "{";
    public static final Pattern VARIABLE_SEGMENT = Pattern.compile("^\\{([^{}=]+)(=[^{}=]+)?}");


    public static List<RestApiMethod> buildService(
            Descriptors.ServiceDescriptor serviceDescriptor,
            String apiPrefix, ClassLoader classLoader) {

        var methodList = new ArrayList<RestApiMethod>();

        var protoMethods = serviceDescriptor.getMethods();

        for (var protoMethod : protoMethods) {

            var options = protoMethod.getOptions().getAllFields();
            var httpOption = options.entrySet().stream()
                    .filter(option -> option.getKey().getMessageType().equals(HttpRule.getDescriptor()))
                    .findFirst();

            if (httpOption.isPresent()) {

                var httpRule = (HttpRule) httpOption.get().getValue();
                var restMethod = buildMethod(protoMethod, httpRule, apiPrefix, classLoader);
                methodList.add(restMethod);

                // Add extra mappings for additional bindings (further nesting not allowed)
                for (int i = 0; i < httpRule.getAdditionalBindingsCount(); i++) {
                    var altHttpRule = httpRule.getAdditionalBindings(i);
                    var altRestMethod = buildMethod(protoMethod, altHttpRule, apiPrefix, classLoader);
                    methodList.add(altRestMethod);
                }
            }
        }

        return methodList;
    }

    public static RestApiMethod buildMethod(
            Descriptors.MethodDescriptor methodDescriptor,
            HttpRule httpRule, String apiPrefix,
            ClassLoader classLoader) {

        if (httpRule.hasGet())

            return buildRequestMatcher(
                    methodDescriptor, httpRule, HttpMethod.GET.name(),
                    httpRule.getGet(), apiPrefix,
                    classLoader);

        if (httpRule.hasPost())

            return buildRequestMatcher(
                    methodDescriptor, httpRule,
                    HttpMethod.POST.name(), httpRule.getPost(), apiPrefix,
                    classLoader);

        if (httpRule.hasPut())

            return buildRequestMatcher(
                    methodDescriptor, httpRule,
                    HttpMethod.PUT.name(), httpRule.getPut(), apiPrefix,
                    classLoader);

        if (httpRule.hasPatch())

            return buildRequestMatcher(
                    methodDescriptor, httpRule,
                    HttpMethod.PATCH.name(), httpRule.getPatch(), apiPrefix,
                    classLoader);

        if (httpRule.hasDelete())

            return buildRequestMatcher(
                    methodDescriptor, httpRule,
                    HttpMethod.DELETE.name(), httpRule.getDelete(), apiPrefix,
                    classLoader);

        if (httpRule.hasCustom())

            return buildRequestMatcher(
                    methodDescriptor, httpRule,
                    httpRule.getCustom().getKind(), httpRule.getCustom().getPath(), apiPrefix,
                    classLoader);

        var message = String.format(
                RestApiErrors.INVALID_MAPPING_BAD_HTTP_RULE, methodDescriptor.getName(),
                "HTTP rule does not match any known HTTP method");

        throw new EStartup(message);
    }

    private static RestApiMethod buildRequestMatcher(
            Descriptors.MethodDescriptor methodDescriptor, HttpRule httpRule,
            String httpMethod, String pathTemplate, String apiPrefix,
            ClassLoader classLoader) {

        try {

            var requestProtoType = methodDescriptor.getInputType();
            var requestJavaPackage = requestProtoType.getFile().getOptions().getJavaPackage();
            var requestJavaTypeName = requestJavaPackage + "." + requestProtoType.getName();
            var requestJavaType = classLoader.loadClass(requestJavaTypeName);
            var request = (Message) requestJavaType.getMethod("getDefaultInstance").invoke(null);

            var responseProtoType = methodDescriptor.getOutputType();
            var responseJavaPackage = responseProtoType.getFile().getOptions().getJavaPackage();
            var responseJavaTypeName = responseJavaPackage + "." + responseProtoType.getName();
            var responseJavaType = classLoader.loadClass(responseJavaTypeName);
            var response = (Message) responseJavaType.getMethod("getDefaultInstance").invoke(null);

            var methodMatcher = new LiteralMatcher(httpMethod);

            var pathHandlers = buildPathHandlers(methodDescriptor, pathTemplate, apiPrefix);
            var pathMatcher = new PathMatcher(pathHandlers.pathMatchers, pathHandlers.pathMultiMatcher);
            var pathTranslator = new PathTranslator(pathHandlers.pathTranslators);

            var hasBody = !httpRule.getBody().isEmpty();
            var bodyTranslator = hasBody
                    ? buildBodyTranslator(methodDescriptor, methodDescriptor.getInputType(), httpRule.getBody())
                    : new NoOpTranslator<ByteBuf>();

            var queryExcludedFields = Stream.concat(
                    pathTranslator.fields().stream(),
                    bodyTranslator.fields().stream())
                    .collect(Collectors.toList());

            var queryTranslator = new QueryTranslator(queryExcludedFields);

            var responseBodyTranslator = ! httpRule.getResponseBody().isEmpty()
                    ? buildResponseBodyTranslator(methodDescriptor, methodDescriptor.getOutputType(), httpRule.getResponseBody())
                    : new JsonResponseTranslator();

            // Special case handling for download requests
            var isDownload = response instanceof DownloadResponse;

            var requestMatcher = new RequestMatcher(methodMatcher, pathMatcher);
            var requestTranslator = new RequestTranslator(request::newBuilderForType, pathTranslator, bodyTranslator, queryTranslator);
            var responseTranslator = new ResponseTranslator(response::newBuilderForType, responseBodyTranslator);

            return new RestApiMethod(
                    methodDescriptor,
                    requestMatcher,
                    requestTranslator,
                    responseTranslator,
                    hasBody,
                    isDownload);
        }
        catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {

            throw new EStartup("REST mapping for " + methodDescriptor.getName() + " could not be created", e);
        }
    }

    private static RequestHandlers buildPathHandlers(
            Descriptors.MethodDescriptor methodDescriptor,
            String pathTemplate, String apiPrefix) {

        if (pathTemplate == null || !pathTemplate.startsWith(SEPARATOR)) {

            var message = String.format(
                    RestApiErrors.INVALID_MAPPING_BAD_TEMPLATE, methodDescriptor.getName(),
                    "Path template cannot be empty and must start with /");

            throw new EStartup(message);
        }

        var handlers = new RequestHandlers();

        // API prefix is all literal segments (not part of the HTTP rule)
        if (apiPrefix != null && !apiPrefix.isEmpty()) {

            if (!apiPrefix.startsWith(SEPARATOR))
                throw new EStartup("Invalid api prefix: " + apiPrefix);

            var prefixSegments = apiPrefix.substring(1).split(SEPARATOR);

            for (var segment : prefixSegments) {
                handlers.pathMatchers.add(new LiteralMatcher(segment));
                handlers.pathTranslators.add(new NoOpTranslator<>());
            }
        }

        buildSegmentHandlers(methodDescriptor, pathTemplate.substring(1), handlers);

        return handlers;
    }

    private static RequestHandlers buildSegmentHandlers(
            Descriptors.MethodDescriptor methodDescriptor,
            String pathTemplate, RequestHandlers handlers) {

        // Doing pathTemplate.split("/") does not account for variables with nested segments
        // Instead, consume the path segment by segment to ensure each segment is valid / complete

        int segmentLength;

        if (pathTemplate.startsWith(DOUBLE_WILDCARD)) {

            var message = String.format(
                    RestApiErrors.INVALID_MAPPING_BAD_TEMPLATE, methodDescriptor.getName(),
                    "Multi-segment capture is not currently supported for " + pathTemplate);

            throw new EStartup(message);
        }
        else if (pathTemplate.startsWith(WILDCARD)) {

            handlers.pathMatchers.add(new StringMatcher());
            handlers.pathTranslators.add(new NoOpTranslator<>());

            segmentLength = WILDCARD.length();
        }
        else if (pathTemplate.startsWith(LEFT_BRACE)) {

            var variable = VARIABLE_SEGMENT.matcher(pathTemplate);

            if (!variable.find() || variable.groupCount() != 2) {

                var message = String.format(
                        RestApiErrors.INVALID_MAPPING_BAD_TEMPLATE, methodDescriptor.getName(),
                        "Invalid variable capture stating at " + pathTemplate);

                throw new EStartup(message);
            }

            var fieldPath = variable.group(1);
            var nestedSegment = variable.group(2);

            // Currently no support for nested segments {var=foo/*}
            if (nestedSegment != null) {

                var message = String.format(
                        RestApiErrors.INVALID_MAPPING_BAD_TEMPLATE, methodDescriptor.getName(),
                        "Nested variable expansion is not currently supported for " + pathTemplate);

                throw new EStartup(message);
            }

            buildVariableHandler(methodDescriptor, methodDescriptor.getInputType(), fieldPath, handlers);

            segmentLength = variable.end();
        }
        else {

            var separator = pathTemplate.indexOf(SEPARATOR);

            if (separator >= 0) {

                var literal = pathTemplate.substring(0, separator);
                var remaining = pathTemplate.substring(separator + 1);
                handlers.pathMatchers.add(new LiteralMatcher(literal));
                handlers.pathTranslators.add(new NoOpTranslator<>());

                return buildSegmentHandlers(methodDescriptor, remaining, handlers);
            }
            else {

                handlers.pathMatchers.add(new LiteralMatcher(pathTemplate));
                handlers.pathTranslators.add(new NoOpTranslator<>());

                return handlers;
            }
        }

        if (pathTemplate.length() == segmentLength)
            return handlers;

        if (pathTemplate.length() == segmentLength + 1 || pathTemplate.charAt(segmentLength) != '/') {

            var message = String.format(
                    RestApiErrors.INVALID_MAPPING_BAD_TEMPLATE, methodDescriptor.getName(),
                    "Path template does not end with a valid segment for " + pathTemplate);

            throw new EStartup(message);
        }

        var pathRemaining = pathTemplate.substring(segmentLength + 1);
        return buildSegmentHandlers(methodDescriptor, pathRemaining, handlers);
    }

    private static void buildVariableHandler(
            Descriptors.MethodDescriptor methodDescriptor,
            Descriptors.Descriptor messageDescriptor,
            String fieldPath, RequestHandlers handlers) {

        if (fieldPath.contains(PERIOD)) {

            var sep =  fieldPath.indexOf(PERIOD);
            var fieldName = fieldPath.substring(0, sep);
            var childPath = fieldPath.substring(sep + 1);

            var childDescriptor = findFieldDescriptor(methodDescriptor, messageDescriptor, fieldName);

            if (childDescriptor.getType() != Descriptors.FieldDescriptor.Type.MESSAGE)
                throw requiredMessageType(methodDescriptor,fieldPath, "body");

            buildVariableHandler(methodDescriptor, childDescriptor.getMessageType(), childPath, handlers);

            var childTranslator = handlers.pathTranslators.remove(handlers.pathTranslators.size() - 1);
            handlers.pathTranslators.add(new SubFieldTranslator<>(childDescriptor, childTranslator));
        }
        else {

            var fieldDescriptor = findFieldDescriptor(methodDescriptor, messageDescriptor, fieldPath);

            switch (fieldDescriptor.getJavaType()) {

                case STRING:
                    handlers.pathMatchers.add(new StringMatcher());
                    handlers.pathTranslators.add(new StringTranslator(fieldDescriptor));
                    break;

                case ENUM:
                    handlers.pathMatchers.add(new EnumMatcher(fieldDescriptor.getEnumType()));
                    handlers.pathTranslators.add(new EnumTranslator(fieldDescriptor));
                    break;

                case LONG:
                    handlers.pathMatchers.add(new LongMatcher());
                    handlers.pathTranslators.add(new LongTranslator(fieldDescriptor));
                    break;

                case INT:
                    handlers.pathMatchers.add(new IntMatcher());
                    handlers.pathTranslators.add(new IntTranslator(fieldDescriptor));
                    break;

                default:
                    throw unsupportedFieldType(methodDescriptor, messageDescriptor, fieldPath, "path variable");
            }
        }
    }

    private static IRequestTranslator<ByteBuf> buildBodyTranslator(
            Descriptors.MethodDescriptor methodDescriptor,
            Descriptors.Descriptor messageDescriptor,
            String fieldPath) {

        if (WILDCARD.equals(fieldPath)) {

            return new JsonRequestTranslator();
        }
        else if (fieldPath.contains(PERIOD)) {

            var sep =  fieldPath.indexOf(PERIOD);
            var filedName = fieldPath.substring(0, sep);
            var childPath = fieldPath.substring(sep + 1);

            var childDescriptor = findFieldDescriptor(methodDescriptor, messageDescriptor, filedName);

            if (childDescriptor.getType() != Descriptors.FieldDescriptor.Type.MESSAGE)
                throw requiredMessageType(methodDescriptor, fieldPath, "body");

            var childTranslator = buildBodyTranslator(methodDescriptor, childDescriptor.getMessageType(), childPath);
            return new SubFieldTranslator<>(childDescriptor, childTranslator);
        }
        else {

            var fieldDescriptor = findFieldDescriptor(methodDescriptor, messageDescriptor, fieldPath);

            if (fieldDescriptor.getType() != Descriptors.FieldDescriptor.Type.MESSAGE)
                throw requiredMessageType(methodDescriptor, fieldPath, "body");

            return new SubFieldTranslator<>(fieldDescriptor, new JsonRequestTranslator());
        }
    }

    private static IResponseTranslator<Message> buildResponseBodyTranslator(
            Descriptors.MethodDescriptor methodDescriptor,
            Descriptors.Descriptor messageDescriptor,
            String fieldPath) {

        if (fieldPath.isEmpty() || fieldPath.equals(WILDCARD)) {

            return new JsonResponseTranslator();
        }
        else if (fieldPath.contains(PERIOD)) {

            var sep =  fieldPath.indexOf(PERIOD);
            var filedName = fieldPath.substring(0, sep);
            var childPath = fieldPath.substring(sep + 1);

            var childDescriptor = findFieldDescriptor(methodDescriptor, messageDescriptor, filedName);

            if (childDescriptor.getType() != Descriptors.FieldDescriptor.Type.MESSAGE)
                throw requiredMessageType(methodDescriptor, fieldPath, "response body");

            var childTranslator = buildResponseBodyTranslator(methodDescriptor, childDescriptor.getMessageType(), childPath);
            return new SubFieldResponseTranslator<>(childDescriptor, childTranslator);
        }
        else {

            var fieldDescriptor = findFieldDescriptor(methodDescriptor, messageDescriptor, fieldPath);

            if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE)
                return new SubFieldResponseTranslator<>(fieldDescriptor, new JsonResponseTranslator());

            if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.BYTES)
                return new SubFieldResponseTranslator<>(fieldDescriptor, new BinaryResponseTranslator());

            throw unsupportedFieldType(methodDescriptor, messageDescriptor, fieldPath, "response body");
        }
    }

    private static Descriptors.FieldDescriptor findFieldDescriptor(
            Descriptors.MethodDescriptor methodDescriptor,
            Descriptors.Descriptor messageDescriptor,
            String fieldName) {

        var fieldDescriptor = messageDescriptor.findFieldByName(fieldName);

        if (fieldDescriptor == null) {

            var message = String.format(
                    RestApiErrors.INVALID_MAPPING_UNKNOWN_FIELD,
                    fieldName, messageDescriptor.getName(), methodDescriptor.getName());

            throw new EStartup(message);
        }

        return fieldDescriptor;
    }

    private static EStartup requiredMessageType(
            Descriptors.MethodDescriptor methodDescriptor,
            String fieldName, String operation) {

        var message = String.format(
                RestApiErrors.INVALID_MAPPING_BAD_FIELD_TYPE,
                fieldName, operation, Descriptors.FieldDescriptor.Type.MESSAGE,
                methodDescriptor.getName());

        return new EStartup(message);
    }

    private static EStartup unsupportedFieldType(
            Descriptors.MethodDescriptor methodDescriptor,
            Descriptors.Descriptor messageDescriptor,
            String fieldName, String operation) {

        var fieldDescriptor = messageDescriptor.findFieldByName(fieldName);

        var message = String.format(
                RestApiErrors.INVALID_MAPPING_BAD_FIELD_TYPE,
                fieldName, operation,
                fieldDescriptor.getType().name(),
                methodDescriptor.getName());

        return new EStartup(message);
    }

    private static class RequestHandlers {

        List<Matcher<String>> pathMatchers = new ArrayList<>();
        Matcher<List<String>> pathMultiMatcher = null;

        List<IRequestTranslator<String>> pathTranslators = new ArrayList<>();
    }
}
