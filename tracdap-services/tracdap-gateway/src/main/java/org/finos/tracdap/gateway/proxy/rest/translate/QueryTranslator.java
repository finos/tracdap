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

package org.finos.tracdap.gateway.proxy.rest.translate;

import org.finos.tracdap.gateway.proxy.rest.RestApiRequest;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class QueryTranslator implements IRequestTranslator<RestApiRequest> {

    // Negative cache parameters could be exposed in config if that becomes useful
    private static final long CACHE_SIZE = 1000;
    private static final Cache<String, Boolean> negativeCache =
            CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .build();

    private final List<String> excludedFields;
    private final ConcurrentHashMap<String, IRequestTranslator<String>> translators;

    public QueryTranslator(List<String> excludedFields) {

        this.excludedFields = excludedFields;
        this.translators = new ConcurrentHashMap<>();
    }

    @Override
    public List<String> fields() {
        return List.of();
    }

    @Override
    public Message.Builder translate(Message.Builder builder, RestApiRequest request) {

        for (var param : request.queryParams().entrySet()) {

            var fieldPath = param.getKey();
            var fieldValue = param.getValue();

            // Happy path - commonly used fields will normally have a translator available
            var translator = translators.get(fieldPath);

            if (translator != null) {
                translator.translate(builder, fieldValue);
                continue;
            }

            // Negative cache avoids expensive processing for unused query params
            // This ia an LRU cache, to avoid overflow if query params change on every request
            if (negativeCache.getIfPresent(fieldPath) != null)
                continue;

            // Do not translate fields are ones with a static mapping in the HTTP rule
            if (isExcludedField(fieldPath)) {
                negativeCache.put(fieldPath, Boolean.TRUE);
                continue;
            }

            // New field path - try to build a translator
            var newTranslator = buildFieldTranslator(builder, fieldPath);

            // Fields that cannot be translated are silently ignored
            // Use the negative cache to avoid recomputing on every request
            if (newTranslator == null) {
                negativeCache.put(fieldPath, Boolean.TRUE);
                continue;
            }

            // New translator created - save and apply
            // Translators are kept forever, they are only created for valid field paths
            translators.put(fieldPath, newTranslator);
            newTranslator.translate(builder, fieldValue);
        }



        return builder;
    }

    private boolean isExcludedField(String fieldPath) {

        return excludedFields.stream().anyMatch(excludedField -> fieldPathMatches(fieldPath, excludedField));
    }

    private boolean fieldPathMatches(String fieldPath, String excludedPath) {

        var pathSegments = fieldPath.split("\\.");
        var excludedSegments = excludedPath.split("\\.");

        for (int i = 0; i < excludedSegments.length; ++i) {

            if (excludedSegments[i].equals("*"))
                return true;

            if (pathSegments.length < i)
                return true;

            if (!pathSegments[i].equals(excludedSegments[i]))
                return false;
        }

        return true;
    }

    private IRequestTranslator<String> buildFieldTranslator(Message.Builder builder, String fieldPath) {

        if (fieldPath.contains(".")) {

            var sep = fieldPath.indexOf('.');
            var fieldName = fieldPath.substring(0, sep);
            var fieldDescriptor = builder.getDescriptorForType().findFieldByName(fieldName);

            // Ignore unknown or invalid field paths
            if (fieldDescriptor == null || fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE)
                return null;

            var childBuilder = builder.getFieldBuilder(fieldDescriptor);
            var childFieldName = fieldPath.substring(sep + 1);

            return buildFieldTranslator(childBuilder, childFieldName);
        }

        var fieldDescriptor = builder.getDescriptorForType().findFieldByName(fieldPath);

        // Ignore unknown field paths
        if (fieldDescriptor == null)
            return null;

        switch (fieldDescriptor.getJavaType()) {

            case STRING:
                return new StringTranslator(fieldDescriptor);

            case ENUM:
                return new EnumTranslator(fieldDescriptor);

            case LONG:
                return new LongTranslator(fieldDescriptor);

            case INT:
                return new IntTranslator(fieldDescriptor);

            // Ignore unsupported field paths
            default:
                return null;
        }
    }
}
