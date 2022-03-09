/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.gateway.proxy.rest;

import com.accenture.trac.common.exception.EStartup;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;


public class RestApiFields {

    // For now, only support simple variables, e.g. /path/to/{request.field1}/{request.field2}
    // Captures of the form {request.field=some/path/*} require more work
    public static final Pattern SEGMENT_CAPTURE_PATTERN = Pattern.compile(
            "\\A\\{(?<fieldName>\\w+(?:\\.\\w+)*)}\\Z");

    public static boolean
    isSegmentCapture(String pathSegment) {

        return SEGMENT_CAPTURE_PATTERN.matcher(pathSegment).matches();
    }

    public static List<Descriptors.FieldDescriptor>
    prepareFieldsForPathSegment(Descriptors.Descriptor requestDescriptor, String pathSegment) {

        var segmentCapture = RestApiFields.SEGMENT_CAPTURE_PATTERN.matcher(pathSegment);

        if (!segmentCapture.matches())
            return List.of();

        var fullFieldName = segmentCapture.group("fieldName");

        return prepareFieldDescriptors(requestDescriptor, fullFieldName);
    }

    public static List<Descriptors.FieldDescriptor>
    prepareFieldDescriptors(Descriptors.Descriptor messageDescriptor, String fullFieldName) {

        var fieldNames = Arrays.asList(fullFieldName.split("\\."));
        var fieldDescriptors = new ArrayList<Descriptors.FieldDescriptor>();

        for (var fieldLevel = 0; fieldLevel < fieldNames.size(); fieldLevel++) {

            var fieldName = fieldNames.get(fieldLevel);
            var fieldDescriptor = messageDescriptor.findFieldByName(fieldName);

            if (fieldDescriptor == null) {
                // TODO: Error message
                var message = String.format("Invalid URL template for Rest API: Unknown request field [%s]", fieldName);
                throw new EStartup(message);
            }

            fieldDescriptors.add(fieldDescriptor);

            if (fieldLevel < fieldNames.size() - 1)
                messageDescriptor = fieldDescriptor.getMessageType();

            if (messageDescriptor == null) {
                // TODO: Error message
                var message = String.format("Invalid URL template for Rest API: Not a nested field [%s]", fieldName);
                throw new EStartup(message);
            }
        }

        return fieldDescriptors;
    }

    public static <TRequest extends Message> Function<TRequest.Builder, Message.Builder>
    prepareSubFieldMapper(List<Descriptors.FieldDescriptor> fields) {

        if (fields.size() == 1)
            return req -> req;

        return req -> mapSubfield(req, fields);
    }

    private static <TRequest extends Message> Message.Builder
    mapSubfield(TRequest.Builder request, List<Descriptors.FieldDescriptor> fields) {

        for (var fieldLevel = 0; fieldLevel < fields.size() - 1; fieldLevel++) {

            var field = fields.get(fieldLevel);
            var subField = request.getFieldBuilder(field);

            if (subField == null) {
                subField = request.newBuilderForField(field);
                request.setField(field, subField);
            }

            request = subField;
        }

        return request;
    }
}
