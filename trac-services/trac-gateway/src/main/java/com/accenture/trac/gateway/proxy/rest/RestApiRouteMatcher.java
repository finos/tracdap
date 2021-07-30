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
import com.accenture.trac.gateway.routing.IRouteMatcher;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;


public class RestApiRouteMatcher implements IRouteMatcher {

    private final static Logger log = LoggerFactory.getLogger(RestApiRouteMatcher.class);

    private final HttpMethod httpMethod;
    private final List<Function<String, Boolean>> pathSegmentMatchers;

    public <TRequest extends Message>
    RestApiRouteMatcher(HttpMethod httpMethod, String urlTemplate, TRequest request) {

        this.httpMethod = httpMethod;

        var pathAndQuery = urlTemplate.split("\\?");
        var pathSegments = pathAndQuery[0].split("/");

        this.pathSegmentMatchers = Arrays.stream(pathSegments)
                .map(segment -> prepareMatcherForSegment(segment, request))
                .collect(Collectors.toList());
    }

    private <TRequest extends Message>
    Function<String, Boolean>
    prepareMatcherForSegment(String segmentTemplate, TRequest blankRequest) {

        // Segments that do not contain variables are a straight-up literal match
        if (!segmentTemplate.contains("{"))
            return segment -> RestApiRouteMatcher.matchLiteralSegment(segmentTemplate, segment);

        if (RestApiFields.isSegmentCapture(segmentTemplate)) {

            var requestDescriptor = blankRequest.getDescriptorForType();
            var segmentFields = RestApiFields.prepareFieldsForPathSegment(requestDescriptor, segmentTemplate);
            var targetField = segmentFields.get(segmentFields.size() - 1);

            return prepareMatcherForTargetField(targetField);
        }

        throw new EStartup("Invalid URL template for Rest API route matching: " + segmentTemplate);
    }

    private Function<String, Boolean>
    prepareMatcherForTargetField(Descriptors.FieldDescriptor targetField) {

        switch (targetField.getJavaType()) {

            case STRING:
                return RestApiRouteMatcher::matchStringSegment;

            case ENUM:
                var enumType = targetField.getEnumType();
                return segment -> RestApiRouteMatcher.matchEnumSegment(enumType, segment);

            case LONG:
                return RestApiRouteMatcher::matchLongSegment;

            case INT:
                return RestApiRouteMatcher::matchIntSegment;

            default:
                // TODO: Error message
                throw new EStartup("Bad rest API template");
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Runtime methods
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public boolean matches(URI uri, HttpMethod httpMethod, HttpHeaders headers) {

        if (httpMethod != this.httpMethod)
            return false;

        var pathSegments = uri.getPath().split("/");

        if (pathSegments.length != pathSegmentMatchers.size())
            return false;

        for (var segmentIndex = 0; segmentIndex < pathSegments.length; segmentIndex++) {

            var segment = pathSegments[segmentIndex];
            var matcher = pathSegmentMatchers.get(segmentIndex);

            var segmentMatch = matcher.apply(segment);

            if (!segmentMatch)
                return false;
        }

        return true;
    }

    private static boolean
    matchLiteralSegment(String literal, String pathSegment) {
        return literal.equals(pathSegment);
    }

    private static boolean
    matchStringSegment(String pathSegment) {
        return true;
    }

    private static boolean
    matchEnumSegment(Descriptors.EnumDescriptor enumType, String pathSegment) {
        return enumType.findValueByName(pathSegment.toUpperCase()) != null;
    }

    private static boolean
    matchLongSegment(String pathSegment) {

        try {
            Long.parseLong(pathSegment);
            return true;
        }
        catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean
    matchIntSegment(String pathSegment) {

        try {
            Integer.parseInt(pathSegment);
            return true;
        }
        catch (NumberFormatException e) {
            return false;
        }
    }
}
