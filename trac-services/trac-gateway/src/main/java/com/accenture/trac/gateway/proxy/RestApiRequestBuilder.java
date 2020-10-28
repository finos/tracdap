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

package com.accenture.trac.gateway.proxy;

import com.accenture.trac.common.exception.EInputValidation;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;


public class RestApiRequestBuilder<TRequest extends Message> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final TRequest blankRequest;

    private final boolean hasBody;
    private final Function<TRequest.Builder, Message.Builder> bodySubFieldMapper;
    private final Descriptors.FieldDescriptor bodyFieldDescriptor;
    private final List<BiFunction<URI, TRequest.Builder, TRequest.Builder>> fieldExtractors;


    public RestApiRequestBuilder(String urlTemplate, TRequest blankRequest, String bodyField) {

        this.blankRequest = blankRequest;

        var requestDescriptor = blankRequest.getDescriptorForType();

        var bodyFields = RestApiFields.prepareFieldDescriptors(requestDescriptor, bodyField);
        this.bodySubFieldMapper = RestApiFields.prepareSubFieldMapper(bodyFields);
        this.bodyFieldDescriptor = bodyFields.get(bodyFields.size() - 1);
        this.hasBody = true;

        this.fieldExtractors = prepareFieldExtractors(urlTemplate, blankRequest.getDescriptorForType());
    }

    public RestApiRequestBuilder(String urlTemplate, TRequest blankRequest, boolean hasBody) {

        this.blankRequest = blankRequest;

        this.bodySubFieldMapper = null;
        this.bodyFieldDescriptor = null;
        this.hasBody = hasBody;

        this.fieldExtractors = prepareFieldExtractors(urlTemplate, blankRequest.getDescriptorForType());
    }

    private List<BiFunction<URI, TRequest.Builder, TRequest.Builder>>
    prepareFieldExtractors(String urlTemplate, Descriptors.Descriptor requestDescriptor) {

        var pathAndQuery = urlTemplate.split("\\?");
        var pathTemplate = pathAndQuery[0];
        var pathSegments = pathTemplate.split("/");

        var extractors = new ArrayList<BiFunction<URI, TRequest.Builder, TRequest.Builder>>();

        for (var segmentIndex = 0; segmentIndex < pathSegments.length; segmentIndex++) {

            var segment = pathSegments[segmentIndex];

            // If segment does not reference a variable, then there's nothing to extract
            if (!segment.contains("{"))
                continue;

            if (RestApiFields.isSegmentCapture(segment)) {

                var segmentFields = RestApiFields.prepareFieldsForPathSegment(requestDescriptor, segment);
                var targetField = segmentFields.get(segmentFields.size() - 1);

                var pathExtractor = preparePathSegmentExtractor(segmentIndex);
                var subFieldMapper = RestApiFields.prepareSubFieldMapper(segmentFields);

                var extractor = prepareExtractorForTargetField(pathExtractor, subFieldMapper, targetField);
                extractors.add(extractor);
            }

            else
                // TODO: Error
                throw new RuntimeException("");
        }

        return extractors;
    }

    private BiFunction<URI, TRequest.Builder, TRequest.Builder>
    prepareExtractorForTargetField(
            Function<URI, String> rawValueExtractor,
            Function<TRequest.Builder, Message.Builder> subFieldMapper,
            Descriptors.FieldDescriptor targetField) {

        switch (targetField.getJavaType()) {

            case STRING:
                return (url, request) -> extractString(rawValueExtractor, subFieldMapper, targetField, url, request);

            case ENUM:
                return (url, request) -> extractEnum(rawValueExtractor, subFieldMapper, targetField, url, request);

            case LONG:
                return (url, request) -> extractLong(rawValueExtractor, subFieldMapper, targetField, url, request);

            case INT:
                return (url, request) -> extractInt(rawValueExtractor, subFieldMapper, targetField, url, request);

            case MESSAGE:
                return prepareExtractorForObjectType(rawValueExtractor, subFieldMapper, targetField);

            default:
                // TODO: Error
                throw new EUnexpected();
        }
    }

    private BiFunction<URI, TRequest.Builder, TRequest.Builder>
    prepareExtractorForObjectType(
            Function<URI, String> rawValueExtractor,
            Function<TRequest.Builder, Message.Builder> subFieldMapper,
            Descriptors.FieldDescriptor targetField) {

        var objectType = targetField.getMessageType();

        if (objectType.equals(com.accenture.trac.common.metadata.UUID.getDescriptor()))
            return (url, request) -> extractUuid(rawValueExtractor, subFieldMapper, targetField, url, request);

        // TODO: Error
        throw new EUnexpected();
    }

    private Function<URI, String> preparePathSegmentExtractor(int pathSegmentIndex) {
        return uri -> extractPathSegment(pathSegmentIndex, uri);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Runtime methods
    // -----------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public TRequest build(String url, Message body) {

        // This should be set up correctly when the API route is created
        if (!this.hasBody)
            throw new EUnexpected();

        var request = blankRequest.newBuilderForType();

        // If the body is a sub file, use the sub field mapper to add it to the request
        if (bodySubFieldMapper != null) {

            var bodySubField = bodySubFieldMapper.apply(request);
            bodySubField.setField(bodyFieldDescriptor, body);
        }
        // Otherwise the body is the top level request, merge it before applying URL fields
        else
            request.mergeFrom(body);

        var requestUrl = URI.create(url);

        for (var extractor : fieldExtractors)
            request = extractor.apply(requestUrl, request);

        return (TRequest) request.build();
    }

    @SuppressWarnings("unchecked")
    public TRequest build(String url) {

        // This should be set up correctly when the API route is created
        if (this.hasBody)
            throw new EUnexpected();

        var request = blankRequest.newBuilderForType();

        var requestUrl = URI.create(url);

        for (var extractor : fieldExtractors)
            request = extractor.apply(requestUrl, request);

        return (TRequest) request.build();
    }

    private String extractPathSegment(int pathSegmentIndex, URI uri) {

        var pathSegments = uri.getPath().split("/");

        // This should be checked by the router matcher before a request is given to the handler
        if (pathSegmentIndex >= pathSegments.length)
            throw new EUnexpected();

        return pathSegments[pathSegmentIndex];
    }

    private TRequest.Builder extractString(
            Function<URI, String> rawValueExtractor,
            Function<TRequest.Builder, Message.Builder> subFieldMapper,
            Descriptors.FieldDescriptor targetField,
            URI uri, TRequest.Builder request) {

        var rawValue = rawValueExtractor.apply(uri);
        var stringValue = URLDecoder.decode(rawValue, StandardCharsets.US_ASCII);

        var subField = subFieldMapper.apply(request);
        subField.setField(targetField, stringValue);

        return request;
    }

    private TRequest.Builder extractEnum(
            Function<URI, String> rawValueExtractor,
            Function<TRequest.Builder, Message.Builder> subFieldMapper,
            Descriptors.FieldDescriptor targetField,
            URI uri, TRequest.Builder request) {

        var rawValue = rawValueExtractor.apply(uri);
        var stringValue = URLDecoder.decode(rawValue, StandardCharsets.US_ASCII);

        var enumType = targetField.getEnumType();
        var enumValue = enumType.findValueByName(stringValue.toUpperCase());

        // This should be checked by RestApiRouteMatcher before a request is given to the handler
        if (enumValue == null)
            throw new EUnexpected();

        var subField = subFieldMapper.apply(request);
        subField.setField(targetField, enumValue);

        return request;
    }

    private TRequest.Builder extractLong(
            Function<URI, String> rawValueExtractor,
            Function<TRequest.Builder, Message.Builder> subFieldMapper,
            Descriptors.FieldDescriptor targetField,
            URI uri, TRequest.Builder request) {

        var rawValue = rawValueExtractor.apply(uri);
        var stringValue = URLDecoder.decode(rawValue, StandardCharsets.US_ASCII);

        try {
            var longValue = Long.parseLong(stringValue);
            var subField = subFieldMapper.apply(request);
            subField.setField(targetField, longValue);

            return request;
        }
        catch (NumberFormatException e) {
            // Invalid values should not make it past the router matcher
            throw new EUnexpected();
        }
    }

    private TRequest.Builder extractInt(
            Function<URI, String> rawValueExtractor,
            Function<TRequest.Builder, Message.Builder> subFieldMapper,
            Descriptors.FieldDescriptor targetField,
            URI uri, TRequest.Builder request) {

        var rawValue = rawValueExtractor.apply(uri);
        var stringValue = URLDecoder.decode(rawValue, StandardCharsets.US_ASCII);

        try {
            var intValue = Integer.parseInt(stringValue);
            var subField = subFieldMapper.apply(request);
            subField.setField(targetField, intValue);

            return request;
        }
        catch (NumberFormatException e) {
            // Invalid values should not make it past the router matcher
            throw new EUnexpected();
        }
    }

    private TRequest.Builder extractUuid(
            Function<URI, String> rawValueExtractor,
            Function<TRequest.Builder, Message.Builder> subFieldMapper,
            Descriptors.FieldDescriptor targetField,
            URI uri, TRequest.Builder request) {

        var rawValue = rawValueExtractor.apply(uri);
        var stringValue = URLDecoder.decode(rawValue, StandardCharsets.US_ASCII);

        try {

            var nativeValue = java.util.UUID.fromString(stringValue);
            var protoValue = MetadataCodec.encode(nativeValue);

            var subField = subFieldMapper.apply(request);
            subField.setField(targetField, protoValue);

            return request;
        }
        catch (IllegalArgumentException e) {

            var message = String.format("Invalid object ID in URL: [%s]", stringValue);
            log.warn(message);

            throw new EInputValidation(message, e);
        }
    }
}
