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

import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.exception.EUnexpected;

import com.google.gson.stream.MalformedJsonException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.StatusRuntimeException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;


public class RestApiTranslator<TRequest extends Message, TResponse extends Message> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final TRequest blankRequest;

    private final List<BiFunction<URI, TRequest.Builder, TRequest.Builder>> requestFieldExtractors;
    private final List<Descriptors.FieldDescriptor> requestBodyPath;
    private final boolean hasBody;

    public RestApiTranslator(String urlTemplate, TRequest blankRequest, String bodyField) {

        this.blankRequest = blankRequest;

        var requestDescriptor = blankRequest.getDescriptorForType();

        this.requestFieldExtractors = prepareFieldExtractors(urlTemplate, requestDescriptor);
        this.requestBodyPath = RestApiFields.prepareFieldDescriptors(requestDescriptor, bodyField);
        this.hasBody = true;
    }

    public RestApiTranslator(String urlTemplate, TRequest blankRequest, boolean hasBody) {

        this.blankRequest = blankRequest;

        var requestDescriptor = blankRequest.getDescriptorForType();

        this.requestFieldExtractors = prepareFieldExtractors(urlTemplate, requestDescriptor);
        this.requestBodyPath = List.of();
        this.hasBody = hasBody;

    }

    // -----------------------------------------------------------------------------------------------------------------
    // Runtime methods
    // -----------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public TRequest translateRequest(String url) {

        // This should be set up correctly when the API route is created
        if (this.hasBody)
            throw new EUnexpected();

        var requestUrl = URI.create(url);

        var request = blankRequest.newBuilderForType();

        for (var extractor : requestFieldExtractors)
            request = extractor.apply(requestUrl, request);

        return (TRequest) request.build();
    }

    @SuppressWarnings("unchecked")
    public TRequest translateRequest(String url, ByteBuf bodyBuffer) {

        // This should be set up correctly when the API route is created
        if (!this.hasBody)
            throw new EUnexpected();

        var requestUrl = URI.create(url);

        var request = blankRequest.newBuilderForType();

        for (var extractor : requestFieldExtractors)
            request = extractor.apply(requestUrl, request);

        var body = request;

        for (var pathElement : requestBodyPath)
            body = body.getFieldBuilder(pathElement);

        translateRequestBody(bodyBuffer, body);

        return (TRequest) request.build();
    }

    private void translateRequestBody(ByteBuf bodyBuffer, Message.Builder body) {

        try (var jsonStream = new ByteBufInputStream(bodyBuffer);
             var jsonReader = new InputStreamReader(jsonStream)) {

            var jsonParser = JsonFormat.parser();
            jsonParser.merge(jsonReader, body);
        }
        catch (InvalidProtocolBufferException e) {

            // Validation failures will go back to users (API users, i.e. application developers)
            // Strip out GSON class name from the error message for readability
            var detailMessage = e.getLocalizedMessage();
            var classNamePrefix = MalformedJsonException.class.getName() + ": ";

            if (detailMessage.startsWith(classNamePrefix))
                detailMessage = detailMessage.substring(classNamePrefix.length());

            var message = String.format(
                    "Invalid JSON input for type [%s]: %s",
                    body.getDescriptorForType().getName(),
                    detailMessage);

            log.warn(message);
            throw new EInputValidation(message, e);
        }
        catch (IOException e) {

            // Shouldn't happen, reader source is a buffer already held in memory
            log.error("Unexpected IO error reading from internal buffer", e);
            throw new EUnexpected();
        }
    }

    public String translateResponseBody(Message grpcResponseBody) throws Exception {

        return JsonFormat.printer().print(grpcResponseBody);
    }

    public HttpResponseStatus translateGrpcErrorCode(StatusRuntimeException grpcError) {

        var grpcCode = grpcError.getStatus().getCode();

        switch (grpcCode) {

            case UNAVAILABLE:
                return HttpResponseStatus.SERVICE_UNAVAILABLE;

            case UNAUTHENTICATED:
                return HttpResponseStatus.UNAUTHORIZED;

            case PERMISSION_DENIED:
                return HttpResponseStatus.FORBIDDEN;

            case INVALID_ARGUMENT:
                return HttpResponseStatus.BAD_REQUEST;

            case NOT_FOUND:
                return HttpResponseStatus.NOT_FOUND;

            case ALREADY_EXISTS:
                return HttpResponseStatus.CONFLICT;

            case FAILED_PRECONDITION:
                return HttpResponseStatus.PRECONDITION_FAILED;

            default:
                // For unrecognised errors, send error code 500 with no message
                return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }
    }

    public String translateGrpcErrorMessage(StatusRuntimeException grpcError) {

        var grpcCode = grpcError.getStatus().getCode();

        switch (grpcCode) {

            case INVALID_ARGUMENT:
            case NOT_FOUND:
            case ALREADY_EXISTS:
            case FAILED_PRECONDITION:
                return grpcError.getStatus().getDescription();

            default:
                // For unrecognised errors, send error code 500 with no message
                return null;
        }
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


    // -----------------------------------------------------------------------------------------------------------------
    // Preparation methods
    // -----------------------------------------------------------------------------------------------------------------

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

            default:
                // TODO: Error
                throw new EUnexpected();
        }
    }

    private Function<URI, String> preparePathSegmentExtractor(int pathSegmentIndex) {
        return uri -> extractPathSegment(pathSegmentIndex, uri);
    }
}
