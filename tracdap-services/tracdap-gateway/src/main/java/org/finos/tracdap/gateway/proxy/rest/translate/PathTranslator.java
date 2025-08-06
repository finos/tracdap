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

import com.google.common.collect.Streams;
import com.google.protobuf.Message;
import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.gateway.proxy.rest.RestApiErrors;
import org.finos.tracdap.gateway.proxy.rest.RestApiRequest;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class PathTranslator implements IRequestTranslator<RestApiRequest> {

    private final List<IRequestTranslator<String>> segmentTranslators;
    private final IRequestTranslator<List<String>> multiSegmentTranslator;

    private final List<String> fields;

    public PathTranslator(List<IRequestTranslator<String>> segmentTranslators) {

        this.segmentTranslators = segmentTranslators;
        this.multiSegmentTranslator = null;

        this.fields = buildFTranslatedFields();
    }

    public PathTranslator(
            List<IRequestTranslator<String>> segmentTranslators,
            IRequestTranslator<List<String>> multiSegmentTranslator) {

        this.segmentTranslators = segmentTranslators;
        this.multiSegmentTranslator = multiSegmentTranslator;

        this.fields = buildFTranslatedFields();
    }

    private List<String> buildFTranslatedFields() {

        var segmentFields = segmentTranslators.stream()
                .map(IRequestTranslator::fields)
                .flatMap(List::stream);

        var multiSegmentFields = multiSegmentTranslator != null
                ? multiSegmentTranslator.fields().stream()
                : Stream.<String>empty();

        return Streams
                .concat(segmentFields, multiSegmentFields)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> fields() {
        return fields;
    }

    @Override
    public Message.Builder translate(Message.Builder builder, RestApiRequest request) {

        var segments = request.pathSegments();

        if (segments.size() < segmentTranslators.size())
            throw new EInputValidation(RestApiErrors.INVALID_REQUEST_TOO_FEW_PATH_SEGMENTS);

        if (segments.size() > segmentTranslators.size() && multiSegmentTranslator == null)
            throw new EInputValidation(RestApiErrors.INVALID_REQUEST_TOO_MANY_PATH_SEGMENTS);

        for (int i = 0; i < segmentTranslators.size(); i++) {

            var translator = segmentTranslators.get(i);
            var segment = segments.get(i);

            builder = translator.translate(builder, segment);
        }

        if (multiSegmentTranslator == null)
            return builder;

        var remainingSegments = segments.subList(segmentTranslators.size(), segments.size());
        return multiSegmentTranslator.translate(builder, remainingSegments);
    }
}
