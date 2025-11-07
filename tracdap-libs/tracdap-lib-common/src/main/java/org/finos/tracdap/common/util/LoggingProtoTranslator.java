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

package org.finos.tracdap.common.util;

import com.google.protobuf.MessageOrBuilder;
import org.finos.tracdap.api.*;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.metadata.TagSelector;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


/**
 * <p>This translator is used to translate requests / response protobuf messages
 * in the API interceptors. Any types that have a translation available
 * will be logged. Both request and response messages are considered.
 * For streaming calls, only the first message in the stream is logged
 * (if there is a translator available).
 *
 * <p>The default translator provided by createDefault() logs only basic information,
 * it is intended to allow tracing requests for debugging and does not include any
 * sensitive details. Additional logging can be added by adding or overriding
 * translators for specific message types. If additional logging is added, care must
 * be taken to ensure no sensitive details are nut included in the log output.
 */
public class LoggingProtoTranslator {

    public static List<Class<? extends Message>> DEFAULT_MESSAGE_TYPES = List.of(
            MetadataReadRequest.class,
            DataReadRequest.class,
            FileReadRequest.class,
            TagSelector.class
    );

    public static Builder createEmpty() {

        return new Builder();
    }

    public static Builder createDefault() {

        var builder = createEmpty();

        for (var messageType : DEFAULT_MESSAGE_TYPES)
            builder.add(messageType, LoggingProtoTranslator::translateDefault);

        builder.add(TagHeader.class, LoggingProtoTranslator::translateTagHeader);

        return builder;
    }

    public static class Builder {

        private final Map<Class<?>, Function<Message, String>> translators;

        private Builder() {
            translators = new HashMap<>();
        }

        @SuppressWarnings("unchecked")
        public <T extends Message> Builder add(Class<T> messageType, Function<T, String> translator) {

            translators.put(messageType, (Function<Message, String>) translator);
            return this;
        }

        public LoggingProtoTranslator build() {
            return new LoggingProtoTranslator(Collections.unmodifiableMap(translators));
        }
    }

    private final Map<Class<?>, Function<Message, String>> translators;

    private LoggingProtoTranslator(Map<Class<?>, Function<Message, String>> translators) {
        this.translators = translators;
    }

    public String formatMessage(Message message) {

        return formatMessage(message, false);
    }

    public String formatMessage(Message message, boolean logNulls) {

        if (message == null) {
            return logNulls ? "null" : null;
        }

        var translator = translators.get(message.getClass());

        if (translator == null) {
            return logNulls ? message.getClass().getName() : null;
        }

        return translator.apply(message);
    }

    private static String translateDefault(MessageOrBuilder message) {
        try {
            return printer.print(message);
        }
        catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    private static String translateTagHeader(TagHeader tagHeader) {

        // Version numbers are enough to identify an object
        var loggable = tagHeader.toBuilder()
                .clearObjectTimestamp()
                .clearTagTimestamp()
                .clearIsLatestObject()
                .clearIsLatestTag();

        return translateDefault(loggable);
    }

    private static final JsonFormat.Printer printer = JsonFormat.printer()
            .preservingProtoFieldNames()
            .omittingInsignificantWhitespace();
}
