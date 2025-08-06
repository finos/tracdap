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

import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.exception.ETracInternal;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.finos.tracdap.gateway.proxy.rest.RestApiErrors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;


public class JsonRequestTranslator implements IRequestTranslator<ByteBuf> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public Message.Builder translate(Message.Builder builder, ByteBuf bodyBuffer) {

        try (var jsonStream = new ByteBufInputStream(bodyBuffer);
             var jsonReader = new InputStreamReader(jsonStream)) {

            var jsonParser = JsonFormat.parser();
            jsonParser.merge(jsonReader, builder);

            return builder;
        }
        catch (InvalidProtocolBufferException e) {

            var errorDetails = sanitizeErrorMessage(e.getMessage());
            var message = String.format(RestApiErrors.INVALID_REQUEST_BAD_JSON_CONTENT, errorDetails);

            log.error(message);
            throw new EInputValidation(message, e);
        }
        catch (IOException e) {

            // Shouldn't happen, reader source is a buffer already held in memory
            log.error("Unexpected IO error reading from internal buffer", e);
            throw new ETracInternal("Unexpected IO error reading from internal buffer", e);
        }
    }

    @Override
    public List<String> fields(){
        return List.of("*");
    }

    private String sanitizeErrorMessage(String message) {

        // Some error messages from JSON parsing include the exception class name
        // Clean these up to make errors more readable for users

        if (message.contains(GSON_ERROR_PREFIX))
            return message.replace(GSON_ERROR_PREFIX, "");
        else
            return message;
    }

    private static final String GSON_ERROR_PREFIX = "com.google.gson.stream.MalformedJsonException: ";
}
