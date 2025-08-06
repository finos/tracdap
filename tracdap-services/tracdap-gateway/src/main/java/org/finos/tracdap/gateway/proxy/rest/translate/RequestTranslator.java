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

import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;

import java.util.function.Supplier;


public class RequestTranslator {

    private final Supplier<Message.Builder> requestFactory;

    private final PathTranslator pathTranslator;
    private final IRequestTranslator<ByteBuf> requestBodyTranslator;
    private final QueryTranslator queryTranslator;

    public RequestTranslator(
            Supplier<Message.Builder> requestFactory,
            PathTranslator pathTranslator,
            IRequestTranslator<ByteBuf> requestBodyTranslator,
            QueryTranslator queryTranslator) {

        this.requestFactory = requestFactory;
        this.pathTranslator = pathTranslator;
        this.requestBodyTranslator = requestBodyTranslator;
        this.queryTranslator = queryTranslator;
    }

    public Message translateRequest(RestApiRequest request) {

        var builder = requestFactory.get();

        pathTranslator.translate(builder, request);
        queryTranslator.translate(builder, request);

        return builder.build();
    }

    public Message translateRequest(RestApiRequest request, ByteBuf bodyBuffer) {

        var builder = requestFactory.get();

        pathTranslator.translate(builder, request);
        queryTranslator.translate(builder, request);
        requestBodyTranslator.translate(builder, bodyBuffer);

        return builder.build();
    }
}
