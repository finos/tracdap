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

package org.finos.tracdap.common.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.Headers;
import io.netty.handler.codec.http.HttpResponseStatus;

public class CommonHttpResponse {

    private final HttpResponseStatus status;
    private final Headers<CharSequence, CharSequence, ?> headers;
    private final ByteBuf content;

    public CommonHttpResponse(
            HttpResponseStatus status,
            Headers<CharSequence, CharSequence, ?> headers,
            ByteBuf content) {

        this.status = status;
        this.headers = headers;
        this.content = content != null ? content : Unpooled.EMPTY_BUFFER;
    }

    public HttpResponseStatus status() {
        return status;
    }

    public Headers<CharSequence, CharSequence,?> headers() {
        return headers;
    }

    public boolean hasContent() {
        return content.readableBytes() > 0;
    }

    public ByteBuf content() {
        return content;
    }
}
