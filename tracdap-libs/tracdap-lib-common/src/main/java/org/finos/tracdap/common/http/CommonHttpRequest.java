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
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;

public class CommonHttpRequest {

    private final String method;
    private final String path;
    private final Headers<CharSequence, CharSequence, ?> headers;
    private final ByteBuf content;

    public static CommonHttpRequest fromHttpRequest(HttpRequest httpRequest) {

        var method = httpRequest.method().toString();
        var path = httpRequest.uri();
        var headers = Http1Headers.wrapHttpHeaders(httpRequest.headers());

        var content = (httpRequest instanceof FullHttpRequest)
                ? ((FullHttpRequest) httpRequest).content()
                : Unpooled.EMPTY_BUFFER;

        return new CommonHttpRequest(method, path, headers, content);
    }

    public CommonHttpRequest(String method, String path, Headers<CharSequence, CharSequence, ?> headers, ByteBuf content) {
        this.method = method;
        this.path = path;
        this.headers = headers;
        this.content = content;
    }

    public String method() {
        return method;
    }

    public String path() {
        return path;
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
