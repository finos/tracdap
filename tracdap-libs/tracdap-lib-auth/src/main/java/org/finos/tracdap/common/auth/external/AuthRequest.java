/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.auth.external;


import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;

public class AuthRequest {

    private final String method;
    private final String url;
    private final IAuthHeaders headers;
    private final byte[] content;

    public static AuthRequest forHttp1Request(HttpRequest request, IAuthHeaders headers) {

        byte[] content = null;

        if (request instanceof FullHttpRequest) {
            var fullRequest = (FullHttpRequest) request;
            content = new byte[fullRequest.content().readableBytes()];
            fullRequest.content().readBytes(content);
        }

        return new AuthRequest(request.method().toString(), request.uri(), headers, content);
    }

    public AuthRequest(String method, String url, IAuthHeaders headers, byte[] content) {
        this.method = method;
        this.url = url;
        this.headers = headers;
        this.content = content;
    }

    public String getMethod() {
        return method;
    }

    public String getUrl() {
        return url;
    }

    public IAuthHeaders getHeaders() {
        return headers;
    }

    public byte[] getContent() {
        return content;
    }
}
