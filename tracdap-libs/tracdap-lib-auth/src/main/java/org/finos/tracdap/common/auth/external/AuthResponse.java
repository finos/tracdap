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

import io.netty.buffer.ByteBuf;

public class AuthResponse {

    private final int statusCode;
    private final String statusMessage;
    private final IAuthHeaders headers;
    private final ByteBuf content;

    public AuthResponse(int statusCode, String statusMessage, IAuthHeaders headers, ByteBuf content) {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.headers = headers;
        this.content = content;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public IAuthHeaders getHeaders() {
        return headers;
    }

    public ByteBuf getContent() {
        return content;
    }
}
