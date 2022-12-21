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

package org.finos.tracdap.gateway.proxy.grpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Headers;

import java.nio.charset.StandardCharsets;


public class GrpcUtils {

    public static int LPM_PREFIX_LENGTH = 5;

    public static Http2Headers decodeHeadersFrame(ByteBuf headersBuf) {

        var headers = new DefaultHttp2Headers();

        var bytes = new byte[headersBuf.readableBytes()];
        headersBuf.readBytes(bytes);

        var text = new String(bytes, StandardCharsets.UTF_8);
        var lines = text.split("\\r\\n");

        for (var line : lines) {

            // Shouldn't happen because trailers in LPM blocks shouldn't have a trailing new line
            // But this protects against broken clients
            if (line.isBlank())
                continue;

            var separator = line.indexOf(':', 1);
            var headerName = line.substring(0, separator);
            var headerValue = line.substring(separator + 2);

            headers.add(headerName, headerValue);
        }

        return headers;
    }

    public static ByteBuf lpmHeaders(HttpHeaders headers, ByteBufAllocator allocator) {

        var builder = new StringBuilder();

        for (var header : headers) {

            builder.append(header.getKey());
            builder.append(": ");
            builder.append(header.getValue());
            builder.append("\r\n");
        }

        // Do not append a final \r\n, as per https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md

        return asciiToLpm(builder, allocator);
    }

    public static ByteBuf lpmHeaders(Http2Headers headers, ByteBufAllocator allocator) {

        var builder = new StringBuilder();

        for (var header : headers) {

            builder.append(header.getKey());
            builder.append(": ");
            builder.append(header.getValue());
            builder.append("\r\n");
        }

        // Do not append a final \r\n, as per https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md

        return asciiToLpm(builder, allocator);
    }

    private static ByteBuf asciiToLpm(StringBuilder content, ByteBufAllocator allocator) {

        var lpmFlags = (byte) 1 << 7;  // most significant bit = 1, signals trailer frame
        var lpmSize = content.length();  // size of the message content

        var buffer = allocator.buffer(lpmSize + 5);
        buffer.writeByte(lpmFlags);
        buffer.writeInt(lpmSize);
        buffer.writeCharSequence(content, StandardCharsets.US_ASCII);

        return buffer;
    }
}