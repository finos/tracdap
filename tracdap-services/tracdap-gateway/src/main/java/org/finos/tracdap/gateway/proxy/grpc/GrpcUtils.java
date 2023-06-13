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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.netty.buffer.*;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Headers;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class GrpcUtils {

    public static int LPM_PREFIX_LENGTH = 5;

    public static <TMsg extends Message>
    ByteBuf encodeLpm(TMsg msg, ByteBufAllocator allocator) {

        var compression = (byte) 0x00;
        var msgSize = msg.getSerializedSize();
        var lpmSize = LPM_PREFIX_LENGTH + msgSize;
        var buffer = allocator.directBuffer(lpmSize);

        try (var stream = new ByteBufOutputStream(buffer)) {

            stream.writeByte(compression);
            stream.writeInt(msgSize);
            msg.writeTo(stream);

            return buffer;
        }
        catch (IOException e) {
            throw new EUnexpected(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <TMsg extends Message>
    TMsg decodeLpm(TMsg blankMsg, ByteBuf buffer) throws InvalidProtocolBufferException {

        if (buffer.readableBytes() < LPM_PREFIX_LENGTH)
            throw new InvalidProtocolBufferException("Unexpected end of stream");

        var compression = buffer.readByte();
        var msgSize = buffer.readInt();

        if (buffer.readableBytes() < msgSize)
            throw new InvalidProtocolBufferException("Unexpected end of stream");

        // TODO: Decompression support
        if (compression != 0)
            throw new ETracInternal("compression not supported yet");

        try (var stream = new ByteBufInputStream(buffer, msgSize)) {

            var builder = blankMsg.newBuilderForType();
            builder.mergeFrom(stream);

            return (TMsg) builder.build();
        }
        catch (InvalidProtocolBufferException e) {
            throw e;
        }
        catch (IOException e) {
            throw new EUnexpected(e);
        }
    }

    public static boolean canDecodeLpm(ByteBuf buffer) {

        if (buffer.readableBytes() < LPM_PREFIX_LENGTH)
            return false;

        // Assume a new LPM starts at readerIndex
        // compression flag is at readerIndex
        // msgSize is at readerIndex + 1

        var msgSize = buffer.getInt(buffer.readerIndex() + 1);

        return buffer.readableBytes() >= LPM_PREFIX_LENGTH + msgSize;
    }

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

    public static short readLpmFlag(byte[] prefixBytes) {

        return (short) (prefixBytes[0] & 0xFF);
    }

    public static long readLpmLength(byte[] bytes) {

        return bytes[1] << 24 | (bytes[2] & 0xFF) << 16 | (bytes[3] & 0xFF) << 8 | (bytes[4] & 0xFF);
    }
}