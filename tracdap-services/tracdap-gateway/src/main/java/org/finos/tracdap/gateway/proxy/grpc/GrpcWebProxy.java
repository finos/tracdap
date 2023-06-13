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

import org.finos.tracdap.common.exception.EUnexpected;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;


public class GrpcWebProxy extends Http2ChannelDuplexHandler {

    private static final Pattern CONTENT_TYPE_MATCHER = Pattern.compile(
            "(?<type>\\w+)/(?<subtype>\\w+)(?:\\+(?<payload>\\w+))?(?:;\\w+=\\w+)*");

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final int connId;
    private final boolean isWebTextProtocol;

    public GrpcWebProxy(int connId) {
        this.connId = connId;
        this.isWebTextProtocol = false;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        // gRPC proxy layer expects all messages to be HTTP/2 frames
        if (!(msg instanceof Http2Frame))
            throw new EUnexpected();

        var frame = (Http2Frame) msg;

        if (frame instanceof Http2HeadersFrame) {

            var grpcWebFrame = (Http2HeadersFrame) frame;
            var grpcFrame = translateRequestHeaders(grpcWebFrame);

            ctx.write(grpcFrame, promise);

            // Stream ID is not available until the first frame is written to the HTTP/2 codec
            promise.addListener(f -> log.info("conn = {}, stream = {}, TRANSLATE gRPC-Web {}",
                    connId, grpcWebFrame.stream().id(), grpcFrame.headers().get(":path")));
        }
        else if (frame instanceof Http2DataFrame) {

            var grpcWebFrame = (Http2DataFrame) frame;
            var grpcFrame = isWebTextProtocol
                ? decodeWebTextFrame(grpcWebFrame)
                : grpcWebFrame.retain();

            ctx.write(grpcFrame, promise);

            // The original frame is no longer needed and can be released
            grpcWebFrame.release();
        }
        else {

            // gRPC proxy layer does not interact with control frames (settings/ping/goaway/etc)
            // Most likely there shouldn't be any of these at this layer anyway

            log.warn("conn = {}, Unexpected HTTP/2 request frame ({}) in gRPC-web proxy layer", connId, frame.name());
            ctx.write(msg, promise);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        // gRPC proxy layer expects all messages to be HTTP/2 frames
        if (!(msg instanceof Http2Frame))
            throw new EUnexpected();

        var frame = (Http2Frame) msg;

        if (frame instanceof Http2HeadersFrame) {

            var grpcFrame = (Http2HeadersFrame) frame;
            var grpcWebFrame = grpcFrame.headers().contains(":status")
                    ? translateResponseHeaders(grpcFrame)
                    : translateResponseTrailers(grpcFrame, ctx.alloc());

            ctx.fireChannelRead(grpcWebFrame);
        }
        else if (frame instanceof Http2DataFrame) {

            var grpcFrame = (Http2DataFrame) frame;
            var grpcWebFrame = isWebTextProtocol
                    ? encodeWebTextFrame(grpcFrame)
                    : grpcFrame.retain();

            ctx.fireChannelRead(grpcWebFrame);

            // The original frame is no longer needed and can be released
            grpcFrame.release();
        }
        else {

            // gRPC proxy layer does not interact with control frames (settings/ping/goaway/etc)
            // Most likely there shouldn't be any of these at this layer anyway

            log.warn("Unexpected HTTP/2 response frame ({}) in gRPC-web proxy layer", frame.name());
            // ctx.fireChannelRead(frame);
        }
    }

    private Http2HeadersFrame translateRequestHeaders(Http2HeadersFrame headersFrame) {

        var contentType = headersFrame.headers().get("content-type");

        if (contentType != null && contentType.toString().startsWith("application/grpc-web")) {

            var grpcContentType = contentType.toString().replace("grpc-web", "grpc");
            headersFrame.headers().remove("content-type");
            headersFrame.headers().add("content-type", grpcContentType);
            headersFrame.headers().add("te", "trailers");
        }

        return headersFrame;
    }

    private Http2HeadersFrame translateResponseHeaders(Http2HeadersFrame headersFrame) {

        var contentType = headersFrame.headers().get("content-type");

        if (contentType != null && contentType.toString().startsWith("application/grpc")) {

            var grpcContentType = contentType.toString().replace("grpc", "grpc-web");
            headersFrame.headers().remove("content-type");
            headersFrame.headers().add("content-type", grpcContentType);
        }

        return headersFrame;
    }

    private Http2DataFrame translateResponseTrailers(Http2HeadersFrame trailersFrame, ByteBufAllocator allocator) {

        var h2Trailers = trailersFrame.headers();
        var h1Trailers = new DefaultHttpHeaders();

        for (var trailer : h2Trailers)
            h1Trailers.add(trailer.getKey(), trailer.getValue());

        var trailerBuf = lengthPrefixedMessage(h1Trailers, allocator);

        var bufSize = trailerBuf.readableBytes();
        var msgSize = bufSize - 5;

        if (log.isDebugEnabled())
            log.debug("conn = {}, Trailer frame: size = {}, grpc size = {}", connId, bufSize, msgSize);

        return new DefaultHttp2DataFrame(trailerBuf, true);
    }

    private Http2DataFrame decodeWebTextFrame(Http2DataFrame webTextFrame) {

        throw new RuntimeException("Web text protocol not implemented");
    }

    private Http2DataFrame encodeWebTextFrame(Http2DataFrame grpcFrame) {

        throw new RuntimeException("Web text protocol not implemented");
    }

    private ByteBuf lengthPrefixedMessage(HttpHeaders trailers, ByteBufAllocator allocator) {

        var codec = new HttpRequestEncoder() {
            public void encodeHeaders0(HttpHeaders headers, ByteBuf buf) {
                this.encodeHeaders(headers, buf);
            }
        };

        var buffer = allocator.buffer();

        // Write headers into the buffer, leaving 5 bytes for flags and size fields
        buffer.writerIndex(5);
        codec.encodeHeaders0(trailers, buffer);

        var lpmFlags = (byte) 1 << 7;  // most significant bit = 1, signals trailer frame
        var lpmSize = buffer.readableBytes() - 5;  // size of the message content

        // Write flags and size at the start of the buffer
        buffer.writerIndex(0);
        buffer.writeByte(lpmFlags);
        buffer.writeInt(lpmSize);

        // Put the write index back at the end of the buffer
        buffer.writerIndex(5 + lpmSize);

        return buffer;
    }
}
