/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.gateway.proxy.grpc;

import com.accenture.trac.common.exception.EUnexpected;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


public class GrpcWebProxy extends Http2ChannelDuplexHandler {

    private static final Pattern CONTENT_TYPE_MATCHER = Pattern.compile(
            "(?<type>\\w+)/(?<subtype>\\w+)(?:\\+(?<payload>\\w+))?(?:;\\w+=\\w+)*");

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<Integer, RequestState> requests;

    public GrpcWebProxy() {
        this.requests = new HashMap<>();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        // gRPC proxy layer expects all messages to be HTTP/2 frames
        if (!(msg instanceof Http2Frame))
            throw new EUnexpected();

        // gRPC proxy layer does not interact with control frames (settings/ping/goaway/etc)
        // Most likely there shouldn't be any of these at this layer anyway
        if (!(msg instanceof Http2StreamFrame)) {
            log.warn("Unexpected HTTP/2 control frame ({}) in gRPC proxy layer", ((Http2Frame) msg).name());
            ctx.write(msg, promise);
            return;
        }

        var frame = (Http2StreamFrame) msg;
        var stream = frame.stream();
        //var request = this.requests.

        log.info("Translating gRPC request for stream {}", stream.id());

        if (frame instanceof Http2HeadersFrame) {

            log.info("Translating request headers for message of type {}", msg.getClass().getSimpleName());

            var headersFrame = (Http2HeadersFrame) frame;
            var contentType = headersFrame.headers().get("content-type");

            if (contentType.toString().startsWith("application/grpc-web")) {

                var grpcContentType = contentType.toString().replace("grpc-web", "grpc");
                headersFrame.headers().remove("content-type");
                headersFrame.headers().add("content-type", grpcContentType);
            }
        }

        ctx.write(frame, promise);
    }

    private void recordOutboundState(int requestId) {

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // gRPC proxy layer expects all messages to be HTTP/2 frames
        if (!(msg instanceof Http2Frame))
            throw new EUnexpected();

        // gRPC proxy layer does not interact with control frames (settings/ping/goaway/etc)
        // Most likely there shouldn't be any of these at this layer anyway
        if (!(msg instanceof Http2StreamFrame)) {
            log.warn("Unexpected HTTP/2 control frame ({}) in gRPC proxy layer", ((Http2Frame) msg).name());
            //ctx.fireChannelRead(msg);
            return;
        }

        var frame = (Http2StreamFrame) msg;
        var stream = frame.stream();
//        var request = requests.get(stream.id());
//
//        if (request == null)
//            throw new RuntimeException();  // TODO: Error

        if (frame instanceof Http2HeadersFrame) {

            log.info("Translating response headers for message of type {}", msg.getClass().getSimpleName());

            var headersFrame = (Http2HeadersFrame) frame;

            if (!headersFrame.headers().contains(":status")) {
                doTrailerTranslation(ctx, headersFrame);
                return;
            }

            var contentType = headersFrame.headers().get("content-type");

            if (contentType.toString().startsWith("application/grpc")) {

                var grpcContentType = contentType.toString().replace("grpc", "grpc-web");
                headersFrame.headers().remove("content-type");
                headersFrame.headers().add("content-type", grpcContentType);
            }
        }

        ctx.fireChannelRead(msg);
    }

    private void doTrailerTranslation(ChannelHandlerContext ctx, Http2HeadersFrame trailersFrame) {

        log.info("Translating trailers frame");

        var h2Trailers = trailersFrame.headers();
        var h1Trailers = new DefaultHttpHeaders();

        for (var trailer : h2Trailers)
            h1Trailers.add(trailer.getKey(), trailer.getValue());

        var trailerBuf = lengthPrefixedMessage(h1Trailers, ctx.alloc());

        var bufSize = trailerBuf.readableBytes();
        var msgSize = bufSize - 5;
        log.info("Trailer frame: size = {}, grpc size = {}", bufSize, msgSize);

        var trailerDataFrame = new DefaultHttp2DataFrame(trailerBuf, true);
        ctx.fireChannelRead(trailerDataFrame);
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

    private static class ContentType {

        String type;
        String subType;
        String payload;
    }

    private static class RequestState {

    }


}
