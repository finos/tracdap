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

package com.accenture.trac.gateway.proxy.http;

import com.accenture.trac.common.exception.EUnexpected;
import org.finos.tracdap.config.GwRoute;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class Http1to2Framing extends Http2ChannelDuplexHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final GwRoute routeConfig;

    private final Map<Integer, Http2FrameStream> streams;
    private final AtomicInteger nextSeqId;
    private int inboundSeqId;
    private int outboundSeqId;

    public Http1to2Framing(GwRoute routeConfig) {

        this.routeConfig = routeConfig;

        this.streams = new HashMap<>();
        this.nextSeqId = new AtomicInteger(0);
        this.inboundSeqId = -1;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        log.info("Translating HTTP/1 message of type {}", msg.getClass().getSimpleName());

        if (msg instanceof HttpRequest)
            newSeqStream();

        var frames = translateRequestFrames(msg);
        var notLastFrame = frames.subList(0, frames.size() - 1);
        var lastFrame = frames.get(frames.size() - 1);

        for (var frame : notLastFrame)
            ctx.write(frame);

        promise.addListener(fut -> {

            var stream = streams.get(inboundSeqId);
            log.info("on stream  {}, {}", stream.id(), stream.state());
        });

        ctx.write(lastFrame, promise);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (!(msg instanceof Http2Frame))
            throw new EUnexpected();

        var frame = (Http2Frame) msg;
        var httpObjs = translateResponseFrame(frame);

        for (var httpObj : httpObjs)
            ctx.fireChannelRead(httpObj);
    }

    private void newSeqStream() {

        inboundSeqId = nextSeqId.getAndIncrement();

        var stream = this.newStream();
        streams.put(inboundSeqId, stream);

        log.info("SEQ ID {} -> STREAM {}", inboundSeqId, stream.id());
    }

    private void deleteSeqStream() {

    }

    private List<Http2Frame> translateRequestFrames(Object http1) {

        var seqId = inboundSeqId;
        var stream = streams.get(seqId);

        if (stream == null)
            throw new EUnexpected();

        var frames = new ArrayList<Http2Frame>();

        if (http1 instanceof HttpRequest) {

            var h1Request = (HttpRequest) http1;
            var h1Headers = h1Request.headers();

            var h2Headers = new DefaultHttp2Headers()
                    .method(h1Request.method().name())
                    .scheme(routeConfig.getTarget().getScheme())
                    .path(h1Request.uri());

            if (h1Headers.contains(HttpHeaderNames.HOST))
                h2Headers.authority(h1Headers.get(HttpHeaderNames.HOST));
            else
                h2Headers.authority(routeConfig.getTarget().getHost());

            // Copy across all other HTTP/1 headers that we are not explicitly changing or removing
            var filterHeaders = List.of(
                    HttpHeaderNames.HOST.toString(),
                    HttpHeaderNames.CONNECTION.toString(),
                    HttpHeaderNames.CONTENT_LENGTH.toString());

            for (var header : h1Headers)
                if (!filterHeaders.contains(header.getKey().toLowerCase()))
                    h2Headers.add(header.getKey().toLowerCase(), header.getValue());

            var frame = new DefaultHttp2HeadersFrame(h2Headers, false).stream(stream);
            frames.add(frame);
        }

        if (http1 instanceof HttpContent) {

            var h1Content = (HttpContent) http1;
            var contentBuf = h1Content.content();

            var MAX_DATA_SIZE = 16 * 1024;

            contentBuf.retain();

            log.info("Size of content: {}", contentBuf.readableBytes());

            while (contentBuf.readableBytes() > MAX_DATA_SIZE) {

                var slice = contentBuf.readSlice(MAX_DATA_SIZE);
                var frame = new DefaultHttp2DataFrame(slice).stream(stream);
                frames.add(frame);
            }

            var endStreamFlag = (http1 instanceof LastHttpContent);
            var slice = contentBuf.readSlice(contentBuf.readableBytes());

            log.info("Size of slice: {}", slice.readableBytes());
            log.info("end of stream: {}", endStreamFlag);

            var padding = 256 - (slice.readableBytes() % 256) % 256;

            var frame = new DefaultHttp2DataFrame(slice, endStreamFlag, padding).stream(stream);
            frames.add(frame);
        }

        if (frames.isEmpty())
            throw new EUnexpected();

        return frames;
    }

    private List<HttpObject> translateResponseFrame(Http2Frame frame) {

        if (frame instanceof Http2HeadersFrame)
            return translateResponseHeaders((Http2HeadersFrame) frame);

        if (frame instanceof Http2DataFrame)
            return translateResponseData((Http2DataFrame) frame);

        throw new EUnexpected();
    }

    private List<HttpObject> translateResponseHeaders(Http2HeadersFrame headersFrame) {

        var h2Headers = headersFrame.headers();
        var h1Headers = new DefaultHttpHeaders();

        for (var header : h2Headers)
            if (!header.getKey().toString().startsWith(":"))
                h1Headers.add(header.getKey(), header.getValue());

        if (!h1Headers.contains(HttpHeaderNames.CONTENT_LENGTH) &&
            !h1Headers.contains(HttpHeaderNames.TRANSFER_ENCODING)) {

            h1Headers.add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        }

        var statusCode = HttpResponseStatus.parseLine(h2Headers.get(":status"));
        var headerObj = new DefaultHttpResponse(HttpVersion.HTTP_1_1, statusCode, h1Headers);

        if (headersFrame.isEndStream())
            return List.of(headerObj, new DefaultLastHttpContent());
        else
            return List.of(headerObj);
    }

    private List<HttpObject> translateResponseData(Http2DataFrame dataFrame) {

        dataFrame.content().retain();

        if (dataFrame.isEndStream())
            return List.of(new DefaultLastHttpContent(dataFrame.content()));
        else
            return List.of(new DefaultHttpContent(dataFrame.content()));
    }

}
