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

package org.finos.tracdap.gateway.proxy.internal;

import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.util.LoggingHelpers;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.*;
import io.netty.util.ReferenceCountUtil;
import org.finos.tracdap.gateway.proxy.http.HttpProtocol;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;


public class HealthCheckHandler extends ChannelInboundHandlerAdapter {

    public static final String PROTOCOL = "healthz";

    public static final byte[] SERVING_RESPONSE = "{\"status\": \"SERVING\"}".getBytes(StandardCharsets.UTF_8);

    private static final ThreadLocal<Logger> logMap = new ThreadLocal<>();
    private final Logger log = LoggingHelpers.threadLocalLogger(this, logMap);

    private final HttpProtocol httpProtocol;
    private final long connId;

    public HealthCheckHandler(HttpProtocol httpProtocol, long connId) {
        this.httpProtocol = httpProtocol;
        this.connId = connId;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {

        if (log.isDebugEnabled())
            log.debug("{} handlerAdded: conn = {}, protocol = {}", getClass().getSimpleName(), connId, httpProtocol);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {

        if (log.isDebugEnabled())
            log.debug("{} handlerRemoved: conn = {}, protocol = {}", getClass().getSimpleName(), connId, httpProtocol);
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {

        try {

            if (msg instanceof HttpObject) {

                if (httpProtocol != HttpProtocol.HTTP_1_1 && httpProtocol != HttpProtocol.HTTP_1_0)
                    throw new EUnexpected();

                if (msg instanceof HttpRequest) {

                    var request = (HttpRequest) msg;
                    processRequest(ctx, request);
                }
                else if (!(msg instanceof HttpContent)) {

                    log.warn("Unexpected HTTP object ({}) in health check handler", msg.getClass().getName());
                }
            }
            else if (msg instanceof Http2Frame) {

                if (httpProtocol != HttpProtocol.HTTP_2_0)
                    throw new EUnexpected();

                var frame = (Http2Frame) msg;

                if (frame instanceof Http2HeadersFrame) {

                    var request = (Http2HeadersFrame) frame;
                    processRequest(ctx, request);
                }
                else {

                    log.warn("Unexpected HTTP/2 frame ({}) in health check handler", frame.name());
                }
            }
            else {

                log.error("Unexpected HTTP protocol ({}) in health check handler", httpProtocol.name());
                ctx.close();
            }
        }
        catch (Throwable t) {

            log.error(t.getMessage(), t);

            var headers = new DefaultHttp2Headers().status(HttpResponseStatus.INTERNAL_SERVER_ERROR.codeAsText());
            var response = new DefaultHttp2HeadersFrame(headers, false);
            ctx.writeAndFlush(response);
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void processRequest(ChannelHandlerContext ctx, HttpRequest request) {

        var method = request.method();

        if (method != HttpMethod.HEAD && method != HttpMethod.GET)
            throw new ETracInternal("Invalid HTTP method for health check: " + method);

        var responseBuf = method.equals(HttpMethod.GET)
                ? Unpooled.wrappedBuffer(SERVING_RESPONSE)
                : Unpooled.EMPTY_BUFFER;

        var headers = new DefaultHttpHeaders()
                .add(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8")
                .addInt(HttpHeaderNames.CONTENT_LENGTH, SERVING_RESPONSE.length);

        var response = new DefaultFullHttpResponse(
                request.protocolVersion(),
                HttpResponseStatus.OK,
                responseBuf,
                headers, new DefaultHttpHeaders());

        ctx.writeAndFlush(response);
    }

    private void processRequest(ChannelHandlerContext ctx, Http2HeadersFrame request) {

        var method = request.headers().method().toString().toUpperCase();

        if (!(method.equals(HttpMethod.HEAD.name()) || method.equals(HttpMethod.GET.name())))
            throw new ETracInternal("Invalid HTTP method for health check: " + method);

        var headers = new DefaultHttp2Headers()
                .status(HttpResponseStatus.OK.codeAsText())
                .add(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8")
                .addInt(HttpHeaderNames.CONTENT_LENGTH, SERVING_RESPONSE.length);

        var sendData = method.equals(HttpMethod.GET.name());
        var headerFrame = new DefaultHttp2HeadersFrame(headers, !sendData);
        ctx.write(headerFrame);

        if (sendData) {
            var dataFrame = new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(SERVING_RESPONSE), true);
            ctx.write(dataFrame);
        }

        ctx.flush();
    }
}
