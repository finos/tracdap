/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import org.finos.tracdap.common.util.LoggingHelpers;
import org.finos.tracdap.common.util.RoutingUtils;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.gateway.builders.ServiceInfo;
import org.finos.tracdap.gateway.proxy.http.HttpProtocol;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public class AvailabilityHandler extends ChannelInboundHandlerAdapter {

    public static final String PROTOCOL = "available";

    private static final int PROBE_TIMEOUT_MS = 2000;
    private static final int TOTAL_TIMEOUT_S = 5;

    private static final ThreadLocal<Logger> logMap = new ThreadLocal<>();
    private final Logger log = LoggingHelpers.threadLocalLogger(this, logMap);

    private final HttpProtocol httpProtocol;
    private final long connId;
    private final PlatformConfig platformConfig;

    public AvailabilityHandler(HttpProtocol httpProtocol, long connId, PlatformConfig platformConfig) {
        this.httpProtocol = httpProtocol;
        this.connId = connId;
        this.platformConfig = platformConfig;
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {

        try {

            if (msg instanceof HttpRequest) {

                if (httpProtocol != HttpProtocol.HTTP_1_1 && httpProtocol != HttpProtocol.HTTP_1_0)
                    throw new ETracInternal("Availability check requires HTTP/1 protocol");

                var request = (HttpRequest) msg;
                processRequest(ctx, request);
            }
            else if (!(msg instanceof HttpContent)) {

                log.warn("Unexpected message type ({}) in availability handler", msg.getClass().getName());
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void processRequest(ChannelHandlerContext ctx, HttpRequest request) {

        var method = request.method();

        if (method != HttpMethod.GET)
            throw new ETracInternal("Invalid HTTP method for availability check: " + method);

        var responseBody = checkAvailability();
        var responseBytes = responseBody.getBytes(StandardCharsets.UTF_8);
        var responseBuf = Unpooled.wrappedBuffer(responseBytes);

        var headers = new DefaultHttpHeaders()
                .add(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8")
                .addInt(HttpHeaderNames.CONTENT_LENGTH, responseBytes.length);

        var response = new DefaultFullHttpResponse(
                request.protocolVersion(),
                HttpResponseStatus.OK,
                responseBuf,
                headers, new DefaultHttpHeaders());

        ctx.writeAndFlush(response);
    }

    private String checkAvailability() {

        var services = ServiceInfo.buildServiceInfo(platformConfig);

        // Start a probe for each service in parallel
        var probes = new LinkedHashMap<String, CompletableFuture<Boolean>>();

        for (var service : services) {
            var serviceKey = service.serviceKey();
            var probe = CompletableFuture.supplyAsync(() -> probeService(serviceKey));
            probes.put(serviceKey, probe);
        }

        // Wait for all probes to complete
        try {
            CompletableFuture
                    .allOf(probes.values().toArray(new CompletableFuture[0]))
                    .get(TOTAL_TIMEOUT_S, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            log.warn("Availability probe timed out or was interrupted: {}", e.getMessage());
        }

        // Build the plain-text response
        var sb = new StringBuilder();

        for (var entry : probes.entrySet()) {
            var available = false;
            try {
                available = entry.getValue().getNow(false);
            }
            catch (Exception e) {
                log.debug("Probe result unavailable for [{}]: {}", entry.getKey(), e.getMessage());
            }
            sb.append(entry.getKey()).append('=').append(available ? "yes" : "no").append('\n');
        }

        return sb.toString();
    }

    private boolean probeService(String serviceKey) {

        try {
            var target = RoutingUtils.serviceTarget(platformConfig, serviceKey);
            var host = target.getHost();
            var port = (int) target.getPort();

            try (var socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), PROBE_TIMEOUT_MS);
                return true;
            }
        }
        catch (IOException e) {
            log.debug("Service [{}] probe failed: {}", serviceKey, e.getMessage());
            return false;
        }
    }
}
