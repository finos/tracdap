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

package org.finos.tracdap.svc.auth;

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.netty.ConnectionId;
import org.finos.tracdap.common.util.LoggingHelpers;

import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;


public class Http1ProviderLookup extends ChannelInboundHandlerAdapter {

    private static final ThreadLocal<Logger> logMap = new ThreadLocal<>();
    private final Logger log = LoggingHelpers.threadLocalLogger(this, logMap);

    private final ProviderLookup providerLookup;
    private final Map<String, EmbeddedChannel> providerChannels;

    private EmbeddedChannel providerChannel;

    public Http1ProviderLookup(ProviderLookup providerLookup) {
        this.providerLookup = providerLookup;
        this.providerChannels = new HashMap<>();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {

        for (var channel : providerChannels.values()) {

            var channelCtx = channel.pipeline().firstContext();
            channelCtx.close();
            channel.close();
        }

        providerChannels.clear();
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {

        var connId = ConnectionId.get(ctx.channel());

        if (log.isTraceEnabled())
            log.trace("{} channelRead: conn = {}, msg = {}", getClass().getSimpleName(), connId, msg);

        try {

            if (!(msg instanceof HttpObject)) {
                ctx.close();
                throw new EUnexpected();
            }

            if (msg instanceof HttpRequest) {

                var request = (HttpRequest) msg;
                var providerKey = providerLookup.findProvider(request);

                if (providerKey != null) {
                    log.info("SELECT PROVIDER: conn = {}, provider = {}, {}", connId, providerKey, request.uri());
                    providerChannel = getProviderChannel(ctx, providerKey);
                }
                else{
                    log.error("SELECT PROVIDER FAILED: conn = {}, provider not found, {}", connId, request.uri());
                    sendProviderNotFound(ctx, request);
                    ctx.close();
                }
            }

            if (providerChannel != null) {
                ReferenceCountUtil.retain(msg);
                providerChannel.pipeline().fireChannelRead(msg);
            }

            if (msg instanceof LastHttpContent) {
                providerChannel = null;
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private EmbeddedChannel getProviderChannel(ChannelHandlerContext ctx, String providerKey) {

        var existingChannel = providerChannels.get(providerKey);

        if (existingChannel != null)
            return existingChannel;

        var relay = new OutboundRelay(ctx);
        var provider = providerLookup.createProvider(providerKey);

        // Make sure the embedded channel has a reference to the original inbound channel as its parent
        // For providers that make outbound connections, they need this to share event loop, allocator etc.
        var providerChannel = new EmbeddedChannel(
                ctx.channel(), DefaultChannelId.newInstance(),
                true, false, relay, provider);

        providerChannels.put(providerKey, providerChannel);

        return providerChannel;
    }

    private void sendProviderNotFound(ChannelHandlerContext ctx, HttpRequest request) {

        var protocolVersion = request.protocolVersion();
        var status = HttpResponseStatus.NOT_FOUND;
        var response = new DefaultFullHttpResponse(protocolVersion, status);
        ctx.writeAndFlush(response);
    }
}
