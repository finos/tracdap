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

import org.finos.tracdap.common.netty.ConnectionId;
import org.finos.tracdap.common.util.LoggingHelpers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;

import org.slf4j.Logger;


public class OutboundRelay extends ChannelOutboundHandlerAdapter {

    private static final ThreadLocal<Logger> logMap = new ThreadLocal<>();
    private final Logger log = LoggingHelpers.threadLocalLogger(this, logMap);

    private final ChannelHandlerContext relayCtx;

    OutboundRelay(ChannelHandlerContext relayCtx) {
        this.relayCtx = relayCtx;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        if (log.isTraceEnabled()) {
            var connId = ConnectionId.get(ctx.channel());
            log.trace("{} write: conn = {}, msg = {}", getClass(), connId, msg);
        }

        var relayPromise = relayCtx.newPromise();
        relayPromise.addListener(future -> relayPromise(future, promise));

        relayCtx.pipeline().write(msg, relayPromise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {

        if (log.isTraceEnabled()) {
            var connId = ConnectionId.get(ctx.channel());
            log.trace("{} flush: conn = {}", getClass(), connId);
        }

        relayCtx.pipeline().flush();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {

        if (log.isTraceEnabled()) {
            var connId = ConnectionId.get(ctx.channel());
            log.trace("{} close: conn = {}", getClass(), connId);
        }

        var relayPromise = relayCtx.newPromise();
        relayPromise.addListener(future -> relayPromise(future, promise));

        relayCtx.channel().close(relayPromise);
    }

    private void relayPromise(Future<?> future, ChannelPromise promise) {

        if (future.isSuccess())
            promise.setSuccess();
        else
            promise.setFailure(future.cause());
    }
}
