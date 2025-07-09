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

import org.finos.tracdap.common.util.LoggingHelpers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.slf4j.Logger;


public class InternalProxyRelay extends ChannelOutboundHandlerAdapter {

    private static final ThreadLocal<Logger> logMap = new ThreadLocal<>();
    private final Logger log = LoggingHelpers.threadLocalLogger(this, logMap);

    private final EmbeddedChannel target;
    private final long connId;
    private boolean closed;

    public InternalProxyRelay(EmbeddedChannel target, long connId) {
        this.target = target;
        this.connId = connId;
        this.closed = false;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {

        if (log.isDebugEnabled())
            log.debug("{} handlerAdded: conn = {}", getClass().getSimpleName(), connId);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {

        if (log.isDebugEnabled())
            log.debug("{} handlerRemoved: conn = {}", getClass().getSimpleName(), connId);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        target.writeInbound(msg);
        promise.setSuccess();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {

        target.flushInbound();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {

        if (!closed) {

            closed = true;

            if (target.isOpen())
                target.close();
        }

        super.close(ctx, promise);
    }
}
