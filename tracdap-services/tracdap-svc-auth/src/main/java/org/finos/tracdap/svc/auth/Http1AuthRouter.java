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

import io.netty.channel.*;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;

import javax.annotation.Nonnull;


public class Http1AuthRouter extends ChannelDuplexHandler {

    private final Http1AuthSelector selector;
    private ChannelInboundHandler handler;

    public Http1AuthRouter(Http1AuthSelector selector) {
        this.selector = selector;
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {

        try {

            if (msg instanceof HttpRequest) {

                var request = (HttpRequest) msg;
                var handler = selector.selectAuthProcessor(request);

                ctx.pipeline().addLast(handler);
                ctx.pipeline().remove(this);

                ReferenceCountUtil.retain(msg);
                ctx.fireChannelRead(msg);
            }
            else if (msg instanceof HttpContent) {

                ReferenceCountUtil.retain(msg);
                ctx.fireChannelRead(msg);
            }
            else
                throw new EUnexpected();
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        super.write(ctx, msg, promise);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }
}
