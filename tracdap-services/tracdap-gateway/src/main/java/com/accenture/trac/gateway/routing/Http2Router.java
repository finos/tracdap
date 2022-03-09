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

package com.accenture.trac.gateway.routing;

import org.finos.tracdap.common.exception.EUnexpected;

import org.finos.tracdap.config.GwRoute;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2ChannelDuplexHandler;
import io.netty.handler.codec.http2.Http2Frame;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class Http2Router extends Http2ChannelDuplexHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public Http2Router(List<GwRoute> routeConfigs) {

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (!(msg instanceof Http2Frame))
            throw new EUnexpected();

        channelRead0(ctx, (Http2Frame) msg);
    }

    private void channelRead0(ChannelHandlerContext ctx, Http2Frame frame) throws Exception {

        log.info("Got frame: " + frame.name());
    }
}
