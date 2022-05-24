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

package org.finos.tracdap.gateway.proxy.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.*;
import org.finos.tracdap.common.exception.EUnexpected;

import javax.annotation.Nonnull;


public class Http2FlowControl extends Http2ChannelDuplexHandler {

    private static final boolean DROP_SETTINGS = true;
    private static final boolean DROP_WINDOW_UPDATE = true;
    private static final boolean DROP_PING = true;

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

        if (!(msg instanceof Http2Frame))
            throw new EUnexpected();

        if (msg instanceof Http2DataFrame) {

            // Respond to data frames with a window update indicating the frame has been consumed

            var dataFrame = (Http2DataFrame) msg;
            var dataSize = dataFrame.content().readableBytes();

            var flowFrame = new DefaultHttp2WindowUpdateFrame(dataSize);
            flowFrame.stream(dataFrame.stream());

            ctx.write(flowFrame);
        }

        // Do not pass HTTP/2 control frames further up the pipeline

        if (DROP_SETTINGS && (msg instanceof Http2SettingsFrame || msg instanceof Http2SettingsAckFrame))
            return;

        if (DROP_WINDOW_UPDATE && msg instanceof Http2WindowUpdateFrame)
            return;

        if (DROP_PING && msg instanceof Http2PingFrame)
            return;

        ctx.fireChannelRead(msg);
    }
}
