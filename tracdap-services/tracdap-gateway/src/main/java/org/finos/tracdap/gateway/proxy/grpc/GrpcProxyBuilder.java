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

package org.finos.tracdap.gateway.proxy.grpc;

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.GwRoute;
import org.finos.tracdap.gateway.proxy.http.Http1to2Framing;

import io.netty.channel.*;
import io.netty.handler.codec.http2.*;
import org.finos.tracdap.gateway.proxy.http.Http2FlowControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;


public class GrpcProxyBuilder extends ChannelInitializer<Channel> {

    private static final String HTTP2_PREFACE_HANDLER = "HTTP2_PREFACE";
    private static final String HTTP2_FRAME_CODEC = "HTTP2_FRAME_CODEC";
    private static final String HTTP2_FLOW_CTRL = "HTTP2_FLOW_CTRL";
    private static final String GRPC_PROXY_HANDLER = "GRPC_PROXY_HANDLER";
    private static final String GRPC_WEB_PROXY_HANDLER = "GRPC_WEB_PROXY_HANDLER";
    private static final String HTTP_1_TO_2_FRAMING = "HTTP_1_TO_2_FRAMING";
    private static final String HTTP_1_ROUTER_LINK = "HTTP_1_ROUTER_LINK";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final GwRoute routeConfig;
    private final int sourceHttpVersion;
    private final ChannelDuplexHandler routerLink;
    private final int connId;

    public GrpcProxyBuilder(
            GwRoute routeConfig,
            int sourceHttpVersion,
            ChannelDuplexHandler routerLink,
            int connId) {

        this.routeConfig = routeConfig;
        this.sourceHttpVersion = sourceHttpVersion;
        this.routerLink = routerLink;
        this.connId = connId;
    }

    @Override
    protected void initChannel(@Nonnull Channel channel) {

        if (log.isDebugEnabled())
            log.debug("conn = {}, Init gRPC proxy channel", connId);

        var pipeline = channel.pipeline();

        var initialSettings = new Http2Settings()
                .maxFrameSize(Http2FlowControl.DEFAULT_MAX_FRAME_SIZE)
                .initialWindowSize(Http2FlowControl.DEFAULT_INITIAL_WINDOW_SIZE);

        var http2Codec = Http2FrameCodecBuilder.forClient()
                .initialSettings(initialSettings)
                .autoAckSettingsFrame(true)
                .autoAckPingFrame(true)
                .validateHeaders(true)
                .build();

        var http2FlowControl = new Http2FlowControl(connId, http2Codec, initialSettings);

        pipeline.addLast(HTTP2_FRAME_CODEC, http2Codec);
        pipeline.addLast(HTTP2_FLOW_CTRL, http2FlowControl);
        pipeline.addLast(GRPC_PROXY_HANDLER, new GrpcProxy(connId));
        pipeline.addLast(GRPC_WEB_PROXY_HANDLER, new GrpcWebProxy(connId));

        if (sourceHttpVersion == 1) {

            pipeline.addLast(HTTP_1_TO_2_FRAMING, new Http1to2Framing(routeConfig, connId));
            pipeline.addLast(HTTP_1_ROUTER_LINK, routerLink);
        }
        else if (sourceHttpVersion == 2) {

            throw new RuntimeException("HTTP/2 source connection for REST not implemented yet");
        }
        else
            throw new EUnexpected();
    }
}
