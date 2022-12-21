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

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.logging.LogLevel;
import org.finos.tracdap.common.exception.ENetworkHttp;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.GwRoute;
import org.finos.tracdap.gateway.proxy.http.Http1to2Proxy;

import io.netty.channel.*;
import io.netty.handler.codec.http2.*;
import org.finos.tracdap.gateway.proxy.http.Http2FlowControl;
import org.finos.tracdap.gateway.proxy.http.HttpProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.SocketAddress;


public class GrpcProxyBuilder extends ChannelInitializer<Channel> {

    private static final String DATA_API_NAME = "TracDataApi";

    private static final String HTTP2_PREFACE_HANDLER = "HTTP2_PREFACE";
    private static final String HTTP2_FRAME_CODEC = "HTTP2_FRAME_CODEC";
    private static final String HTTP2_FLOW_CTRL = "HTTP2_FLOW_CTRL";
    private static final String GRPC_PROXY_HANDLER = "GRPC_PROXY_HANDLER";
    private static final String GRPC_WEB_PROXY_HANDLER = "GRPC_WEB_PROXY_HANDLER";
    private static final String GRPC_WEBSOCKETS_TRANSLATOR = "GRPC_WEBSOCKETS_TRANSLATOR";
    private static final String HTTP_1_TO_2_PROXY = "HTTP_1_TO_2_PROXY";
    private static final String CORE_ROUTER_LINK = "CORE_ROUTER_LINK";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final GwRoute routeConfig;
    private final ChannelDuplexHandler routerLink;

    private final int connId;
    private final HttpProtocol httpProtocol;
    private final GrpcProtocol grpcProtocol;

    public GrpcProxyBuilder(
            GwRoute routeConfig,
            ChannelDuplexHandler routerLink,
            int connId,
            HttpProtocol httpProtocol,
            GrpcProtocol grpcProtocol) {

        this.routeConfig = routeConfig;
        this.routerLink = routerLink;

        this.connId = connId;
        this.httpProtocol = httpProtocol;
        this.grpcProtocol = grpcProtocol;
    }

    @Override
    protected void initChannel(@Nonnull Channel channel) {

        var target = channel.remoteAddress();

        if (log.isDebugEnabled())
            log.debug("conn = {}, target = {}, init gRPC proxy channel", connId, target);

        var pipeline = channel.pipeline();

        // Set up foundation HTTP/2 proxy channel
        setupHttp2(pipeline, target);

        // Now add the gRPC protocol handlers
        setupGrpcTranslation(pipeline);

        // Finally, add the HTTP proxy and router link, which are closest to the core router
        setupRouterLink(pipeline);
    }

    private void setupHttp2(ChannelPipeline pipeline, SocketAddress target) {

        var initialSettings = (Http2Settings) null;

        // Use a larger initial frame / window size for data transfers

        if (isDataRoute()) {

            log.info("conn = {}, target = {}, configured for bulk data transfer", connId, target);

            initialSettings = new Http2Settings()
                    .maxFrameSize(Http2FlowControl.TRAC_DATA_MAX_FRAME_SIZE)
                    .initialWindowSize(Http2FlowControl.TRAC_DATA_INITIAL_WINDOW_SIZE);
        }
        else {

            initialSettings = new Http2Settings()
                    .maxFrameSize(Http2FlowControl.HTTP2_DEFAULT_MAX_FRAME_SIZE)
                    .initialWindowSize(Http2FlowControl.HTTP2_DEFAULT_INITIAL_WINDOW_SIZE);
        }

        var http2Codec = Http2FrameCodecBuilder.forClient()
                .initialSettings(initialSettings)
                .autoAckSettingsFrame(true)
                .autoAckPingFrame(true)
                .validateHeaders(true);

        // For trace logging, add an HTTP/2 frame logger
        // Use DEBUG level, setting log level = TRACE on the frame logger logs the content of data frames
        if (log.isTraceEnabled())
            http2Codec.frameLogger(new Http2FrameLogger(LogLevel.DEBUG));

        var http2FlowControl = new Http2FlowControl(connId, target, initialSettings);

        pipeline.addLast(HTTP2_FRAME_CODEC, http2Codec.build());
        pipeline.addLast(HTTP2_FLOW_CTRL, http2FlowControl);
    }

    private void setupGrpcTranslation(ChannelPipeline pipeline) {

        switch (grpcProtocol) {

            case GRPC:
                pipeline.addLast(GRPC_PROXY_HANDLER, new GrpcProxy(connId));
                break;

            case GRPC_WEB:
                pipeline.addLast(GRPC_PROXY_HANDLER, new GrpcProxy(connId));
                pipeline.addLast(GRPC_WEB_PROXY_HANDLER, new GrpcWebProxy(connId));
                break;

            case GRPC_WEBSOCKETS:
                pipeline.addLast(GRPC_PROXY_HANDLER, new GrpcProxy(connId));
                pipeline.addLast(GRPC_WEB_PROXY_HANDLER, new GrpcWebProxy(connId));
                pipeline.addLast(GRPC_WEBSOCKETS_TRANSLATOR, new WebSocketsTranslator(connId));
                break;

            default:
                throw new EUnexpected();
        }
    }

    private void setupRouterLink(ChannelPipeline pipeline) {

        switch (httpProtocol) {

            case HTTP_1_0:
            case HTTP_1_1:
                pipeline.addLast(HTTP_1_TO_2_PROXY, new Http1to2Proxy(routeConfig, connId));
                pipeline.addLast(CORE_ROUTER_LINK, routerLink);
                break;

            case WEBSOCKETS:
                pipeline.addLast(CORE_ROUTER_LINK, routerLink);
                break;

            default:

                var message = String.format(
                        "HTTP protocol version [%s] is not supported for target [%s]",
                        httpProtocol.name(), routeConfig.getRouteName());

                throw new ENetworkHttp(HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED.code(), message);
        }
    }

    private boolean isDataRoute() {

        return routeConfig.getMatch().getPath().contains(DATA_API_NAME);
    }
}
