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

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.gateway.proxy.http.Http1ProxyBuilder;
import com.accenture.trac.gateway.proxy.grpc.GrpcProxyBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;


public class Http1Router extends SimpleChannelInboundHandler<HttpObject> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static int nextRouterId = 0;
    private final int routerId = nextRouterId++;

    private final EventLoopGroup workerGroup;
    private ChannelHandlerContext clientCtx;

    private Bootstrap bootstrap;
    private final Map<String, ClientState> clients;
    private final List<Route> routes;

    private long currentInboundRequest;
    private long currentOutboundRequest;
    private final Map<Long, RequestState> requests;

    public Http1Router(EventLoopGroup workerGroup) {

        this.workerGroup = workerGroup;

        this.clients = new HashMap<>();
        this.requests = new HashMap<>();
        this.routes = new ArrayList<>();

        addHardCodedRoutes();
    }

    private void addHardCodedRoutes() {

        var metaApiRoute = new Route();
        metaApiRoute.clientKey = "API: Metadata";
        metaApiRoute.matcher = (uri, method, headers) -> uri
                .getPath()
                .matches("^/trac.api.TracMetadataApi/.+");
        metaApiRoute.host = "localhost";
        metaApiRoute.port = 8086;
        metaApiRoute.initializer = GrpcProxyBuilder::new;

        var defaultRoute = new Route();
        defaultRoute.clientKey = "Static content";
        defaultRoute.matcher = (uri, method, headers) -> true;;
        defaultRoute.host = "localhost";
        defaultRoute.port = 8090;
        defaultRoute.initializer = Http1ProxyBuilder::new;

        routes.add(0, metaApiRoute);
        routes.add(1, defaultRoute);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

        log.info("HTTP 1.1 handler added with ID {}", routerId);

        var eventLoop = ctx.channel().eventLoop();
        var executor = ctx.executor();

        this.bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, ctx.alloc());

        clientCtx = ctx;

        super.handlerAdded(ctx);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {

        log.info("HTTP 1.1 channel registered");

        super.channelRegistered(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {

        processInbound(ctx, msg);
    }

    public void processInbound(ChannelHandlerContext ctx, HttpObject msg) throws Exception {

        if (msg instanceof HttpRequest) {
            processNewRequest(ctx, (HttpRequest) msg);
        }
        else if (msg instanceof HttpContent) {
            processRequestContent(ctx, (HttpContent) msg);
        }
        else {
            throw new EUnexpected();
        }

        if (msg instanceof LastHttpContent) {
            processEndOfRequest(ctx, (LastHttpContent) msg);
        }
    }

    public void processOutbound(ChannelHandlerContext ctx, HttpObject msg, long requestId) {

        if (requestId == currentOutboundRequest) {

        }
        else {
            // queue
        }

        if (msg instanceof LastHttpContent) {
            currentOutboundRequest++;
        }
    }


    private void processNewRequest(ChannelHandlerContext ctx, HttpRequest msg) throws Exception {

        // Increment counter for inbound requests
        currentInboundRequest++;

        // Set up a new request state with initial values
        var request = new RequestState();
        request.requestId = currentInboundRequest;
        request.status = RequestStatus.RECEIVING;
        requests.put(request.requestId, request);

        // Look for a matching route for this request
        var uri = URI.create(msg.uri());
        var method = msg.method();
        var headers = msg.headers();

        Route selectedRoute = null;

        for (var route : this.routes) {
            if (route.matcher.matches(uri, method, headers)) {

                log.info("{} {} -> {} ({})", method, uri, route.clientKey, routerId);
                selectedRoute = route;
                break;
            }
        }

        // If there is no matching route, fail the request and respond with 404
        if (selectedRoute == null) {

            // No route available, send a 404 back to the client
            log.warn("No route available: " + method + " " + uri);

            var protocolVersion = msg.protocolVersion();
            var response = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.NOT_FOUND);
            ctx.writeAndFlush(response);

            request.status = RequestStatus.FAILED;

            // No need to release, base class does it automatically
            // ReferenceCountUtil.release(msg);

            return;
        }

        request.clientKey = selectedRoute.clientKey;

        var existingClient = clients.getOrDefault(request.clientKey, null);
        var client = existingClient != null ? existingClient : new ClientState();

        if (existingClient == null)
            clients.put(request.clientKey, client);

        if (client.channel == null) {

            var channelActiveFuture = ctx.newPromise();

            var channelInit = selectedRoute
                    .initializer
                    .makeInitializer(clientCtx, channelActiveFuture);

            var channelOpenFuture = bootstrap
                    .handler(channelInit)
                    .connect(selectedRoute.host, selectedRoute.port);

            channelActiveFuture.addListener((ChannelFutureListener) future -> {

                if (future.isSuccess()) {
                    log.info("Connection to server ok");

                    var outboundHead = client.outboundQueue.poll();

                    while (outboundHead != null) {
                        client.channel.write(outboundHead);
                        outboundHead = client.outboundQueue.poll();
                    }

                    client.channel.flush();
                }
                else
                    log.error("Connection to server failed", future.cause());
            });

            client.channel = channelOpenFuture.channel();
            client.channelOpenFuture = channelOpenFuture;
            client.channelActiveFuture = channelActiveFuture;
        }

        if (client.channel.isActive())
            client.channel.write(msg);
        else
            client.outboundQueue.add(msg);
    }

    private void processRequestContent(ChannelHandlerContext ctx, HttpContent msg) {

        var requestState = requests.getOrDefault(currentInboundRequest, null);

        if (requestState == null)
            throw new EUnexpected();

        var client = clients.getOrDefault(requestState.clientKey, null);

        if (client == null)
            throw new EUnexpected();

        msg.retain();

        if (client.channel.isActive())
            client.channel.write(msg);
        else
            client.outboundQueue.add(msg);
    }

    private void processEndOfRequest(ChannelHandlerContext ctx, LastHttpContent msg) {

        var requestState = requests.getOrDefault(currentInboundRequest, null);

        if (requestState == null)
            throw new EUnexpected();

        var client = clients.getOrDefault(requestState.clientKey, null);

        if (client == null)
            throw new EUnexpected();

        if (client.channel.isActive())
            client.channel.flush();
    }


    private enum RequestStatus {
        RECEIVING,
        RECEIVING_BIDI,
        WAITING_FOR_RESPONSE,
        RESPONDING,
        SUCCEEDED,
        FAILED
    }

    private static class RequestState {

        long requestId;
        RequestStatus status;
        String clientKey;
    }

    private static class ClientState {

        Channel channel;
        ChannelFuture channelOpenFuture;
        ChannelFuture channelActiveFuture;

        Queue<Object> outboundQueue = new LinkedList<>();
    }

    private static class Route {

        IRouteMatcher matcher;
        String clientKey;

        String host;
        int port;
        ProxyInitializer initializer;
    }

    @FunctionalInterface
    private interface ProxyInitializer {

        ChannelInitializer<Channel> makeInitializer(
                ChannelHandlerContext routerCtx,
                ChannelPromise routeActivePromise);
    }
}
