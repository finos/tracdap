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

package org.finos.tracdap.gateway;

import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.middleware.NettyConcern;
import org.finos.tracdap.common.netty.ProtocolNegotiator;
import org.finos.tracdap.common.netty.EventLoopScheduler;
import org.finos.tracdap.common.netty.NettyHelpers;
import org.finos.tracdap.common.netty.ProtocolHandler;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.TracNettyConfig;
import org.finos.tracdap.common.service.TracServiceBase;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.ServiceConfig;
import org.finos.tracdap.gateway.auth.AuthConcern;
import org.finos.tracdap.gateway.builders.RedirectBuilder;
import org.finos.tracdap.gateway.builders.RouteBuilder;
import org.finos.tracdap.gateway.exec.Redirect;
import org.finos.tracdap.gateway.exec.Route;
import org.finos.tracdap.gateway.routing.Http1Router;
import org.finos.tracdap.gateway.routing.WebSocketsRouter;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class TracPlatformGateway extends TracServiceBase {

    /*
     * This version of the gateway provides some basic structures, including a configurable router component
     * and handler pipelines for HTTP, gRPC (inc. gRPC-Web) and REST proxies. It is intended to provide enough
     * functionality to allow platform and client development work to proceed. As such it handles the central code
     * pathways and some of the most obvious/common errors. It is not in a production-ready state!
     *
     * The expectation is that a unit of work will be dedicated to gateway when more of the core platform
     * components are complete.
     */

    private static final int MAX_SERVICE_CORES = 6;
    private static final int MIN_SERVICE_CORES = 2;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private ServiceConfig serviceConfig;

    private EventLoopGroup bossGroup = null;
    private EventLoopGroup workerGroup = null;

    private AtomicInteger connId;
    private List<Route> routes;
    private List<Redirect> redirects;

    public static void main(String[] args) {

        TracServiceBase.svcMain(TracPlatformGateway.class, args);
    }

    public TracPlatformGateway(PluginManager pluginManager, ConfigManager configManager) {
        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) throws InterruptedException {

        short proxyPort;

        try {
            log.info("Preparing gateway config...");

            var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
            serviceConfig = platformConfig.getServicesOrThrow(ConfigKeys.GATEWAY_SERVICE_KEY);
            proxyPort = (short) serviceConfig.getPort();

            connId = new AtomicInteger();
            routes = new RouteBuilder().buildRoutes(platformConfig);
            redirects = new RedirectBuilder().buildRedirects(platformConfig);

            log.info("Gateway config looks ok");
        }
        catch (Exception e) {

            var errorMessage = "There was an error preparing the gateway config: " + e.getMessage();
            log.error(errorMessage, e);
            throw new EStartup(errorMessage, e);
        }

        try {

            log.info("Starting the gateway server on port {}...", proxyPort);

            // Currently HTTP and WS are supported
            var mainHandler = ProtocolHandler.create()
                    .withHttp(this::httpHandler)
                    .withWebsocket(this::websocketHandler, this::websocketConfig);

            // Common framework for cross-cutting concerns
            var commonConcerns = buildCommonConcerns();

            // The protocol negotiator is the top level initializer for new inbound connections
            var protocolNegotiator = new ProtocolNegotiator(mainHandler, commonConcerns);

            var bossThreadCount = 1;
            var bossExecutor = NettyHelpers.eventLoopExecutor("gw-boss");
            var bossScheduler = EventLoopScheduler.roundRobin();

            bossGroup = NettyHelpers.nioEventLoopGroup(bossExecutor, bossScheduler, bossThreadCount);

            var serviceCoresAvailable= Runtime.getRuntime().availableProcessors() - 1;
            var serviceThreadCount = Math.max(Math.min(serviceCoresAvailable, MAX_SERVICE_CORES), MIN_SERVICE_CORES);
            var serviceExecutor = NettyHelpers.eventLoopExecutor("gw-svc");
            var serviceScheduler = EventLoopScheduler.preferLoopAffinity();

            workerGroup = NettyHelpers.nioEventLoopGroup(serviceExecutor, serviceScheduler, serviceThreadCount);

            var bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(protocolNegotiator)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // Bind and start to accept incoming connections.
            var startupFuture = bootstrap.bind(proxyPort);

            // Block until the server channel is ready - it's just easier this way!
            // The sync call will rethrow any errors, so they can be handled before leaving the start() method

            startupFuture.await();

            if (startupFuture.isSuccess()) {

                var socket = startupFuture.channel().localAddress();
                log.info("Server socket open: {}", socket);
            }
            else {

                var cause = startupFuture.cause();
                var message = "Server socket could not be opened: " + cause.getMessage();
                log.error(message);

                throw new EStartup(message, cause);
            }

            // No need to keep a reference to the server channel
            // Shutdown is managed using the event loop groups
        }
        catch (Exception e) {

            if (workerGroup != null)
                workerGroup.shutdownGracefully();

            if (bossGroup != null)
                bossGroup.shutdownGracefully();

            // The call to startupFuture.sync can throw error types that are not RuntimeException
            // Java should not allow this, but it happens!
            // Wrap any other error types in an EStartup, so as not to confuse top level error handling

            if (Set.of(RuntimeException.class, InterruptedException.class).contains(e.getClass()))
                throw e;
            else
                throw new EStartup(e.getMessage(), e);
        }
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) throws InterruptedException {

        var shutdownStartTime = Instant.now();

        log.info("Closing the gateway to new connections...");

        var bossShutdown = bossGroup.shutdownGracefully(0, shutdownTimeout.getSeconds(), TimeUnit.SECONDS);
        bossShutdown.await(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);

        if (!bossShutdown.isSuccess()) {

            log.error("Gateway shutdown did not complete successfully in the allotted time");
            return -1;
        }

        log.info("Waiting for existing connections to clear...");

        var shutdownElapsedTime = Duration.between(shutdownStartTime, Instant.now());
        var shutdownTimeRemaining = shutdownTimeout.minus(shutdownElapsedTime);

        var workerShutdown = workerGroup.shutdownGracefully(0, shutdownTimeRemaining.getSeconds(), TimeUnit.SECONDS);
        workerShutdown.await(shutdownTimeRemaining.getSeconds(), TimeUnit.SECONDS);

        if (!workerShutdown.isSuccess()) {

            log.error("Gateway shutdown did not complete successfully in the allotted time");
            return -1;
        }

        log.info("All gateway connections are closed");
        return 0;
    }

    private NettyConcern buildCommonConcerns() {

        var commonConcerns = TracNettyConfig.coreConcerns("gateway service", serviceConfig);

        var authConcern = new AuthConcern(configManager);
        commonConcerns = commonConcerns.addFirst(authConcern);

        // Additional cross-cutting concerns configured by extensions
        for (var extension : pluginManager.getExtensions()) {
            commonConcerns = extension.addGatewayConcerns(commonConcerns);
        }

        return commonConcerns.build();
    }

    private ChannelHandler httpHandler() {
        return new Http1Router(routes, redirects, connId.getAndIncrement());
    }

    private ChannelHandler websocketHandler() {
        return new WebSocketsRouter(routes, connId.getAndIncrement());
    }

    private WebSocketServerProtocolConfig websocketConfig(HttpRequest upgradeRequest) {

        return WebSocketServerProtocolConfig.newBuilder()
                .websocketPath(upgradeRequest.uri())
                .subprotocols("grpc-websockets")
                .allowExtensions(true)
                .handleCloseFrames(false)
                .build();
    }
}
