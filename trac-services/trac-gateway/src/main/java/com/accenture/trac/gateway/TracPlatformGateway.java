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

package com.accenture.trac.gateway;

import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.plugin.PluginManager;
import com.accenture.trac.common.service.CommonServiceBase;
import com.accenture.trac.gateway.config.*;
import com.accenture.trac.gateway.config.helpers.ConfigTranslator;
import com.accenture.trac.gateway.exec.Route;
import com.accenture.trac.gateway.exec.RouteBuilder;
import com.accenture.trac.gateway.routing.*;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class TracPlatformGateway extends CommonServiceBase {

    /*
     * This version of the gateway provides some basic structures, including a configurable router component
     * and handler pipelines for HTTP, gRPC (inc. gRPC-Web) and REST proxies. It is intended to provide enough
     * functionality to allow platform and client development work to proceed. As such it handles the central code
     * pathways and some of the most obvious/common errors. It is not in a production-ready state!
     *
     * The expectation is that a unit of work will be dedicated to gateway when more of the core platform
     * components are complete.
     */

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private EventLoopGroup bossGroup = null;
    private EventLoopGroup workerGroup = null;


    public TracPlatformGateway(PluginManager pluginManager, ConfigManager configManager) {

        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) throws InterruptedException {

        GatewayConfig gatewayConfig;
        short proxyPort;
        List<Route> routes;

        try {
            log.info("Preparing gateway config...");

            var rawConfig = configManager.loadRootConfigObject(RootConfig.class);
            var config = ConfigTranslator.translateServiceRoutes(rawConfig);

            gatewayConfig = config.getTrac().getGateway();
            proxyPort = gatewayConfig.getProxy().getPort();

            routes = RouteBuilder.buildAll(gatewayConfig.getRoutes());

            log.info("Gateway config looks ok");
        }
        catch (Exception e) {

            var errorMessage = "There was an error preparing the gateway config: " + e.getMessage();
            log.error(errorMessage, e);
            throw new EStartup(errorMessage, e);
        }

        try {

            log.info("Starting the gateway server on port {}...", proxyPort);

            // The protocol negotiator is the top level initializer for new inbound connections
            var protocolNegotiator = new HttpProtocolNegotiator(gatewayConfig, routes);

            // TODO: Review configuration of thread pools and channel options

            bossGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("boss"));
            workerGroup = new NioEventLoopGroup(6, new DefaultThreadFactory("worker"));

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

        var bossShutdown = bossGroup.shutdownGracefully();
        bossShutdown.await(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);

        if (!bossShutdown.isSuccess()) {

            log.error("Gateway shutdown did not complete successfully in the allotted time");
            return -1;
        }

        log.info("Waiting for existing connections to clear...");

        var shutdownElapsedTime = Duration.between(shutdownStartTime, Instant.now());
        var shutdownTimeRemaining = shutdownTimeout.minus(shutdownElapsedTime);

        var workerShutdown = workerGroup.shutdownGracefully();
        workerShutdown.await(shutdownTimeRemaining.getSeconds(), TimeUnit.SECONDS);

        if (!workerShutdown.isSuccess()) {

            log.error("Gateway shutdown did not complete successfully in the allotted time");
            return -1;
        }

        log.info("All gateway connections are closed");
        return 0;
    }

    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracPlatformGateway.class, args);
    }
}
