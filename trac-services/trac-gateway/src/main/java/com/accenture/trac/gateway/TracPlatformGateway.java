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

import com.accenture.trac.common.config.ConfigBootstrap;
import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.util.VersionInfo;
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
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class TracPlatformGateway {

    // This is a quick first version of the platform gateway to provide REST -> gRPC translation.
    // A full implementation will need much more sophistication around how API routes are generated,
    // ideally using code generation from .proto files to provide the route configuration. It will
    // also need to support HTTP protocol detection, exposing gRPC and routes for static content,
    // load balancing, authentication plugins...

    // The expectation is that the gateway will be substantially re-written at some later point, when
    // more of the core platform components are completed.

    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.of(10, ChronoUnit.SECONDS);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConfigManager configManager;

    private EventLoopGroup bossGroup = null;
    private EventLoopGroup workerGroup = null;


    public TracPlatformGateway(ConfigManager configManager) {

        this.configManager = configManager;
    }

    public void start() {

        var componentName = VersionInfo.getComponentName(TracPlatformGateway.class);
        var componentVersion = VersionInfo.getComponentVersion(TracPlatformGateway.class);
        log.info("{} {}", componentName, componentVersion);

        GatewayConfig gatewayConfig;
        short proxyPort;
        List<Route> routes;

        try {
            log.info("Preparing gateway config...");

            var rawConfig = configManager.loadRootConfig(RootConfig.class);
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
            startupFuture.sync();

            // No need to keep a reference to the server channel
            // Shutdown is managed using the event loop groups

            // Install a shutdown handler for a graceful exit
            var shutdownThread = new Thread(this::jvmShutdownHook, "shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownThread);

            log.info("Gateway server is up and running");
        }
        catch (InterruptedException e) {

            log.error("Startup sequence was interrupted");
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {

            var errorMessage = "Gateway server failed to start: " + e.getMessage();
            log.error(errorMessage, e);

            if (workerGroup != null)
                workerGroup.shutdownGracefully();

            if (bossGroup != null)
                bossGroup.shutdownGracefully();

            throw new EStartup(e.getMessage(), e);
        }
    }

    public void stop() {

        log.info("Gateway server is stopping...");

        var shutdownStartTime = Instant.now();
        var bossShutdown = bossGroup.shutdownGracefully();
        var workerShutdown = workerGroup.shutdownGracefully();

        // Prevent interruption for the duration of the shutdown timeout
        // If someone really wants to kill the process in that time, they can send a hard kill signal

        bossShutdown.awaitUninterruptibly(DEFAULT_SHUTDOWN_TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        var shutdownElapsedTime = Duration.between(shutdownStartTime, Instant.now());
        var shutdownTimeRemaining = DEFAULT_SHUTDOWN_TIMEOUT.minus(shutdownElapsedTime);

        workerShutdown.awaitUninterruptibly(shutdownTimeRemaining.getSeconds(), TimeUnit.SECONDS);

        // Closing messages are written to stdout / stderr as well as the logs
        // The default logging configuration disables the logging shutdown hook
        // However if an alternate configuration is supplied the logging shutdown hook may be active
        // In this case, the logging system will be stopped before the shutdown sequence completes

        if (bossShutdown.isSuccess() && workerShutdown.isSuccess()) {

            log.info("Gateway server has gone down cleanly");
            System.out.println("Gateway server has gone down cleanly");

            // Setting exit code 0 means the process will finish successfully,
            // even if the shutdown was triggered by an interrupt signal

            Runtime.getRuntime().halt(0);
        }
        else {

            log.error("Gateway server shutdown did not complete in the allotted time");
            System.out.println("Gateway server shutdown did not complete in the allotted time");

            // Calling System.exit from inside a shutdown hook can lead to undefined behavior (often JVM hangs)
            // This is because it calls back into the shutdown handlers

            // Runtime.halt will stop the JVM immediately without calling back into shutdown hooks
            // At this point everything is either stopped or has failed to stop
            // So, it should be ok to use Runtime.halt and report the exit code

            Runtime.getRuntime().halt(-1);
        }
    }

    public void jvmShutdownHook() {

        log.info("Shutdown request received");
        this.stop();
    }

    public static void main(String[] args) {

        try {

            var config = ConfigBootstrap.useCommandLine(TracPlatformGateway.class, args);
            var gateway = new TracPlatformGateway(config);
            gateway.start();
        }
        catch (EStartup e) {

            if (e.isQuiet())
                System.exit(e.getExitCode());

            System.err.println("The service failed to start: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(e.getExitCode());
        }
        catch (Exception e) {

            System.err.println("There was an unexpected error on the main thread: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(-1);
        }
    }
}
