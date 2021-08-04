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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.DefaultShutdownCallbackRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class TracPlatformGateway {

    /*
     * This version of the gateway provides some basic structures, including a configurable router component
     * and handler pipelines for HTTP, gRPC (inc. gRPC-Web) and REST proxies. It is intended to provide enough
     * functionality to allow platform and client development work to proceed. As such it handles the central code
     * pathways and some of the most obvious/common errors. It is not in a production-ready state!
     *
     * The expectation is that a unit of work will be dedicated to gateway when more of the core platform
     * components are complete.
     */

    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.of(10, ChronoUnit.SECONDS);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConfigManager configManager;

    private EventLoopGroup bossGroup = null;
    private EventLoopGroup workerGroup = null;


    public TracPlatformGateway(ConfigManager configManager) {

        this.configManager = configManager;
    }

    public void start() {

        // Do not register a shutdown hook unless explicitly requested
        start(false);
    }

    public void start(boolean registerShutdownHook) {

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

            // If requested, install a shutdown handler for a graceful exit
            // This is needed when running a real server instance, but not when running embedded tests
            if (registerShutdownHook) {
                var shutdownThread = new Thread(this::jvmShutdownHook, "shutdown");
                Runtime.getRuntime().addShutdownHook(shutdownThread);
            }

            // Keep the logging system active while shutdown hooks are running
            disableLog4jShutdownHook();

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

    public int stop() {

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

        int exitCode;

        if (bossShutdown.isSuccess() && workerShutdown.isSuccess()) {

            log.info("Gateway server has gone down cleanly");
            System.out.println("Gateway server has gone down cleanly");

            // Setting exit code 0 means the process will finish successfully,
            // even if the shutdown was triggered by an interrupt signal

            exitCode = 0;
        }
        else {

            log.error("Gateway server shutdown did not complete in the allotted time");
            System.out.println("Gateway server shutdown did not complete in the allotted time");

            exitCode = -1;
        }

        // The logging system can be shut down now that the shutdown hook has completed
        explicitLog4jShutdown();

        // Do not forcibly exit the JVM inside stop()
        // Exit code can be checked by embedded tests when the JVM will continue running
        return exitCode;
    }

    private void disableLog4jShutdownHook() {

        // The default logging configuration disables logging in a shutdown hook
        // The logging system goes down when shutdown is initiated and messages in the shutdown sequence are lost
        // Removing the logging shutdown hook allows closing messages to go to the logs as normal

        // This is an internal API in Log4j, there is a config setting available
        // This approach means admins with custom logging configs don't need to know about shutdown hooks
        // Anyway we would need to use the internal API to explicitly close the context

        try {
            var logFactory = (Log4jContextFactory) LogManager.getFactory();
            ((DefaultShutdownCallbackRegistry) logFactory.getShutdownCallbackRegistry()).stop();
        }
        catch (Exception e) {

            // In case disabling the shutdown hook doesn't work, do not interrupt the startup sequence
            // As a backup, final shutdown messages are written to stdout / stderr

            log.warn("Logging shutdown hook is active (shutdown messages may be lost)");
        }
    }

    private void explicitLog4jShutdown() {

        // Since the logging shutdown hook is disabled, provide a way to explicitly shut down the logging system
        // Especially important for custom configurations connecting to external logging services or databases
        // In the event that disabling the shutdown hook did not work, this method will do nothing

        var logContext = LogManager.getContext();

        if (logContext instanceof LoggerContext)
            Configurator.shutdown((LoggerContext) logContext);
    }

    private void jvmShutdownHook() {

        log.info("Shutdown request received");

        var exitCode = this.stop();

        // Calling System.exit from inside a shutdown hook can lead to undefined behavior (often JVM hangs)
        // This is because it calls back into the shutdown handlers

        // Runtime.halt will stop the JVM immediately without calling back into shutdown hooks
        // At this point everything is either stopped or has failed to stop
        // So, it should be ok to use Runtime.halt and report the exit code

        Runtime.getRuntime().halt(exitCode);
    }

    public static void main(String[] args) {

        try {

            var config = ConfigBootstrap.useCommandLine(TracPlatformGateway.class, args);
            var gateway = new TracPlatformGateway(config);
            gateway.start(true);
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
