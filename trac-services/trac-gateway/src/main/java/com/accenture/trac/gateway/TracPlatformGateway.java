/*
 * Copyright 2020 Accenture Global Solutions Limited
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
import com.accenture.trac.gateway.routing.*;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class TracPlatformGateway {

    // This is a quick first version of the platform gateway to provide REST -> gRPC translation.
    // A full implementation will need much more sophistication around how API routes are generated,
    // ideally using code generation from .proto files to provide the route configuration. It will
    // also need to support HTTP protocol detection, exposing gRPC and routes for static content,
    // load balancing, authentication plugins...

    // The expectation is that the gateway will be substantially re-written at some later point, when
    // more of the core platform components are completed.

    private static final String GW_PORT_CONFIG_KEY = "trac.gw.api.port";
    private static final String META_SVC_HOST_CONFIG_KEY = "trac.gw.services.meta.host";
    private static final String META_SVC_PORT_CONFIG_KEY = "trac.gw.services.meta.port";

    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.of(10, ChronoUnit.SECONDS);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConfigManager configManager;

    private Channel serverChannel;
    private Thread mainThread;
    private Thread shutdownThread;


    public TracPlatformGateway(ConfigManager configManager) {

        this.configManager = configManager;
    }

    public void start() throws Exception {

        var componentName = VersionInfo.getComponentName(TracPlatformGateway.class);
        var componentVersion = VersionInfo.getComponentVersion(TracPlatformGateway.class);
        log.info("{} {}", componentName, componentVersion);
        log.info("Gateway is starting...");

        var rootConfig = configManager.loadRootConfig(RootConfig.class);
        var gatewayConfig = rootConfig.getTrac().getGateway();

        var proxyPort = gatewayConfig.getProxy().getPort();
        var routeConfigs = gatewayConfig.getRoutes();

        // log.info("Configuring API routes...");

//        var metaSvcHost = readConfigString(properties, META_SVC_HOST_CONFIG_KEY, null);
//        var metaSvcPort = readConfigInt(properties, META_SVC_PORT_CONFIG_KEY, null);
//        var metaApiRoutes = TracApiConfig.metaApiRoutes(metaSvcHost, metaSvcPort);
//        var metaApiTrustedRoutes = TracApiConfig.metaApiTrustedRoutes(metaSvcHost, metaSvcPort);
//        var routingConfig = RoutingConfig.newBlankConfig()
//                .addRoute(new BasicRouteMatcher("trac-meta"), () -> new RoutingHandler(metaApiRoutes))
//                .addRoute(new BasicRouteMatcher("trac-meta-trusted"), () -> new RoutingHandler(metaApiTrustedRoutes));

        log.info("Opening gateway on port {}...", proxyPort);

        EventLoopGroup bossGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("boss"));
        EventLoopGroup workerGroup = new NioEventLoopGroup(6, new DefaultThreadFactory("worker"));

        var protocolNegotiator = new HttpProtocolNegotiator(
                () -> new Http1Router(routeConfigs),
                () -> new Http2Router(routeConfigs));

//        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) // (3)
                    .childHandler(protocolNegotiator)
                    .option(ChannelOption.SO_BACKLOG, 128)          // (5)
                    .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

            this.mainThread = Thread.currentThread();
            this.shutdownThread = new Thread(this::jvmShutdownHook, "shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownThread);

            // Bind and start to accept incoming connections.
            serverChannel = bootstrap
                    .bind(proxyPort)
                    .sync()
                    .channel();

            log.info("TRAC Platform Gateway is up");

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            //f.channel().closeFuture().sync();
//        }
//        catch (Exception e) {
//            workerGroup.shutdownGracefully();
//            bossGroup.shutdownGracefully();
//        }
    }

    public void stop() {

        try {

            log.info("Gateway is stopping...");

            var closeResult = serverChannel.close();
            closeResult.await(DEFAULT_SHUTDOWN_TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            if (closeResult.isDone() && closeResult.isSuccess())
                log.info("Gateway port closed");

            else
                log.error("Gateway port failed to close");
        }
        catch (InterruptedException e) {

            System.err.println("TRAC Metadata service was interrupted during shutdown");
            log.warn("TRAC Metadata service was interrupted during shutdown");

            Thread.currentThread().interrupt();
        }
    }

    public void jvmShutdownHook() {
//
//        try {
//            log.info("Shutdown request received");
//
//            this.stop();
//            this.mainThread.join(5000);
//
//            log.info("Normal shutdown complete");
//            System.out.println("Normal shutdown complete");
//        }
//        catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
    }

    public void blockUntilShutdown() throws InterruptedException {

        try {
            log.info("Begin normal operations");

            serverChannel.closeFuture().await();

            System.out.println("Going down in main");

            //log.info("TRAC Platform Gateway is going down");
        }
        catch (InterruptedException e) {

            System.out.println("Going down in main int");

            log.info("TRAC Platform Gateway has been interrupted");
            throw e;
        }
    }

    private String readConfigString(Properties props, String propKey, String propDefault) {

        // TODO: Reading config needs to be centralised
        // Standard methods for handling defaults, valid ranges etc.
        // One option is to use proto to define config objects and automate parsing
        // This would work well where configs need to be sent between components, e.g. the TRAC executor

        var propValue = props.getProperty(propKey);

        if (propValue == null || propValue.isBlank()) {

            if (propDefault == null) {

                var message = "Missing required config property: " + propKey;
                log.error(message);
                throw new EStartup(message);
            }
            else
                return propDefault;
        }

        return propValue;
    }

    private int readConfigInt(Properties props, String propKey, String propDefault) {

        // TODO: Reading config needs to be centralised
        // Standard methods for handling defaults, valid ranges etc.
        // One option is to use proto to define config objects and automate parsing
        // This would work well where configs need to be sent between components, e.g. the TRAC executor

        var stringValue = readConfigString(props, propKey, propDefault);

        try {
            return Integer.parseInt(stringValue);
        }
        catch (NumberFormatException e) {

            var message = "Config property must be an integer: " + propKey + ", got value '" + stringValue + "'";
            log.error(message);
            throw new EStartup(message);
        }
    }

    public static void main(String[] args) {

        try {

            var config = ConfigBootstrap.useCommandLine(TracPlatformGateway.class, args);
            var gateway = new TracPlatformGateway(config);
            gateway.start();
            gateway.blockUntilShutdown();

            //System.exit(0);
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
