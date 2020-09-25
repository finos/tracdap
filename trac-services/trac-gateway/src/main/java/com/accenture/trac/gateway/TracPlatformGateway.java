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

import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.config.StandardArgsProcessor;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.util.VersionInfo;
import com.accenture.trac.gateway.routing.BasicRouteMatcher;
import com.accenture.trac.gateway.routing.RoutingConfig;
import com.accenture.trac.gateway.routing.RoutingHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


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

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConfigManager configManager;

    public TracPlatformGateway(ConfigManager configManager) {

        this.configManager = configManager;
    }

    public void run() throws Exception {
        var componentName = VersionInfo.getComponentName(TracPlatformGateway.class);
        var componentVersion = VersionInfo.getComponentVersion(TracPlatformGateway.class);
        log.info("{} {}", componentName, componentVersion);
        log.info("Gateway is starting...");

        var properties = configManager.loadRootProperties();
        var gwPort = readConfigInt(properties, GW_PORT_CONFIG_KEY, null);
        var metaSvcHost = readConfigString(properties, META_SVC_HOST_CONFIG_KEY, null);
        var metaSvcPort = readConfigInt(properties, META_SVC_PORT_CONFIG_KEY, null);

        log.info("Configuring API routes...");

        var metaApiRoutes = TracApiConfig.metaApiRoutes(metaSvcHost, metaSvcPort);
        var metaApiTrustedRoutes = TracApiConfig.metaApiTrustedRoutes(metaSvcHost, metaSvcPort);

        var routingConfig = RoutingConfig.newBlankConfig()
                .addRoute(new BasicRouteMatcher("trac-meta"), () -> new RoutingHandler(metaApiRoutes))
                .addRoute(new BasicRouteMatcher("trac-meta-trusted"), () -> new RoutingHandler(metaApiTrustedRoutes));

        log.info("Opening gateway on port {}...", gwPort);

        EventLoopGroup bossGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("boss"));
        EventLoopGroup workerGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("worker"));

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();

            bootstrap
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) // (3)
                    .childHandler(new TracChannelInitializer(routingConfig))
                    .option(ChannelOption.SO_BACKLOG, 128)          // (5)
                    .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

            // Bind and start to accept incoming connections.
            ChannelFuture f = bootstrap
                    .bind(gwPort)
                    .sync(); // (7)

            log.info("TRAC Platform Gateway is up");

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
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

            var componentName = VersionInfo.getComponentName(TracPlatformGateway.class);
            var componentVersion = VersionInfo.getComponentVersion(TracPlatformGateway.class);
            var startupBanner = String.format(">>> %s %s", componentName, componentVersion);
            System.out.println(startupBanner);

            var standardArgs = StandardArgsProcessor.processArgs(componentName, args);

            System.out.println(">>> Working directory: " + standardArgs.getWorkingDir());
            System.out.println(">>> Config file: " + standardArgs.getConfigFile());
            System.out.println();

            var configManager = new ConfigManager(standardArgs);
            configManager.initConfigPlugins();
            configManager.initLogging();

            var gateway = new TracPlatformGateway(configManager);
            gateway.run();

            System.exit(0);
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
