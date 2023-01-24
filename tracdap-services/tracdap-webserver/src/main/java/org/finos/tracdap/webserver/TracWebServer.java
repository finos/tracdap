/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.webserver;

import org.finos.tracdap.common.auth.AuthSetup;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.CommonServiceBase;
import org.finos.tracdap.config.PlatformConfig;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Supplier;


public class TracWebServer extends CommonServiceBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private PlatformConfig platformConfig;

    private EventLoopGroup bossGroup = null;
    private EventLoopGroup workerGroup = null;

    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracWebServer.class, args);
    }

    public TracWebServer(PluginManager pluginManager, ConfigManager configManager) {

        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) throws InterruptedException {

        var serverPort = 8090;

        log.info("Starting the web server on port {}...", serverPort);

        platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);

        // JWT processor is responsible for signing and validating auth tokens
        var jwtValidator = AuthSetup.createValidator(platformConfig, configManager);

        // Handlers for all support protocols
        var http1Handler = (Supplier<Http1Server>) Http1Server::new;
        var http2Handler = (Supplier<Http2Server>) Http2Server::new;

        // The protocol negotiator is the top level initializer for new inbound connections
        var protocolNegotiator = new ProtocolNegotiator(jwtValidator, http1Handler, http2Handler);

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
        var startupFuture = bootstrap.bind(serverPort);

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
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) throws InterruptedException {
        return 0;
    }
}
