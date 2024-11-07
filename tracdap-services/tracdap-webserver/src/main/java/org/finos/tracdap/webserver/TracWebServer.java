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

package org.finos.tracdap.webserver;

import org.finos.tracdap.common.auth.internal.JwtSetup;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.netty.NettyHelpers;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.CommonServiceBase;
import org.finos.tracdap.common.storage.IFileStorage;
import org.finos.tracdap.common.storage.IStorageManager;
import org.finos.tracdap.config.PlatformConfig;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.netty.NettyAllocationManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class TracWebServer extends CommonServiceBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private BufferAllocator arrowAllocator = null;
    private ByteBufAllocator nettyAllocator = null;
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

        var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);

        if (!platformConfig.hasWebServer() || !platformConfig.getWebServer().getEnabled()) {

            var msg = "Web server is not enabled in the TRAC platform configuration";
            log.error(msg);
            throw new EStartup(msg);
        }

        // The storage layer uses Arrow's memory framework with ArrowBuf / BufferAllocator
        // Buffers returned from the storage layer are wrapped (zero-copy) to send on as Netty ByteBuf
        // Buffers allocated by Netty use nettyAllocator, which is separate atm

        // It would be quite easy to implement ByteBufAllocator as a wrapper on an Arrow BufferAllocator
        // This would let the web server use a single allocation framework

        var serviceconfig = platformConfig.getServicesOrThrow(ConfigKeys.WEB_SERVER_SERVICE_KEY);

        var arrowAllocatorConfig = RootAllocator
                .configBuilder()
                .allocationManagerFactory(NettyAllocationManager.FACTORY)
                .build();

        this.arrowAllocator = new RootAllocator(arrowAllocatorConfig);
        this.nettyAllocator = ByteBufAllocator.DEFAULT;

        // TODO: Review configuration of thread pools and channel options

        var bossFactory = NettyHelpers.threadFactory("webs-boss");
        var workerFactory = NettyHelpers.threadFactory("web-svc");

        bossGroup = new NioEventLoopGroup(2, bossFactory);
        workerGroup = new NioEventLoopGroup(6, workerFactory);

        // JWT processor is responsible for signing and validating auth tokens
        var jwtValidator = JwtSetup.createValidator(platformConfig, configManager);

        log.info("Accessing storage for content root...");

        var webServerConfig = platformConfig.getWebServer();

        // Set the storage key - in data service this is done by StorageManager
        var contentRootConfig = webServerConfig.getContentRoot().toBuilder()
                .putProperties(IStorageManager.PROP_STORAGE_KEY, "CONTENT_ROOT")
                .build();

        var contentStorage = pluginManager.createService(IFileStorage.class, contentRootConfig, configManager);
        contentStorage.start(workerGroup);

        // Handlers for all support protocols
        var contentServer = new ContentServer(platformConfig.getWebServer(), contentStorage);
        var http1Handler = (Supplier<Http1Server>) () -> new Http1Server(contentServer, arrowAllocator);
        var http2Handler = (Supplier<Http2Server>) () -> new Http2Server(contentServer);

        // The protocol negotiator is the top level initializer for new inbound connections
        var protocolNegotiator = new ProtocolNegotiator(jwtValidator, http1Handler, http2Handler);

        var bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(protocolNegotiator)
                .option(ChannelOption.ALLOCATOR, nettyAllocator)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections.
        log.info("Starting web server on port [{}]", serviceconfig.getPort());
        var startupFuture = bootstrap.bind(serviceconfig.getPort());

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

        var shutdownStartTime = Instant.now();

        log.info("Closing the web server to new connections...");

        var bossShutdown = bossGroup.shutdownGracefully();
        bossShutdown.await(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);

        if (!bossShutdown.isSuccess()) {

            log.error("Web server shutdown did not complete successfully in the allotted time");
            return -1;
        }

        log.info("Waiting for existing connections to clear...");

        var shutdownElapsedTime = Duration.between(shutdownStartTime, Instant.now());
        var shutdownTimeRemaining = shutdownTimeout.minus(shutdownElapsedTime);

        var workerShutdown = workerGroup.shutdownGracefully();
        workerShutdown.await(shutdownTimeRemaining.getSeconds(), TimeUnit.SECONDS);

        if (!workerShutdown.isSuccess()) {

            log.error("Web server shutdown did not complete successfully in the allotted time");
            return -1;
        }

        arrowAllocator.close();
        arrowAllocator = null;
        nettyAllocator = null;

        log.info("All web server connections are closed");
        return 0;
    }
}
