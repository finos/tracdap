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

package org.finos.tracdap.svc.auth;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.netty.NettyHelpers;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.CommonServiceBase;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;

import org.finos.tracdap.config.PlatformConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;


public class TracAuthenticationService extends CommonServiceBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private EventLoopGroup bossGroup = null;
    private EventLoopGroup workerGroup = null;
    private ByteBufAllocator allocator = null;


    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracAuthenticationService.class, args);
    }

    public TracAuthenticationService(PluginManager pluginManager, ConfigManager configManager) {

        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) throws InterruptedException {

        var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
        var serviceConfig = platformConfig.getServicesOrThrow(ConfigKeys.AUTHENTICATION_SERVICE_KEY);

        // TODO: Review configuration of thread pools and channel options

        var bossFactory = NettyHelpers.threadFactory("auth-boss");
        var workerFactory = NettyHelpers.threadFactory("auth-svc");

        bossGroup = new NioEventLoopGroup(2, bossFactory);
        workerGroup = new NioEventLoopGroup(6, workerFactory);
        allocator = ByteBufAllocator.DEFAULT;

        var providers = new ProviderLookup(platformConfig, configManager, pluginManager);
        var protocolNegotiator = new ProtocolNegotiator(providers);

        var bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(protocolNegotiator)
                .option(ChannelOption.ALLOCATOR, allocator)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections.
        log.info("Starting authentication service on port [{}]", serviceConfig.getPort());
        var startupFuture = bootstrap.bind(serviceConfig.getPort());

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

        log.info("Closing the authentication service to new connections...");

        var bossShutdown = bossGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
        bossShutdown.await(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);

        if (!bossShutdown.isSuccess()) {

            log.error("Authentication service did not go down cleanly in the allotted time");
            return -1;
        }

        log.info("Waiting for existing connections to clear...");

        var shutdownElapsedTime = Duration.between(shutdownStartTime, Instant.now());
        var shutdownTimeRemaining = shutdownTimeout.minus(shutdownElapsedTime);

        var workerShutdown = workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
        workerShutdown.await(shutdownTimeRemaining.getSeconds(), TimeUnit.SECONDS);

        if (!workerShutdown.isSuccess()) {

            log.error("Authentication service did not go down cleanly in the allotted time");
            return -1;
        }

        allocator = null;

        log.info("All connections are closed");

        return 0;
    }
}
