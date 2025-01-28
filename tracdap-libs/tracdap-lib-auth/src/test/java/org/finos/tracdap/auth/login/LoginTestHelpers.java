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

package org.finos.tracdap.auth.login;

import org.finos.tracdap.common.netty.NettyHelpers;
import org.finos.tracdap.common.netty.ProtocolHandler;
import org.finos.tracdap.common.netty.ProtocolNegotiator;
import org.finos.tracdap.common.service.TracNettyConfig;
import org.finos.tracdap.config.ServiceConfig;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class LoginTestHelpers {

    public static Runnable setupNettyHttp1(Supplier<ChannelHandler>  http1Handler, short port) throws InterruptedException {

        return setupNettyServer(http1Handler, null, port);
    }

    public static Runnable setupNettyHttp2(Supplier<ChannelHandler>  http2Handler, short port) throws InterruptedException {

        return setupNettyServer(null, http2Handler, port);
    }

    public static Runnable setupNettyServer(
            Supplier<ChannelHandler>  http1Handler,
            Supplier<ChannelHandler>  http2Handler,
            short port) throws InterruptedException {

        var bossFactory = NettyHelpers.threadFactory("test-boss");
        var workerFactory = NettyHelpers.threadFactory("test-svc");

        var bossGroup = new NioEventLoopGroup(1, bossFactory);
        var workerGroup = new NioEventLoopGroup(2, workerFactory);
        var allocator = ByteBufAllocator.DEFAULT;

        // Service config is not supplied atm
        var blankServiceConfig = ServiceConfig.newBuilder().build();

        // Set up the supplied handlers
        var mainHandler = ProtocolHandler.create()
                .withHttp(http1Handler)
                .withHttp2(http2Handler);

        // Use default core concerns for testing
        var commonConcerns = TracNettyConfig.coreConcerns("test auth service", blankServiceConfig).build();

        var protocolNegotiator = new ProtocolNegotiator(mainHandler, commonConcerns);

        var bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(protocolNegotiator)
                .option(ChannelOption.ALLOCATOR, allocator)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        var server = bootstrap.bind(port);
        server.sync();

        return () -> {

            workerGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
            bossGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        };
    }
}
