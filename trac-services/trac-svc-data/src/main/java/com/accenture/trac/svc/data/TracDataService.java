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

package com.accenture.trac.svc.data;

import com.accenture.trac.api.TrustedMetadataApiGrpc;
import com.accenture.trac.api.config.DataServiceConfig;
import com.accenture.trac.api.config.RootConfig;
import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.eventloop.ExecutionContext;
import com.accenture.trac.common.eventloop.ExecutionRegister;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.service.CommonServiceBase;
import com.accenture.trac.common.storage.StorageManager;
import com.accenture.trac.svc.data.api.TracDataApi;
import com.accenture.trac.svc.data.service.DataReadService;
import com.accenture.trac.svc.data.service.DataWriteService;

import io.grpc.*;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;


public class TracDataService extends CommonServiceBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConfigManager configManager;
    private Server server;

    public TracDataService(ConfigManager config) {
        this.configManager = config;
    }

    @Override
    protected void doStartup(Duration startupTimeout) {

        RootConfig rootConfig;
        DataServiceConfig dataSvcConfig;

        try {
            log.info("Loading TRAC platform config...");

            rootConfig = configManager.loadRootConfig(RootConfig.class);
            dataSvcConfig = rootConfig.getTrac().getServices().getData();

            // TODO: Config validation

            log.info("Config looks ok");
        }
        catch (Exception e) {

            var errorMessage = "There was an error loading the platform config: " + e.getMessage();
            log.error(errorMessage, e);
            throw new EStartup(errorMessage, e);
        }

        try {

            // TODO: Channel type

            var channelType = NioServerSocketChannel.class;
            var clientChannelType = NioSocketChannel.class;
            var bossGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("boss"));
            var workerGroup = new NioEventLoopGroup(6, new DefaultThreadFactory("worker"));
            var execRegister = new ExecutionRegister(workerGroup);

            var storage = new StorageManager();
            storage.initStoragePlugins();
            storage.initStorage(dataSvcConfig.getStorage());

            // TODO: Meta svc config

            var clientChannelBuilder = NettyChannelBuilder.forAddress("localhost", 1234)
                    .channelType(clientChannelType)
                    .eventLoopGroup(workerGroup)
                    .directExecutor()
                    .usePlaintext();

            var baseChannel = clientChannelBuilder.build();

            // TODO: Make event loop client channel interceptor a reusable class
            // Spin up channels on startup, and shut them all down on close

            var clientChannel = ClientInterceptors.intercept(baseChannel, new ClientInterceptor() {

                @Override public <ReqT, RespT>
                ClientCall<ReqT, RespT> interceptCall(
                        MethodDescriptor<ReqT, RespT> method,
                        CallOptions callOptions,
                        Channel baseChannel) {

                    var execCtx = ExecutionContext.EXEC_CONTEXT_KEY.get();
                    var eventLoop = (EventLoop) execCtx.eventLoopExecutor();

                    if (eventLoop == null) {

                        // TODO: Log warnings
                        return baseChannel.newCall(method, callOptions);
                    }

                    var channel = clientChannelBuilder
                            .eventLoopGroup(eventLoop)
                            .build();

                    return channel.newCall(method, callOptions);
                }
            });

            var metaApi = TrustedMetadataApiGrpc.newFutureStub(clientChannel);

            var dataReadSvc = new DataReadService(storage, metaApi);
            var dataWriteSvc = new DataWriteService(dataSvcConfig, storage, metaApi);

            var publicApi = new TracDataApi(dataReadSvc, dataWriteSvc);

            // Create the main server

            this.server = NettyServerBuilder
                    .forPort(dataSvcConfig.getPort())
                    .addService(publicApi)

                    .channelType(channelType)
                    .bossEventLoopGroup(bossGroup)
                    .workerEventLoopGroup(workerGroup)
                    .directExecutor()
                    .intercept(execRegister.registerExecContext())

                    .build();

            // Good to go, let's start!
            server.start();

            server.getPort();
        }
        catch (IOException e) {

            throw new EStartup(e.getMessage(), e);
        }
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) throws InterruptedException {

        server.shutdown();
        server.awaitTermination(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);

        // TODO: Shut down event loops and client channels

        if (server.isTerminated())
            return 0;

        server.shutdownNow();
        return -1;
    }



    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracDataService.class, args);
    }
}
