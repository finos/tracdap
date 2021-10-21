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
import com.accenture.trac.api.config.TracConfig;
import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.concurrent.ExecutionRegister;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.service.CommonServiceBase;
import com.accenture.trac.common.storage.StorageManager;
import com.accenture.trac.svc.data.api.TracDataApi;
import com.accenture.trac.svc.data.service.DataReadService;
import com.accenture.trac.svc.data.service.DataWriteService;

import io.grpc.*;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;


public class TracDataService extends CommonServiceBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConfigManager configManager;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ManagedChannel clientChannel;
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

            var channelType = NioServerSocketChannel.class;
            var clientChannelType = NioSocketChannel.class;

            var workerThreads = Runtime.getRuntime().availableProcessors() * 2;
            workerGroup = new NioEventLoopGroup(workerThreads, new DefaultThreadFactory("data-svc"));
            bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("data-boss"));

            var execRegister = new ExecutionRegister(workerGroup);

            var storage = new StorageManager();
            storage.initStoragePlugins();
            storage.initStorage(dataSvcConfig.getStorage());

            var metaClient = prepareMetadataClient(rootConfig.getTrac(), clientChannelType);

            var dataReadSvc = new DataReadService(storage, metaClient);
            var dataWriteSvc = new DataWriteService(dataSvcConfig, storage, metaClient);
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

            log.info("Data service is listening on port {}", server.getPort());
        }
        catch (IOException e) {

            throw new EStartup(e.getMessage(), e);
        }
    }

    private TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub
    prepareMetadataClient(
            TracConfig tracConfig,
            Class<? extends io.netty.channel.Channel> channelType) {

        var metaInstances = tracConfig.getInstances().getMeta();

        if (metaInstances.isEmpty()) {

            var err = "Configuration contains no instances of the metadata service";
            log.error(err);
            throw new EStartup(err);
        }

        var metaInstance = metaInstances.get(0);  // Just use the first instance for now

        log.info("Using metadata service instance at [{}:{}]",
                metaInstance.getHost(), metaInstance.getPort());

        var clientChannelBuilder = NettyChannelBuilder
                .forAddress(metaInstance.getHost(), metaInstance.getPort())
                .channelType(channelType)
                .eventLoopGroup(workerGroup)
                .directExecutor()
                .usePlaintext();

        clientChannel = EventLoopChannel.wrapChannel(clientChannelBuilder, workerGroup);

        return TrustedMetadataApiGrpc.newFutureStub(clientChannel);
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) {

        var deadline = Instant.now().plus(shutdownTimeout);

        var serverDown = shutdownResource("Data service server", deadline, remaining -> {

            server.shutdown();
            return server.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var clientDown = shutdownResource("Metadata service client", deadline, remaining -> {

            clientChannel.shutdown();
            return clientChannel.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var workersDown = shutdownResource("Worker thread pool", deadline, remaining -> {

            workerGroup.shutdownGracefully(0, remaining.toMillis(), TimeUnit.MILLISECONDS);
            return workerGroup.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var bossDown = shutdownResource("Boss thread pool", deadline, remaining -> {

            bossGroup.shutdownGracefully(0, remaining.toMillis(), TimeUnit.MILLISECONDS);
            return bossGroup.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        if (serverDown && clientDown && workersDown && bossDown)
            return 0;

        if (!serverDown)
            server.shutdownNow();

        if (!clientDown)
            clientChannel.shutdownNow();

        return -1;
    }

    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracDataService.class, args);
    }
}
