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
import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.eventloop.ExecutionRegister;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.service.CommonServiceBase;
import com.accenture.trac.common.storage.StorageManager;
import com.accenture.trac.svc.data.api.TracDataApi;
import com.accenture.trac.svc.data.service.DataReadService;
import com.accenture.trac.svc.data.service.DataWriteService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class TracDataService extends CommonServiceBase {

    private static final short DATA_SERVICE_PORT = 8082;

    private static final int WORKER_POOL_SIZE = 6;
    private static final int OVERFLOW_SIZE = 10;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConfigManager configManager;
    private Server server;

    public TracDataService(ConfigManager config) {
        this.configManager = config;
    }

    @Override
    protected void doStartup(Duration startupTimeout) {

        try {

            var channelType = NioServerSocketChannel.class;
            var bossGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("boss"));
            var workerGroup = new NioEventLoopGroup(6, new DefaultThreadFactory("worker"));
            var execRegister = new ExecutionRegister(workerGroup);

            var storage = new StorageManager();
            storage.initStoragePlugins();

            var metaApi = (TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub) null;

            var dataReadSvc = new DataReadService(storage, metaApi);
            var dataWriteSvc = new DataWriteService(storage, metaApi);

            var publicApi = new TracDataApi(dataReadSvc, dataWriteSvc);

            // Create the main server

            this.server = NettyServerBuilder
                    .forPort(DATA_SERVICE_PORT)
                    .addService(publicApi)

                    .channelType(channelType)
                    .bossEventLoopGroup(bossGroup)
                    .workerEventLoopGroup(workerGroup)
                    .directExecutor()
                    .intercept(execRegister.registerExecContext())

                    .build();

            // Good to go, let's start!
            server.start();
        }
        catch (IOException e) {

            throw new EStartup(e.getMessage(), e);
        }
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) throws InterruptedException {

        server.shutdown();
        server.awaitTermination(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);

        if (server.isTerminated())
            return 0;

        server.shutdownNow();
        return -1;
    }



    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracDataService.class, args);
    }
}
