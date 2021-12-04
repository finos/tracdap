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

package com.accenture.trac.svc.orch;import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.plugin.PluginManager;
import com.accenture.trac.common.service.CommonServiceBase;
import com.accenture.trac.svc.orch.api.TracOrchestratorApi;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class TracOrchestratorService extends CommonServiceBase {

    private static final short DEFAULT_PORT = 8083;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Server server;

    public TracOrchestratorService(PluginManager pluginManager, ConfigManager configManager) {

        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) {

        try {

            var channelType = NioServerSocketChannel.class;
            var workerThreads = Runtime.getRuntime().availableProcessors() * 2;
            workerGroup = new NioEventLoopGroup(workerThreads, new DefaultThreadFactory("orch-svc"));
            bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("orch-boss"));

            this.server = NettyServerBuilder.forPort(DEFAULT_PORT)
                    .addService(new TracOrchestratorApi())
                    .channelType(channelType)
                    .bossEventLoopGroup(bossGroup)
                    .workerEventLoopGroup(workerGroup)
                    .build();

            this.server.start();
        }
        catch (IOException e) {

            throw new EStartup(e.getMessage(), e);
        }
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) {

        var deadline = Instant.now().plus(shutdownTimeout);

        var serverDown = shutdownResource("Orchestrator service server", deadline, remaining -> {

            server.shutdown();
            return server.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var workersDown = shutdownResource("Worker thread pool", deadline, remaining -> {

            workerGroup.shutdownGracefully(0, remaining.toMillis(), TimeUnit.MILLISECONDS);
            return workerGroup.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var bossDown = shutdownResource("Boss thread pool", deadline, remaining -> {

            bossGroup.shutdownGracefully(0, remaining.toMillis(), TimeUnit.MILLISECONDS);
            return bossGroup.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        if (serverDown &&  workersDown && bossDown)
            return 0;

        if (!serverDown)
            server.shutdownNow();

        return -1;
    }

    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracOrchestratorService.class, args);
    }
}
