/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.orch;

import org.finos.tracdap.api.TrustedMetadataApiGrpc;
import org.finos.tracdap.common.auth.GrpcServerAuth;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.grpc.ErrorMappingInterceptor;
import org.finos.tracdap.common.grpc.LoggingClientInterceptor;
import org.finos.tracdap.common.grpc.LoggingServerInterceptor;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.CommonServiceBase;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.ServiceConfig;
import org.finos.tracdap.svc.orch.api.TracOrchestratorApi;
import org.finos.tracdap.svc.orch.cache.IJobCache;
import org.finos.tracdap.svc.orch.cache.local.LocalJobCache;
import org.finos.tracdap.common.exec.IBatchExecutor;
import org.finos.tracdap.svc.orch.service.JobApiService;

import org.finos.tracdap.svc.orch.service.JobLifecycle;
import org.finos.tracdap.svc.orch.service.JobManagementService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
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


public class TracOrchestratorService extends CommonServiceBase {

    private static final int CONCURRENT_REQUESTS = 30;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private EventLoopGroup bossGroup;
    private EventLoopGroup nettyGroup;
    private EventLoopGroup serviceGroup;

    private Server server;
    private ManagedChannel clientChannel;

    private IJobCache jobCache;
    private IBatchExecutor jobExecCtrl;
    private JobManagementService jobMonitor;

    public TracOrchestratorService(PluginManager pluginManager, ConfigManager configManager) {

        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) {

        PlatformConfig platformConfig;
        ServiceConfig orchestratorConfig;

        try {
            log.info("Loading TRAC platform config...");

            platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
            orchestratorConfig = platformConfig.getServices().getOrch();

            // TODO: Config validation

            log.info("Config looks ok");
        }
        catch (Exception e) {

            var errorMessage = "There was a problem loading the platform config: " + e.getMessage();
            log.error(errorMessage, e);
            throw new EStartup(errorMessage, e);
        }

        try {

            var channelType = NioServerSocketChannel.class;
            var clientChannelType = NioSocketChannel.class;

            bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("orch-boss"));
            nettyGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("orch-netty"));
            serviceGroup = new NioEventLoopGroup(CONCURRENT_REQUESTS, new DefaultThreadFactory("orch-svc"));

            prepareMetadataClientChannel(platformConfig, clientChannelType);
            var metaClient = TrustedMetadataApiGrpc.newBlockingStub(clientChannel);

            var jobLifecycleMetaClient = metaClient.withInterceptors(new LoggingClientInterceptor(JobLifecycle.class));
            var jobLifecycle = new JobLifecycle(platformConfig, jobLifecycleMetaClient);

            jobCache = new LocalJobCache();
            // jobCache = InterfaceLogging.wrap(jobCache, IJobCache.class);

            jobExecCtrl = pluginManager.createService(
                    IBatchExecutor.class, configManager,
                    platformConfig.getExecutor());

            jobMonitor = new JobManagementService(
                    jobLifecycle, jobCache,
                    jobExecCtrl, serviceGroup);

            jobMonitor.start();

            var orchestrator = new JobApiService(jobLifecycle, jobCache);
            var orchestratorApi = new TracOrchestratorApi(orchestrator);

            var authentication = GrpcServerAuth.setupAuth(
                    platformConfig.getAuthentication(),
                    platformConfig.getPlatformInfo(),
                    configManager);

            this.server = NettyServerBuilder
                    .forPort(orchestratorConfig.getPort())
                    .intercept(new LoggingServerInterceptor(TracOrchestratorService.class))
                    .intercept(authentication)
                    .intercept(new ErrorMappingInterceptor())
                    .addService(orchestratorApi)

                    .channelType(channelType)
                    .bossEventLoopGroup(bossGroup)
                    .workerEventLoopGroup(nettyGroup)
                    .executor(serviceGroup)
                    .build();

            this.server.start();

            log.info("Orchestrator is listening on port {}", server.getPort());
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

        var clientDown = shutdownResource("Metadata service client", deadline, remaining -> {

            clientChannel.shutdown();
            return clientChannel.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var jobMonitorDown = shutdownResource("Job monitor service", deadline, remaining -> {

            jobMonitor.stop();
            return true;
        });

        var serviceThreadsDown = shutdownResource("Service thread pool", deadline, remaining -> {

            serviceGroup.shutdownGracefully(0, remaining.toMillis(), TimeUnit.MILLISECONDS);
            return serviceGroup.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var nettyDown = shutdownResource("Netty thread pool", deadline, remaining -> {

            nettyGroup.shutdownGracefully(0, remaining.toMillis(), TimeUnit.MILLISECONDS);
            return nettyGroup.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var bossDown = shutdownResource("Boss thread pool", deadline, remaining -> {

            bossGroup.shutdownGracefully(0, remaining.toMillis(), TimeUnit.MILLISECONDS);
            return bossGroup.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        if (serverDown && clientDown && jobMonitorDown &&
            serviceThreadsDown && nettyDown && bossDown)
            return 0;

        if (!serverDown)
            server.shutdownNow();

        if (!clientDown)
            clientChannel.shutdownNow();

        return -1;
    }

    private void prepareMetadataClientChannel(
            PlatformConfig platformConfig,
            Class<? extends io.netty.channel.Channel> channelType) {

        var metaInstances = platformConfig.getInstances().getMetaList();

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
                .eventLoopGroup(nettyGroup)
                .executor(serviceGroup)
                .usePlaintext();

        clientChannel = clientChannelBuilder.build();
    }

    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracOrchestratorService.class, args);
    }
}
