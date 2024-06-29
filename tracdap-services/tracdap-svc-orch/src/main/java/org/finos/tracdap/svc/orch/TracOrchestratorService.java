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
import org.finos.tracdap.common.auth.internal.InternalAuthProvider;
import org.finos.tracdap.common.auth.internal.JwtSetup;
import org.finos.tracdap.common.auth.internal.InternalAuthValidator;
import org.finos.tracdap.common.cache.IJobCache;
import org.finos.tracdap.common.cache.IJobCacheManager;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exec.IBatchExecutor;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.grpc.*;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.CommonServiceBase;
import org.finos.tracdap.common.util.RoutingUtils;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.ServiceConfig;
import org.finos.tracdap.svc.orch.api.TracOrchestratorApi;
import org.finos.tracdap.svc.orch.service.JobManager;
import org.finos.tracdap.svc.orch.service.JobProcessor;
import org.finos.tracdap.svc.orch.service.JobProcessorHelpers;
import org.finos.tracdap.svc.orch.service.JobState;

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

    private static final String JOB_CACHE_NAME = "TRAC_JOB_STATE";
    private static final int CONCURRENT_REQUESTS = 30;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private EventLoopGroup bossGroup;
    private EventLoopGroup nettyGroup;
    private EventLoopGroup serviceGroup;

    private Server server;
    private ManagedChannel clientChannel;

    private IBatchExecutor<?> jobExecutor;
    private IJobCache<JobState> jobCache;
    private JobManager jobManager;

    public TracOrchestratorService(PluginManager pluginManager, ConfigManager configManager) {

        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) {

        PlatformConfig platformConfig;
        ServiceConfig serviceConfig;

        try {
            log.info("Loading TRAC platform config...");

            platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
            serviceConfig = platformConfig.getServicesOrThrow(ConfigKeys.ORCHESTRATOR_SERVICE_KEY);

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

            var metaClient = prepareMetadataClient(platformConfig, clientChannelType);

            var jwtProcessor = JwtSetup.createProcessor(platformConfig, configManager);
            var internalAuth = new InternalAuthProvider(jwtProcessor, platformConfig.getAuthentication());

            jobExecutor = pluginManager.createService(
                    IBatchExecutor.class,
                    platformConfig.getExecutor(),
                    configManager);

            var cacheManager = pluginManager.createService(
                    IJobCacheManager.class,
                    platformConfig.getJobCache(),
                    configManager);

            jobCache = cacheManager.getCache(JOB_CACHE_NAME, JobState.class);

            var jobProcessorHelpers = new JobProcessorHelpers(platformConfig, metaClient);
            var jobProcessor = new JobProcessor(metaClient, internalAuth, jobExecutor, jobProcessorHelpers);

            jobManager = new JobManager(platformConfig, jobProcessor, jobCache, serviceGroup);

            jobExecutor.start();
            jobManager.start();

            this.server = NettyServerBuilder
                    .forPort(serviceConfig.getPort())

                    // Netty config
                    .channelType(channelType)
                    .bossEventLoopGroup(bossGroup)
                    .workerEventLoopGroup(nettyGroup)
                    .executor(serviceGroup)

                    // Interceptor order: Last added is executed first
                    // But note, on close it is the other way round, because the stack is unwinding
                    // We want error mapping at the bottom of the stack, so it unwinds before logging

                    .intercept(new ErrorMappingInterceptor())
                    .intercept(new LoggingServerInterceptor(TracOrchestratorService.class))
                    .intercept(new CompressionServerInterceptor())
                    .intercept(new InternalAuthValidator(platformConfig.getAuthentication(), jwtProcessor))

                    // The main service
                    .addService(new TracOrchestratorApi(jobManager, jobProcessor))
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

        var executorDown = shutdownResource("Executor service", deadline, remainingTime -> {

            jobExecutor.stop();
            return true;
        });

        var jobMonitorDown = shutdownResource("Job monitor service", deadline, remaining -> {

            jobManager.stop();
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

        if (serverDown && clientDown && executorDown && jobMonitorDown &&
            serviceThreadsDown && nettyDown && bossDown)
            return 0;

        if (!serverDown)
            server.shutdownNow();

        if (!clientDown)
            clientChannel.shutdownNow();

        return -1;
    }

    private TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub prepareMetadataClient(
            PlatformConfig platformConfig,
            Class<? extends io.netty.channel.Channel> channelType) {

        var metadataTarget = RoutingUtils.serviceTarget(platformConfig, ConfigKeys.METADATA_SERVICE_KEY);

        log.info("Using metadata service at [{}:{}]",
                metadataTarget.getHost(), metadataTarget.getPort());

        var clientChannelBuilder = NettyChannelBuilder
                .forAddress(metadataTarget.getHost(), metadataTarget.getPort())
                .channelType(channelType)
                .eventLoopGroup(nettyGroup)
                .executor(serviceGroup)
                .usePlaintext();

        clientChannel = clientChannelBuilder.build();

        return TrustedMetadataApiGrpc
                .newBlockingStub(clientChannel)
                .withCompression(CompressionClientInterceptor.COMPRESSION_TYPE)
                .withInterceptors(new CompressionClientInterceptor())
                .withInterceptors(new LoggingClientInterceptor(TracOrchestratorService.class));
    }

    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracOrchestratorService.class, args);
    }
}
