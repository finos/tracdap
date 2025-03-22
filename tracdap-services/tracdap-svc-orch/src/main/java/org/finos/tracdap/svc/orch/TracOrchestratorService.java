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

package org.finos.tracdap.svc.orch;

import org.finos.tracdap.api.ConfigListRequest;
import org.finos.tracdap.api.MetadataBatchRequest;
import org.finos.tracdap.api.OrchestratorServiceProto;
import org.finos.tracdap.api.internal.InternalMessagingProto;
import org.finos.tracdap.api.internal.TrustedMetadataApiGrpc;
import org.finos.tracdap.common.cache.IJobCacheManager;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.config.DynamicConfig;
import org.finos.tracdap.common.exec.BatchJobExecutor;
import org.finos.tracdap.common.exec.IBatchExecutor;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exec.IJobExecutor;
import org.finos.tracdap.common.grpc.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.TracServiceConfig;
import org.finos.tracdap.common.service.TracServiceBase;
import org.finos.tracdap.common.util.RoutingUtils;
import org.finos.tracdap.common.validation.ValidationConcern;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.ServiceConfig;
import org.finos.tracdap.metadata.ResourceDefinition;
import org.finos.tracdap.svc.orch.api.MessageProcessor;
import org.finos.tracdap.svc.orch.api.TracOrchestratorApi;
import org.finos.tracdap.svc.orch.service.JobManager;
import org.finos.tracdap.svc.orch.service.JobProcessor;
import org.finos.tracdap.svc.orch.service.JobState;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class TracOrchestratorService extends TracServiceBase {

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

    private IJobExecutor<?> jobExecutor;
    private IJobCacheManager jobCacheManager;
    private JobManager jobManager;

    public static void main(String[] args) {

        TracServiceBase.svcMain(TracOrchestratorService.class, args);
    }

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

            // TODO: Setup of the server / channels / ELs needs review
            // Orch svc puts application work on an executor, low level setup is probably not required
            // The main worker executor could be e.q. a scheduled thread pool
            // E.g. meta svc uses high level ServerBuilder with just one worker pool
            // Orch framework still needs a channel factory to connect to instances of TRAC runtime

            var channelType = NioServerSocketChannel.class;
            var clientChannelType = NioSocketChannel.class;
            var clientChannelFactory = new ClientChannelFactory(clientChannelType);

            bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("orch-boss"));
            nettyGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("orch-netty"));
            serviceGroup = new NioEventLoopGroup(CONCURRENT_REQUESTS, new DefaultThreadFactory("orch-svc"));

            // Common framework for cross-cutting concerns
            var commonConcerns = buildCommonConcerns();

            var metaClient = prepareMetadataClient(platformConfig, clientChannelFactory, commonConcerns);

            var resources = new DynamicConfig.Resources();

            for (var tenant : platformConfig.getTenantsMap().keySet())
                loadResources(metaClient, tenant, resources);

            var batchExecutor = (IBatchExecutor<? extends Serializable>) pluginManager.createService(
                    IBatchExecutor.class,
                    platformConfig.getExecutor(),
                    configManager);

            jobExecutor = new BatchJobExecutor<>(batchExecutor);

            jobCacheManager = pluginManager.createService(
                    IJobCacheManager.class,
                    platformConfig.getJobCache(),
                    configManager);

            var jobCache = jobCacheManager.getCache(JOB_CACHE_NAME, JobState.class);

            var jobProcessor = new JobProcessor(platformConfig, resources, metaClient, commonConcerns, jobExecutor, configManager);

            jobManager = new JobManager(platformConfig, jobProcessor, jobCache, serviceGroup);

            jobExecutor.start(clientChannelFactory);
            jobManager.start();

            var serverBuilder = NettyServerBuilder
                    .forPort(serviceConfig.getPort())

                    // Netty config
                    .channelType(channelType)
                    .bossEventLoopGroup(bossGroup)
                    .workerEventLoopGroup(nettyGroup)
                    .executor(serviceGroup)

                    // The main service
                    .addService(new TracOrchestratorApi(jobManager, jobProcessor, commonConcerns))
                    .addService(new MessageProcessor(resources, metaClient));

            // Apply common concerns
            this.server = commonConcerns
                    .configureServer(serverBuilder)
                    .build();

            // Good to go, let's start!
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

    private GrpcConcern buildCommonConcerns() {

        var commonConcerns = TracServiceConfig.coreConcerns(TracOrchestratorService.class);

        // Validation concern for the APIs being served
        var validationConcern = new ValidationConcern(
                OrchestratorServiceProto.getDescriptor(),
                InternalMessagingProto.getDescriptor());

        commonConcerns = commonConcerns.addAfter(TracServiceConfig.TRAC_PROTOCOL, validationConcern);

        // Additional cross-cutting concerns configured by extensions
        for (var extension : pluginManager.getExtensions()) {
            commonConcerns = extension.addServiceConcerns(commonConcerns, configManager, ConfigKeys.ORCHESTRATOR_SERVICE_KEY);
        }

        return commonConcerns.build();
    }

    private TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub prepareMetadataClient(
            PlatformConfig platformConfig, GrpcChannelFactory channelFactory,
            GrpcConcern commonConcerns) {

        var metadataTarget = RoutingUtils.serviceTarget(platformConfig, ConfigKeys.METADATA_SERVICE_KEY);

        log.info("Using metadata service at [{}:{}]",
                metadataTarget.getHost(), metadataTarget.getPort());

        clientChannel = channelFactory.createChannel(metadataTarget.getHost(), metadataTarget.getPort());

        var metadataClient = TrustedMetadataApiGrpc.newBlockingStub(clientChannel);

        return commonConcerns.configureClient(metadataClient);
    }

    private class ClientChannelFactory implements GrpcChannelFactory {

        private final Class<? extends io.netty.channel.Channel> channelType;

        public ClientChannelFactory(Class<? extends Channel> channelType) {
            this.channelType = channelType;
        }

        @Override
        public ManagedChannel createChannel(String host, int port) {

            var clientChannelBuilder = NettyChannelBuilder
                    .forAddress(host, port)
                    .channelType(channelType)
                    .eventLoopGroup(nettyGroup)
                    .executor(serviceGroup)
                    .usePlaintext();

            return clientChannelBuilder.build();
        }
    }

    void loadResources(
            TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub metaClient,
            String tenant, DynamicConfig<ResourceDefinition> resources) {

        var configList = ConfigListRequest.newBuilder()
                .setTenant(tenant)
                .setConfigClass(ConfigKeys.TRAC_RESOURCES)
                .build();

        var listing = metaClient.listConfigEntries(configList);

        var selectors = listing.getEntriesList().stream()
                .map(entry -> entry.getDetails().getObjectSelector())
                .collect(Collectors.toList());

        var batchRequest = MetadataBatchRequest.newBuilder()
                .setTenant(tenant)
                .addAllSelector(selectors)
                .build();

        var resourceObjects = metaClient.readBatch(batchRequest);

        for (var resource : resourceObjects.getTagList()) {

            var resourceKey = resource.getAttrsOrThrow(MetadataConstants.TRAC_CONFIG_KEY);
            var resourceDef = resource.getDefinition().getResource();

            resources.addEntry(MetadataCodec.decodeStringValue(resourceKey), resourceDef);
        }
    }
}
