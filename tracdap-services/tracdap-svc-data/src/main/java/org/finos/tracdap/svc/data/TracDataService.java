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

package org.finos.tracdap.svc.data;

import org.finos.tracdap.api.ConfigListRequest;
import org.finos.tracdap.api.DataServiceProto;
import org.finos.tracdap.api.MetadataBatchRequest;
import org.finos.tracdap.api.internal.InternalMessagingProto;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.netty.*;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.codec.CodecManager;
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.EStorageConfig;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.TracServiceConfig;
import org.finos.tracdap.common.service.TracServiceBase;
import org.finos.tracdap.common.storage.IStorageManager;
import org.finos.tracdap.common.storage.StorageManager;
import org.finos.tracdap.common.util.RoutingUtils;
import org.finos.tracdap.common.validation.ValidationConcern;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.ServiceConfig;
import org.finos.tracdap.config.StorageConfig;
import org.finos.tracdap.metadata.ResourceType;
import org.finos.tracdap.svc.data.api.MessageProcessor;
import org.finos.tracdap.svc.data.api.TracDataApi;
import org.finos.tracdap.svc.data.service.DataService;
import org.finos.tracdap.svc.data.service.FileService;

import io.grpc.*;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.netty.NettyAllocationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class TracDataService extends TracServiceBase {

    private static final int MAX_SERVICE_CORES = 12;
    private static final int MIN_SERVICE_CORES = 2;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private EventLoopGroup bossGroup;
    private EventLoopGroup serviceGroup;
    private ExecutorService offloadExecutor;
    private GrpcConcern commonConcerns;
    private ManagedChannel metaClientChanel;
    private ManagedChannel metaBlockingChanel;
    private StorageManager storage;
    private Server server;

    public static void main(String[] args) {

        TracServiceBase.svcMain(TracDataService.class, args);
    }

    public TracDataService(PluginManager plugins, ConfigManager config) {
        this.pluginManager = plugins;
        this.configManager = config;
    }

    @Override
    protected void doStartup(Duration startupTimeout) {

        PlatformConfig platformConfig;
        ServiceConfig serviceConfig;
        StorageConfig storageConfig;

        try {
            log.info("Loading TRAC platform config...");

            platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
            serviceConfig = platformConfig.getServicesOrThrow(ConfigKeys.DATA_SERVICE_KEY);
            storageConfig = platformConfig.getStorage();

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

            // In an ideal setup, all processing is async on the EL with streaming data chunks
            // So we want 1 EL per core with 1 core free for OS / other tasks
            // Minimum of 2 ELs in the case of a single-core or dual-core host
            // Some storage plugins may not allow this pattern, in which case worker threads may be needed
            // This setting should probably be a default, with the option override in config

            var offloadTracking = new EventLoopOffloadTracker();

            var bossThreadCount = 1;
            var bossExecutor = NettyHelpers.eventLoopExecutor("data-boss");
            var bossScheduler = EventLoopScheduler.roundRobin();

            bossGroup = NettyHelpers.nioEventLoopGroup(bossExecutor, bossScheduler, bossThreadCount);

            var serviceCoresAvailable= Runtime.getRuntime().availableProcessors() - 1;
            var serviceThreadCount = Math.max(Math.min(serviceCoresAvailable, MAX_SERVICE_CORES), MIN_SERVICE_CORES);
            var serviceExecutor = NettyHelpers.eventLoopExecutor("data-svc");
            var serviceScheduler = EventLoopScheduler.preferLoopAffinity(offloadTracking);

            serviceGroup = NettyHelpers.nioEventLoopGroup(serviceExecutor, serviceScheduler, serviceThreadCount);

            var baseOffloadExecutor = NettyHelpers.threadPoolExecutor("data-offload");
            offloadExecutor = offloadTracking.wrappExecutorService(baseOffloadExecutor);

            var eventLoopResolver = new EventLoopResolver(serviceGroup, offloadTracking);

            // Common framework for cross-cutting concerns
            commonConcerns = buildCommonConcerns();

            metaClientChanel = prepareMetadataClientChannel(platformConfig, clientChannelType);
            var metaClient = prepareMetadataClient(eventLoopResolver, commonConcerns);

            metaBlockingChanel = prepareBlockingClientChannel(platformConfig, clientChannelType);
            var metaClientBlocking = prepareMetadataClientBlocking(commonConcerns, metaBlockingChanel);

            // TODO: Review arrow allocator config, for root and child allocators

            var arrowAllocatorConfig = RootAllocator
                    .configBuilder()
                    .allocationManagerFactory(NettyAllocationManager.FACTORY)
                    .build();

            var arrowAllocator = new RootAllocator(arrowAllocatorConfig);

            // Force initialization of Arrow memory subsystem, rather than waiting until the first API call
            // This can fail on Java versions >= 16 if the java.nio module is not marked as open

            try (var buffer = arrowAllocator.buffer(4096)) {
                buffer.writeLong(1L);
            }
            catch (RuntimeException e) {

                log.error("Failed to set up native memory access for Apache Arrow", e);
                throw new EStartup("Failed to set up native memory access for Apache Arrow", e);
            }

            var formats = new CodecManager(pluginManager, configManager);
            storage = new StorageManager(pluginManager, configManager, formats, serviceGroup);

            var tenantConfig = platformConfig.getTenantsMap();

            for (var tenant : tenantConfig.keySet())
                loadStorageResources(metaClientBlocking, tenant);

            // TODO: Config checks need to happen after dynamic resources are added for a tenant
            // Check default storage and format are available
            // checkDefaultStorageAndFormat(storage, formats, storageConfig);

            var fileSvc = new FileService(storageConfig, tenantConfig, storage, metaClient);
            var dataSvc = new DataService(storageConfig, tenantConfig, storage, formats, metaClient);
            var dataApi = new TracDataApi(dataSvc, fileSvc, eventLoopResolver, arrowAllocator, commonConcerns);

            var messageProcessor = new MessageProcessor(
                    metaClientBlocking, offloadExecutor, commonConcerns,
                    storage, configManager.getSecrets());

            var serverBuilder = NettyServerBuilder
                    .forPort(serviceConfig.getPort())

                    // Netty setup
                    .channelType(channelType)
                    .bossEventLoopGroup(bossGroup)
                    .workerEventLoopGroup(serviceGroup)
                    .directExecutor()

                    // Services
                    .addService(dataApi)
                    .addService(messageProcessor);

            // Apply common concerns
            this.server =  commonConcerns
                    .configureServer(serverBuilder)
                    .build();

            // Good to go, let's start!
            this.server.start();

            log.info("Data service is listening on port {}", server.getPort());
        }
        catch (IOException e) {

            throw new EStartup(e.getMessage(), e);
        }
    }

    private GrpcConcern buildCommonConcerns() {

        var commonConcerns = TracServiceConfig.coreConcerns(TracDataService.class);

        // Validation concern for the APIs being served
        var validationConcern = new ValidationConcern(
                DataServiceProto.getDescriptor(),
                InternalMessagingProto.getDescriptor());
        
        commonConcerns = commonConcerns.addAfter(TracServiceConfig.TRAC_PROTOCOL, validationConcern);

        // Additional cross-cutting concerns configured by extensions
        for (var extension : pluginManager.getExtensions()) {
            commonConcerns = extension.addServiceConcerns(commonConcerns, configManager, ConfigKeys.DATA_SERVICE_KEY);
        }

        return commonConcerns.build();
    }

    private ManagedChannel
    prepareMetadataClientChannel(PlatformConfig platformConfig, Class<? extends io.netty.channel.Channel> channelType) {

        var metadataTarget = RoutingUtils.serviceTarget(platformConfig, ConfigKeys.METADATA_SERVICE_KEY);

        log.info("Using metadata service at [{}:{}]",
                metadataTarget.getHost(), metadataTarget.getPort());

        return NettyChannelBuilder
                .forAddress(metadataTarget.getHost(), metadataTarget.getPort())
                .channelType(channelType)
                .eventLoopGroup(serviceGroup)
                .directExecutor()
                .offloadExecutor(offloadExecutor)
                .usePlaintext()
                .build();
    }

    private ManagedChannel
    prepareBlockingClientChannel(PlatformConfig platformConfig, Class<? extends io.netty.channel.Channel> channelType) {

        var metadataTarget = RoutingUtils.serviceTarget(platformConfig, ConfigKeys.METADATA_SERVICE_KEY);

        log.info("Using (blocking) metadata service at [{}:{}]",
                metadataTarget.getHost(), metadataTarget.getPort());

        return NettyChannelBuilder
                .forAddress(metadataTarget.getHost(), metadataTarget.getPort())
                .channelType(channelType)
                .eventLoopGroup(serviceGroup)
                .executor(offloadExecutor)
                .usePlaintext()
                .build();
    }


    private InternalMetadataApiGrpc.InternalMetadataApiFutureStub
    prepareMetadataClient(EventLoopResolver eventLoopResolver, GrpcConcern commonConcerns) {

        var client = InternalMetadataApiGrpc.newFutureStub(metaClientChanel)
                .withInterceptors(new EventLoopInterceptor(eventLoopResolver));

        return commonConcerns.configureClient(client);
    }

    private InternalMetadataApiGrpc.InternalMetadataApiBlockingStub
    prepareMetadataClientBlocking(GrpcConcern commonConcerns, ManagedChannel separateChannel) {

        var client = InternalMetadataApiGrpc.newBlockingStub(separateChannel);
        return commonConcerns.configureClient(client);
    }

    private void loadStorageResources(
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataClient, String tenant) {

        var configList = ConfigListRequest.newBuilder()
                .setTenant(tenant)
                .setConfigClass(ConfigKeys.TRAC_RESOURCES)
                .setResourceType(ResourceType.INTERNAL_STORAGE)
                .build();

        var clientState = commonConcerns.prepareClientCall(Context.ROOT);
        var client = clientState.configureClient(metadataClient);


        var listing = client.listConfigEntries(configList);

        var selectors = listing.getEntriesList().stream()
                .map(entry -> entry.getDetails().getObjectSelector())
                .collect(Collectors.toList());

        var batchRequest = MetadataBatchRequest.newBuilder()
                .setTenant(tenant)
                .addAllSelector(selectors)
                .build();
        var resourceObjects = client.readBatch(batchRequest);

        for (var resourceObject : resourceObjects.getTagList()) {

            var resourceKeyAttr = resourceObject.getAttrsOrThrow(MetadataConstants.TRAC_CONFIG_KEY);
            var resourceKey = MetadataCodec.decodeStringValue(resourceKeyAttr);
            var resourceDef = resourceObject.getDefinition().getResource();

            storage.addStorage(resourceKey, resourceDef);
        }
    }

    private void checkDefaultStorageAndFormat(IStorageManager storage, ICodecManager formats, StorageConfig config) {

        try {

            storage.getFileStorage(config.getDefaultBucket());
            storage.getDataStorage(config.getDefaultBucket());
            formats.getCodec(config.getDefaultFormat());
        }
        catch (EStorageConfig e) {

            var msg = String.format("Storage not configured for default storage key: [%s]", config.getDefaultBucket());
            log.error(msg);
            throw new EStartup(msg, e);
        }
        catch (EPluginNotAvailable e) {

            var msg = String.format("Codec not available for default storage format: [%s]", config.getDefaultFormat());
            log.error(msg);
            throw new EStartup(msg, e);
        }
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) {

        var deadline = Instant.now().plus(shutdownTimeout);

        var serverDown = shutdownResource("Data service server", deadline, remaining -> {

            server.shutdown();
            return server.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var clientDown = shutdownResource("Metadata client", deadline, remaining -> {

            metaClientChanel.shutdown();
            return metaClientChanel.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var blockingClientDown = shutdownResource("Metadata client (blocking)", deadline, remaining -> {

            metaBlockingChanel.shutdown();
            return metaBlockingChanel.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var storageDown = shutdownResource("Storage service", deadline, remaining -> {

            storage.close();
            return true;
        });

        var workersDown = shutdownResource("Service thread pool", deadline, remaining -> {

            serviceGroup.shutdownGracefully(0, remaining.toMillis(), TimeUnit.MILLISECONDS);
            return serviceGroup.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var offloadDown = shutdownResource("Offload thread pool", deadline, remaining -> {

            offloadExecutor.shutdown();
            return offloadExecutor.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var bossDown = shutdownResource("Boss thread pool", deadline, remaining -> {

            bossGroup.shutdownGracefully(0, remaining.toMillis(), TimeUnit.MILLISECONDS);
            return bossGroup.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        if (serverDown && clientDown && blockingClientDown && storageDown && offloadDown &&  workersDown && bossDown)
            return 0;

        if (!serverDown)
            server.shutdownNow();

        return -1;
    }
}
