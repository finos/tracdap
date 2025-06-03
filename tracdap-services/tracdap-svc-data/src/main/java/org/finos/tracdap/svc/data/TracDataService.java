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
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.ServiceConfig;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.config.TenantConfigMap;
import org.finos.tracdap.metadata.ConfigDetails;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.ResourceType;
import org.finos.tracdap.common.config.ConfigHelpers;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.netty.*;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.codec.CodecManager;
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.TracServiceConfig;
import org.finos.tracdap.common.service.TracServiceBase;
import org.finos.tracdap.common.storage.StorageManager;
import org.finos.tracdap.common.util.RoutingUtils;
import org.finos.tracdap.common.validation.ValidationConcern;
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
import org.finos.tracdap.svc.data.service.TenantServices;
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
    private InternalMetadataApiGrpc.InternalMetadataApiFutureStub metaClient;
    private InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClientBlocking;
    private TenantServices.Map services;
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
        TenantConfigMap tenantConfigMap;

        try {
            log.info("Loading TRAC platform config...");

            platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
            serviceConfig = platformConfig.getServicesOrThrow(ConfigKeys.DATA_SERVICE_KEY);

            var tenantConfigUrl = ConfigHelpers.readString("tenant config file", platformConfig.getConfigMap(), ConfigKeys.TENANTS_CONFIG_KEY);
            tenantConfigMap = configManager.loadConfigObject(tenantConfigUrl, TenantConfigMap.class);

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
            metaClient = prepareMetadataClient(eventLoopResolver, commonConcerns);

            metaBlockingChanel = prepareBlockingClientChannel(platformConfig, clientChannelType);
            metaClientBlocking = prepareMetadataClientBlocking(commonConcerns, metaBlockingChanel);

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
            services = buildTenantServiceMap(tenantConfigMap, formats);

            var dataApi = new TracDataApi(services, eventLoopResolver, arrowAllocator, commonConcerns);
            var messageProcessor = new MessageProcessor(services, metaClientBlocking, commonConcerns, offloadExecutor);

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


    private TenantServices.Map buildTenantServiceMap(TenantConfigMap tenantConfigMap, ICodecManager formats) {

        var services = TenantServices.create();

        for (var tenantConfigEntry : tenantConfigMap.getTenantsMap().entrySet()) {

            var tenantCode = tenantConfigEntry.getKey();
            var tenantConfig = tenantConfigEntry.getValue();
            var tenantServices = buildTenantServices(tenantCode, tenantConfig, formats);

            if (!services.addTenant(tenantCode, tenantServices)) {
                log.warn("Failed to add tenant services for [{}]", tenantCode);
                doShutdownTenant(tenantCode, tenantServices);
            }
        }

        return services;
    }

    private TenantServices buildTenantServices(String tenantCode, TenantConfig tenantConfig, ICodecManager formats) {

        log.info("Prepare tenant services: [{}]", tenantCode);

        var secrets = configManager.getSecrets()
                .scope(ConfigKeys.TENANT_SCOPE)
                .scope(tenantCode);

        var storageManager = buildTenantStorage(tenantCode, tenantConfig, formats);

        var dataService = new DataService(storageManager, formats, metaClient);
        var fileService = new FileService(storageManager, metaClient);

        return new TenantServices(
                tenantConfig,
                dataService, fileService,
                storageManager, secrets);
    }

    private StorageManager buildTenantStorage(String tenant, TenantConfig tenantConfig, ICodecManager formats) {

        var storageManager = new StorageManager(pluginManager, configManager, formats, serviceGroup);

        // Set default properties from tenant-level config
        storageManager.updateStorageDefaults(tenantConfig);

        // Add storage resources defined in the tenants config file
        for (var resourceEntry : tenantConfig.getResourcesMap().entrySet()) {
            if (resourceEntry.getValue().getResourceType() == ResourceType.INTERNAL_STORAGE) {
                storageManager.addStorage(resourceEntry.getKey(), resourceEntry.getValue());
            }
        }

        var configList = ConfigListRequest.newBuilder()
                .setTenant(tenant)
                .setConfigClass(ConfigKeys.TRAC_RESOURCES)
                .setResourceType(ResourceType.INTERNAL_STORAGE)
                .build();

        var clientState = commonConcerns.prepareClientCall(Context.ROOT);
        var client = clientState.configureClient(metaClientBlocking);

        var listing = client.listConfigEntries(configList);

        var duplicateEntries = listing.getEntriesList().stream()
                .map(ConfigEntry::getConfigKey)
                .filter(tenantConfig::containsResources)
                .collect(Collectors.toList());

        if (!duplicateEntries.isEmpty()) {
            log.warn("Dynamic config ignored for statically defined resources: {}", String.join(", ", duplicateEntries));
        }

        var filteredEntries = listing.getEntriesList().stream()
                .filter(entry -> !tenantConfig.containsResources( entry.getConfigKey()))
                .collect(Collectors.toList());

        // Do not make an API call if there are no dynamic resources
        if (!filteredEntries.isEmpty()) {

            var filteredIds = filteredEntries.stream()
                    .map(ConfigEntry::getDetails)
                    .map(ConfigDetails::getObjectSelector)
                    .collect(Collectors.toList());

            var batchRequest = MetadataBatchRequest.newBuilder()
                    .setTenant(tenant)
                    .addAllSelector(filteredIds)
                    .build();

            var resourceObjects = client.readBatch(batchRequest);

            for (int i = 0; i < filteredIds.size(); i++) {

                var resourceKey = filteredEntries.get(i).getConfigKey();
                var resourceDef = resourceObjects.getTag(i).getDefinition().getResource();

                storageManager.addStorage(resourceKey, resourceDef);
            }
        }

        return storageManager;
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) {

        var deadline = Instant.now().plus(shutdownTimeout);

        var serverDown = shutdownResource("Data service server", deadline, remaining -> {

            server.shutdown();
            return server.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var storageDown = shutdownResource("tenant services", deadline, remaining -> {

            services.closeAllTenants(this::doShutdownTenant);
            return true;
        });

        var clientDown = shutdownResource("Metadata client", deadline, remaining -> {

            metaClientChanel.shutdown();
            return metaClientChanel.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var blockingClientDown = shutdownResource("Metadata client (blocking)", deadline, remaining -> {

            metaBlockingChanel.shutdown();
            return metaBlockingChanel.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
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

    private void doShutdownTenant(String tenantCode, TenantServices tenantServices) {

        log.info("Shut down tenant services: [{}]", tenantCode);

        tenantServices.getStorageManager().close();
    }
}
