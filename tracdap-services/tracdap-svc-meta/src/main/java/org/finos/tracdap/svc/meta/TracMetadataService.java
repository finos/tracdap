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

package org.finos.tracdap.svc.meta;

import org.finos.tracdap.api.MetadataServiceProto;
import org.finos.tracdap.api.internal.InternalMessagingProto;
import org.finos.tracdap.api.internal.InternalMetadataProto;
import org.finos.tracdap.common.config.ConfigHelpers;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.netty.NettyHelpers;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.TracServiceConfig;
import org.finos.tracdap.common.service.TracServiceBase;
import org.finos.tracdap.common.validation.ValidationConcern;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.common.metadata.store.IMetadataStore;
import org.finos.tracdap.config.TenantConfigMap;
import org.finos.tracdap.metadata.TenantInfo;
import org.finos.tracdap.svc.meta.api.MessageProcessor;
import org.finos.tracdap.svc.meta.services.ConfigService;
import org.finos.tracdap.svc.meta.services.MetadataReadService;
import org.finos.tracdap.svc.meta.services.MetadataSearchService;
import org.finos.tracdap.svc.meta.services.MetadataWriteService;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.finos.tracdap.svc.meta.api.TracMetadataApi;
import org.finos.tracdap.svc.meta.api.InternalMetadataApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.*;


public class TracMetadataService extends TracServiceBase {

    // This is a quick implementation of the service scaffold, it will need to be re-visited!
    // All the components are created in start()

    // Because we are using JDBC, there is no option for fully async execution
    // We will need a thread pool that can handle the maximum number of concurrent requests
    // Since requests will sit on the JDBC thread pool anyway, we can use thread-per-request
    // A primary executor is given to gRPC for handling incoming requests
    // Then use Runnable::run to execute JDBC calls directly, i.e. no hand-off to a secondary pool

    // We do set up a blocking queue as an overflow
    // It would be good to tie this into health reporting and load balancing
    // That is not for this first quick implementation!

    private static final String POOL_SIZE_KEY = "pool.size";
    private static final String POOL_OVERFLOW_KEY = "pool.overflow";

    private static final int DEFAULT_POOL_SIZE = 20;
    private static final int DEFAULT_OVERFLOW_SIZE = 10;

    private final Logger log;

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private IMetadataStore metadataStore;
    private ExecutorService executor;
    private Server server;

    public static void main(String[] args) {

        TracServiceBase.svcMain(TracMetadataService.class, args);
    }

    public TracMetadataService(PluginManager pluginManager, ConfigManager configManager) {

        this.log = LoggerFactory.getLogger(getClass());

        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) {

        try {

            // Load top level config files
            var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
            var tenantConfigMap = ConfigHelpers.loadTenantConfigMap(configManager, platformConfig);

            // Load the DAL service using the plugin loader mechanism
            var metadataStoreConfig = platformConfig.getMetadataStore();
            metadataStore = pluginManager.createService(IMetadataStore.class, metadataStoreConfig, configManager);
            metadataStore.start();

            // Check and log the configured tenants
            checkDatabaseTenants(tenantConfigMap);

            // Metadata DB props contains config need for the executor pool size
            var dalProps = new Properties();
            dalProps.putAll(metadataStoreConfig.getPropertiesMap());
            executor = createPrimaryExecutor(dalProps);

            // Set up services and APIs
            var readService = new MetadataReadService(metadataStore, platformConfig, tenantConfigMap);
            var writeService = new MetadataWriteService(metadataStore);
            var searchService = new MetadataSearchService(metadataStore);
            var configService = new ConfigService(metadataStore);

            var publicApi = new TracMetadataApi(readService, writeService, searchService, configService);
            var internalApi = new InternalMetadataApi(readService, writeService, searchService, configService);
            var messageProcessor = new MessageProcessor();

            // Common framework for cross-cutting concerns
            var commonConcerns = buildCommonConcerns();

            // Create the main server
            // This setup is thread-per-request using a thread pool executor
            // Underlying Netty pools / ELs are managed automatically by gRPC for now

            var serviceConfig = platformConfig.getServicesOrThrow(ConfigKeys.METADATA_SERVICE_KEY);
            var servicePort = serviceConfig.getPort();

            var serverBuilder = ServerBuilder
                    .forPort(servicePort)
                    .executor(executor)
                    .addService(publicApi)
                    .addService(internalApi)
                    .addService(messageProcessor);

            // Apply common concerns
            this.server = commonConcerns
                    .configureServer(serverBuilder)
                    .build();

            // Good to go, let's start!
            this.server.start();

        }
        catch (IOException e) {

            // Wrap startup errors in an EStartup
            throw new EStartup(e.getMessage(), e);
        }
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) throws InterruptedException {

        // Request the server shutdown first, this will stop new connections being accepted
        // Wait for the server to drain
        // Once there are no active requests, clean up internal resources

        var deadline = Instant.now().plus(shutdownTimeout);

        var serverDown = shutdownResource("Data service server", deadline, remaining -> {

            server.shutdown();
            return server.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        // Request / await is not available on the DAL!
        metadataStore.stop();

        var executorDown = shutdownResource("Executor thread pool)", deadline, remaining -> {

            executor.shutdown();
            return executor.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        if (serverDown && executorDown)
            return 0;

        if (!server.isTerminated())
            server.shutdownNow();

        return -1;
    }

    private GrpcConcern buildCommonConcerns() {

        var commonConcerns = TracServiceConfig.coreConcerns(TracMetadataService.class);

        // Validation concern for the APIs being served
        var validationConcern = new ValidationConcern(
                MetadataServiceProto.getDescriptor(),
                InternalMetadataProto.getDescriptor(),
                InternalMessagingProto.getDescriptor());

        commonConcerns = commonConcerns.addAfter(TracServiceConfig.TRAC_PROTOCOL, validationConcern);

        // Additional cross-cutting concerns configured by extensions
        for (var extension : pluginManager.getExtensions()) {
            commonConcerns = extension.addServiceConcerns(commonConcerns, configManager, ConfigKeys.METADATA_SERVICE_KEY);
        }

        return commonConcerns.build();
    }

    ExecutorService createPrimaryExecutor(Properties properties) {

        // Headroom threads - these threads get used after the core pool and the overflow queue is full
        // That is not the behaviour we want, we want to fill up the pool first, then start queuing
        // So, we just use the core pool

        // A small number of headroom threads might be useful for admin tasks to avoid starvation
        // Although, to actually do anything useful with that prioritization would be needed

        var IDLE_THREAD_TIMEOUT = Duration.of(60, ChronoUnit.SECONDS);

        try {

            // Use the DB pool settings to create the primary executor
            // As per comments at the top of this file

            var poolSize = readConfigInt(properties, POOL_SIZE_KEY, DEFAULT_POOL_SIZE);
            var overflowSize = readConfigInt(properties, POOL_OVERFLOW_KEY, DEFAULT_OVERFLOW_SIZE);

            var executor = NettyHelpers.threadPoolExecutor(
                    "meta-svc", poolSize, poolSize, overflowSize,
                    IDLE_THREAD_TIMEOUT.toMillis());

            executor.prestartAllCoreThreads();
            executor.allowCoreThreadTimeOut(false);

            return executor;
        }
        catch (NumberFormatException e) {

            var message = "Pool size and overflow must be integers: " + e.getMessage();
            log.error(message);
            throw new EStartup(message, e);
        }
    }

    private int readConfigInt(Properties props, String propKey, Integer propDefault) {

        // TODO: Reading config needs to be centralised
        // Standard methods for handling defaults, valid ranges etc.
        // One option is to use proto to define config objects and automate parsing
        // This would work well where configs need to be sent between components, e.g. the TRAC executor

        var propValue = props.getProperty(propKey);

        if (propValue == null || propValue.isBlank()) {

            if (propDefault == null) {

                var message = "Missing required config property: " + propKey;
                log.error(message);
                throw new EStartup(message);
            }
            else
                return propDefault;
        }

        try {
            return Integer.parseInt(propValue);
        }
        catch (NumberFormatException e) {

            var message = "Config property must be an integer: " + propKey + ", got value '" + propValue + "'";
            log.error(message);
            throw new EStartup(message);
        }
    }

    private void checkDatabaseTenants(TenantConfigMap tenantConfig) {

        log.info("Checking for active tenants...");

        var metadataTenants = metadataStore.listTenants();
        var configFileTenants = new HashMap<>(tenantConfig.getTenantsMap());

        for (var tenantInfo : metadataTenants) {

            var configFileEntry = configFileTenants.remove(tenantInfo.getTenantCode());

            if (configFileEntry != null && configFileEntry.containsProperties(ConfigKeys.TENANT_DISPLAY_NAME)) {
                var configDisplayName = configFileEntry.getPropertiesOrThrow(ConfigKeys.TENANT_DISPLAY_NAME);
                log.info("{}: {}", tenantInfo.getTenantCode(), configDisplayName);
            }
            else if (!tenantInfo.getDescription().isBlank()) {
                log.info("{}: {}", tenantInfo.getTenantCode(), tenantInfo.getDescription());
            }
            else {
                log.info("{}: (display name not set)", tenantInfo.getTenantCode());
            }
        }

        var activeTenants = metadataTenants.size();

        if (!configFileTenants.isEmpty()) {
            if (tenantConfig.getAutoActivate()) {
                log.info("Found {} new tenant(s), running auto activation...", configFileTenants.size());
                configFileTenants.keySet().forEach(this::activateTenant);
                activeTenants += configFileTenants.size();
            }
            else {
                var inactiveTenants = String.join(",", configFileTenants.keySet());
                log.warn("Some tenants are configured but not activated: [{}]", inactiveTenants);
            }
        }

        if (activeTenants > 0)
            log.info("Found {} active tenant(s)", activeTenants);
        else
            log.warn("No active tenants found");
    }

    private void activateTenant(String tenantCode) {

        log.info("Activate tenant: [{}]", tenantCode);

        var tenantInfo = TenantInfo.newBuilder()
                .setTenantCode(tenantCode)
                .build();

        metadataStore.activateTenant(tenantInfo);
    }
}
