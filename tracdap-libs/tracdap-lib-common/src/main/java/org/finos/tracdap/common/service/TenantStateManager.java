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

package org.finos.tracdap.common.service;

import io.grpc.Context;
import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.*;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.ETenantNotFound;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.config.TenantConfigMap;
import org.finos.tracdap.metadata.ConfigDefinition;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.ObjectType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public abstract class TenantStateManager<TState extends TenantState> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final TenantConfigMap staticConfigMap;
    private final ConcurrentHashMap<String, TState> liveTenantMap;
    private final ConcurrentHashMap<String, CompletableFuture<TState>> discoveries;
    private final Set<String> failedAtStartup;

    private final ConfigManager configManager;
    private final InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient;
    private final GrpcConcern commonConcerns;
    private final Executor discoveryExecutor;

    protected abstract TState initTenant(String tenantCode, TenantConfig initialConfig, boolean discoveredLive);

    public TenantStateManager(
            TenantConfigMap staticConfigMap, ConfigManager configManager,
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient,
            GrpcConcern commonConcerns) {

        this(staticConfigMap, configManager, metaClient, commonConcerns, null);
    }

    /**
     * Tenants missing from a lookup are discovered on the discovery executor if one is supplied,
     * otherwise they are discovered inside the lookup.
     */
    public TenantStateManager(
            TenantConfigMap staticConfigMap, ConfigManager configManager,
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient,
            GrpcConcern commonConcerns, Executor discoveryExecutor) {

        this.staticConfigMap = staticConfigMap;
        this.liveTenantMap = new ConcurrentHashMap<>();
        this.discoveries = new ConcurrentHashMap<>();
        this.failedAtStartup = ConcurrentHashMap.newKeySet();

        this.configManager = configManager;
        this.metaClient = metaClient;
        this.commonConcerns = commonConcerns;
        this.discoveryExecutor = discoveryExecutor;
    }

    public TState getTenant(String tenantCode) {

        var liveTenant = liveTenantMap.get(tenantCode);

        if (liveTenant != null)
            return liveTenant;

        // Tenants that failed at startup are only brought back by a config update
        if (!failedAtStartup.contains(tenantCode)) {

            if (discoveryExecutor == null) {

                var discovered = tryDiscoverTenant(tenantCode);

                if (discovered != null)
                    return discovered;
            }
            else if (!discoveries.containsKey(tenantCode)) {

                discoveryExecutor.execute(() -> tryDiscoverTenant(tenantCode));
            }
        }

        var message = String.format("Tenant not found: [%s]", tenantCode);
        throw new ETenantNotFound(message);
    }

    public void init() {

        log.info("Looking for active tenants...");

        var clientState = commonConcerns.prepareClientCall(Context.ROOT);
        var client = clientState.configureClient(metaClient);

        var liveTenantList = client.listTenants(ListTenantsRequest.getDefaultInstance());

        for (var tenantInfo : liveTenantList.getTenantsList()) {

            var tenantCode = tenantInfo.getTenantCode();

            try {

                log.info("Initializing tenant: [{}]", tenantCode);

                var liveConfig = loadTenantConfig(tenantCode);
                var liveTenant = initTenant(tenantCode, liveConfig, false);

                liveTenantMap.put(tenantCode, liveTenant);
            }
            catch (Exception e) {

                log.error("Tenant could not be initialized: [{}] {}", tenantCode, e.getMessage(), e);
                log.error("Startup will continue without this tenant");

                failedAtStartup.add(tenantCode);
            }
        }

        if (liveTenantMap.isEmpty())
            log.warn("No active tenants found");
        else
            log.info("Initialized {} active tenant(s)", liveTenantMap.size());

        var inactiveTenants = staticConfigMap.getTenantsMap().keySet()
                .stream().filter(tenantCode -> !liveTenantMap.containsKey(tenantCode))
                .collect(Collectors.toList());

        if (!inactiveTenants.isEmpty()) {
            log.warn("Some tenants are configured but not activated: [{}]", String.join(",", inactiveTenants));
        }
    }

    public void shutdown() {

        log.info("Shutting down active tenants...");

        var tenants = liveTenantMap.entrySet().iterator();

        while (tenants.hasNext()) {

            var tenant = tenants.next();
            var tenantState = tenant.getValue();

            try {

                log.info("Shutting down tenant: [{}]", tenant.getKey());

                tenantState.shutdown();
                tenants.remove();
            }
            catch (Exception e) {

                log.error("Tenant was not shut down cleanly: [{}] {}", tenant.getKey(), e.getMessage(), e);
                log.error("Shutdown sequence will continue");
            }
        }
    }

    private TState tryDiscoverTenant(String tenantCode) {

        try {
            return discoverTenant(tenantCode);
        }
        catch (Exception e) {
            return null;
        }
    }

    private TState discoverTenant(String tenantCode) {

        var discovery = new CompletableFuture<TState>();
        var priorDiscovery = discoveries.putIfAbsent(tenantCode, discovery);

        if (priorDiscovery != null) {

            try {
                return priorDiscovery.join();
            }
            catch (CompletionException e) {
                throw e.getCause() instanceof RuntimeException ? (RuntimeException) e.getCause() : e;
            }
        }

        try {

            var liveTenant = liveTenantMap.get(tenantCode);

            if (liveTenant == null)
                liveTenant = initDiscoveredTenant(tenantCode);

            discovery.complete(liveTenant);

            return liveTenant;
        }
        catch (RuntimeException e) {

            log.error("Tenant could not be discovered: [{}] {}", tenantCode, e.getMessage(), e);

            discovery.completeExceptionally(e);
            throw e;
        }
        finally {

            discoveries.remove(tenantCode, discovery);
        }
    }

    private TState initDiscoveredTenant(String tenantCode) {

        var clientState = commonConcerns.prepareClientCall(Context.ROOT);
        var client = clientState.configureClient(metaClient);

        var liveTenantList = client.listTenants(ListTenantsRequest.getDefaultInstance());
        var tenantExists = liveTenantList.getTenantsList().stream()
                .anyMatch(tenantInfo -> tenantInfo.getTenantCode().equals(tenantCode));

        if (!tenantExists)
            return null;

        log.info("Initializing discovered tenant: [{}]", tenantCode);

        var liveConfig = loadTenantConfig(tenantCode);
        var liveTenant = initTenant(tenantCode, liveConfig, true);

        liveTenantMap.put(tenantCode, liveTenant);
        failedAtStartup.remove(tenantCode);

        return liveTenant;
    }

    public TenantConfig loadTenantConfig(String tenantCode) {

        var staticConfig = staticConfigMap.getTenantsOrDefault(tenantCode, TenantConfig.getDefaultInstance());
        var liveConfig = staticConfig.toBuilder();

        var configEntries = fetchMetadataConfig(tenantCode, ConfigKeys.TRAC_CONFIG);

        for (var entry : configEntries) {

            if (entry.getDefinition().getObjectType() != ObjectType.CONFIG)
                throw new EUnexpected();

            var configKey = entry.getEntry().getConfigKey();

            if (configKey.equals(ConfigKeys.TRAC_TENANT_CONFIG)) {
                mergeTenantProperties(staticConfig, liveConfig, entry.getDefinition().getConfig());
            }
            else if (!staticConfig.containsConfig(configKey)) {
                liveConfig.putConfig(configKey, entry.getDefinition().getConfig());
            }
        }

        var resourceEntries = fetchMetadataConfig(tenantCode, ConfigKeys.TRAC_RESOURCES);

        for (var entry : resourceEntries) {

            if (entry.getDefinition().getObjectType() != ObjectType.RESOURCE)
                throw new EUnexpected();

            var resourceKey = entry.getEntry().getConfigKey();

            if (!staticConfig.containsResources(resourceKey))
                liveConfig.putResources(resourceKey, entry.getDefinition().getResource());
        }

        return liveConfig.build();
    }

    public ReceivedStatus applyConfigUpdate(ConfigUpdate update) {

        log.info("Config update: tenant = {}, config class = {}, config key = {}",
                update.getTenant(),
                update.getConfigEntry().getConfigClass(),
                update.getConfigEntry().getConfigKey());

        var staticConfig = staticConfigMap.getTenantsOrDefault(update.getTenant(), TenantConfig.getDefaultInstance());
        var liveState = liveTenantMap.get(update.getTenant());

        // A tenant not seen before is discovered, its current config already includes this update
        if (liveState == null) {

            var discovered = discoverTenant(update.getTenant());

            if (discovered == null) {
                log.warn("Config update ignored (tenant not found: [{}])", update.getTenant());
                return ReceivedStatus.newBuilder().setCode(ReceivedCode.IGNORED).build();
            }

            log.info("Config update applied successfully (tenant discovered: [{}])", update.getTenant());
            return ReceivedStatus.newBuilder().setCode(ReceivedCode.OK).build();
        }

        // If the updated includes any secrets, ensure tenant secrets are refreshed
        if (configManager.hasSecrets() && update.getSecretsUpdated()) {
            var tenantSecrets = configManager.getSecrets().namedScope(ConfigKeys.TENANT_SCOPE, update.getTenant());
            tenantSecrets.reload();
        }

        // Updates to the tenant-level properties are always processed
        if (update.getConfigEntry().getConfigClass().equals(ConfigKeys.TRAC_CONFIG) &&
            update.getConfigEntry().getConfigKey().equals(ConfigKeys.TRAC_TENANT_CONFIG)) {

            var metadataEntry = fetchMetadataConfig(update);
            var metadataConfig = metadataEntry.getDefinition().getConfig();
            var liveConfig = mergeTenantProperties(staticConfig, metadataConfig);
            liveState.tenantPropertiesUpdated(liveConfig);

            log.info("Config update applied successfully");
            return ReceivedStatus.newBuilder().setCode(ReceivedCode.OK).build();
        }

        // Ignore dynamic updates if there is a static config entry
        if (staticEntryExists(staticConfig, update.getConfigEntry())) {
            log.warn("Config update ignored (static config cannot be updated)");
            return ReceivedStatus.newBuilder().setCode(ReceivedCode.IGNORED).build();
        }

        // Ignore updates that are not relevant to the state
        if (!liveState.configIsRelevant(update.getConfigEntry())) {
            log.info("Config update ignored (not relevant)");
            return ReceivedStatus.newBuilder().setCode(ReceivedCode.IGNORED).build();
        }

        // Update is relevant - process it
        var entry = fetchMetadataConfig(update);

        switch (update.getUpdateType()) {

            case CREATE:
                liveState.configCreated(update.getConfigEntry(), entry.getDefinition());
                break;

            case UPDATE:
                liveState.configUpdated(update.getConfigEntry(), entry.getDefinition());
                break;

            case DELETE:
                liveState.configDeleted(update.getConfigEntry());
                break;

            default:
                throw new EUnexpected();
        }

        log.info("Config update applied successfully");
        return ReceivedStatus.newBuilder().setCode(ReceivedCode.OK).build();
    }

    private List<ConfigReadResponse> fetchMetadataConfig(String tenantCode, String configClass) {

        var clientState = commonConcerns.prepareClientCall(Context.ROOT);
        var client = clientState.configureClient(metaClient);

        var listRequest = ConfigListRequest.newBuilder()
                .setTenant(tenantCode)
                .setConfigClass(configClass)
                .build();

        var listResponse = client.listConfigEntries(listRequest);

        // Do not make a read request if there are no config entries
        if (listResponse.getEntriesCount() == 0)
            return List.of();

        var readRequest = ConfigReadBatchRequest.newBuilder()
                .setTenant(tenantCode)
                .addAllEntries(listResponse.getEntriesList());

        var readResponse = client.readConfigBatch(readRequest.build());

        return readResponse.getEntriesList();
    }

    private ConfigReadResponse fetchMetadataConfig(String tenantCode, String configClass, String configKey) {

        var clientState = commonConcerns.prepareClientCall(Context.ROOT);
        var client = clientState.configureClient(metaClient);

        var readRequest = ConfigReadRequest.newBuilder()
                .setTenant(tenantCode)
                .setEntry(ConfigEntry.newBuilder()
                        .setConfigClass(configClass)
                        .setConfigKey(configKey)
                        .setIsLatestConfig(true))
                .build();

        return client.readConfigEntry(readRequest);
    }

    private ConfigReadResponse fetchMetadataConfig(ConfigUpdate update) {

        if (update.getUpdateType() == ConfigUpdateType.CREATE || update.getUpdateType() == ConfigUpdateType.UPDATE) {

            return fetchMetadataConfig(
                    update.getTenant(),
                    update.getConfigEntry().getConfigClass(),
                    update.getConfigEntry().getConfigKey());
        }

        if (update.getUpdateType() == ConfigUpdateType.DELETE) {

            return ConfigReadResponse.newBuilder()
                    .setEntry(update.getConfigEntry())
                    .build();
        }

        throw new EUnexpected();
    }

    private boolean staticEntryExists(TenantConfig staticConfig, ConfigEntry entry) {

        var staticEntry = entry.getConfigClass().equals(ConfigKeys.TRAC_CONFIG) && staticConfig.containsConfig(entry.getConfigKey());
        var staticResource = entry.getConfigClass().equals(ConfigKeys.TRAC_RESOURCES) && staticConfig.containsResources(entry.getConfigKey());

        return staticEntry || staticResource;
    }

    static TenantConfig mergeTenantProperties(TenantConfig staticConfig, ConfigDefinition metadataConfig) {

        return mergeTenantProperties(staticConfig, TenantConfig.newBuilder(), metadataConfig).build();
    }

    static TenantConfig.Builder mergeTenantProperties(
            TenantConfig staticConfig, TenantConfig.Builder liveConfig,
            ConfigDefinition metadataConfig) {

        liveConfig.putAllProperties(staticConfig.getPropertiesMap());
        liveConfig.putAllSecrets(staticConfig.getSecretsMap());

        for (var property : metadataConfig.getPropertiesMap().entrySet()) {
            var key = property.getKey();
            if (!staticConfig.containsProperties(key) && !staticConfig.containsSecrets(key))
                liveConfig.putProperties(key, property.getValue());
        }

        return liveConfig;
    }
}
