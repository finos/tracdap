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
import org.finos.tracdap.api.PlatformConfigListRequest;
import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.api.internal.PlatformConfigUpdate;
import org.finos.tracdap.api.internal.ReceivedCode;
import org.finos.tracdap.common.config.ConfigDefaults;
import org.finos.tracdap.common.config.ConfigHelpers;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.netty.NettyHelpers;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.config.ServiceConfig;
import org.finos.tracdap.metadata.PlatformConfigEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Brings the {@link IPlatformConfigListener} instances in a service's registry up to date with the metadata store
 * on a fixed interval, for platform config updates whose notification never arrived or could not be applied.
 *
 * <p>Each refresh lists the entries of every class with a registered listener and compares their versions with
 * the entries already applied. New, changed and deleted entries are passed to the listener as updates. The first
 * refresh applies every entry, listeners must accept an update they have already seen.</p>
 */
public class PlatformConfigReconciler {

    private static final Duration METADATA_CALL_TIMEOUT = Duration.ofSeconds(30);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginRegistry registry;
    private final InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient;
    private final GrpcConcern commonConcerns;
    private final Duration interval;

    private final Map<String, Map<String, Integer>> appliedVersions;
    private ScheduledExecutorService scheduler;

    public PlatformConfigReconciler(
            PluginRegistry registry,
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient,
            GrpcConcern commonConcerns, Duration interval) {

        this.registry = registry;
        this.metaClient = metaClient;
        this.commonConcerns = commonConcerns;
        this.interval = interval;
        this.appliedVersions = new ConcurrentHashMap<>();
    }

    /**
     * The refresh interval set for a service, zero turns refreshing off.
     */
    public static Duration refreshInterval(String serviceKey, ServiceConfig serviceConfig) {

        var serviceProperties = new Properties();
        serviceProperties.putAll(serviceConfig.getPropertiesMap());

        var seconds = ConfigHelpers.readInt(
                serviceKey, serviceProperties,
                ConfigKeys.PLATFORM_CONFIG_REFRESH_INTERVAL,
                ConfigDefaults.PLATFORM_CONFIG_REFRESH_INTERVAL);

        return Duration.ofSeconds(Math.max(seconds, 0));
    }

    public void start() {

        if (interval.isZero()) {
            log.info("Platform config refresh is turned off");
            return;
        }

        log.info("Platform config refresh every {} second(s)", interval.toSeconds());

        scheduler = Executors.newSingleThreadScheduledExecutor(NettyHelpers.threadFactory("platform-config"));
        scheduler.scheduleWithFixedDelay(this::refresh, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {

        if (scheduler != null)
            scheduler.shutdownNow();
    }

    void refresh() {

        var listeners = registry.getNamedInstances(IPlatformConfigListener.class);

        for (var listener : listeners.entrySet()) {

            try {
                refreshClass(listener.getKey(), listener.getValue());
            }
            catch (Exception e) {
                log.error("Platform config refresh failed: class = [{}], {}", listener.getKey(), e.getMessage(), e);
            }
        }
    }

    private void refreshClass(String configClass, IPlatformConfigListener listener) {

        var clientState = commonConcerns.prepareClientCall(Context.ROOT);
        var client = clientState.configureClient(metaClient)
                .withDeadlineAfter(METADATA_CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        var listRequest = PlatformConfigListRequest.newBuilder()
                .setConfigClass(configClass)
                .build();

        var current = new HashMap<String, PlatformConfigEntry>();

        for (var entry : client.listPlatformConfigEntries(listRequest).getEntriesList())
            current.put(entry.getConfigKey(), entry);

        var firstRefresh = !appliedVersions.containsKey(configClass);
        var applied = appliedVersions.computeIfAbsent(configClass, key -> new ConcurrentHashMap<>());

        for (var entry : current.values()) {

            var appliedVersion = applied.get(entry.getConfigKey());

            if (appliedVersion != null && appliedVersion == entry.getConfigVersion())
                continue;

            var updateType = appliedVersion == null ? ConfigUpdateType.CREATE : ConfigUpdateType.UPDATE;

            if (applyUpdate(listener, updateType, entry, !firstRefresh))
                applied.put(entry.getConfigKey(), entry.getConfigVersion());
        }

        for (var configKey : applied.keySet()) {

            if (current.containsKey(configKey))
                continue;

            var deletedEntry = PlatformConfigEntry.newBuilder()
                    .setConfigClass(configClass)
                    .setConfigKey(configKey)
                    .setConfigDeleted(true)
                    .build();

            if (applyUpdate(listener, ConfigUpdateType.DELETE, deletedEntry, true))
                applied.remove(configKey);
        }
    }

    private boolean applyUpdate(
            IPlatformConfigListener listener, ConfigUpdateType updateType,
            PlatformConfigEntry entry, boolean logApplied) {

        var update = PlatformConfigUpdate.newBuilder()
                .setUpdateType(updateType)
                .setConfigEntry(entry)
                .build();

        try {

            var status = listener.applyConfigUpdate(update);

            if (logApplied && status.getCode() == ReceivedCode.OK)
                log.info("Platform config refreshed: class = [{}], key = [{}], update = {}",
                        entry.getConfigClass(), entry.getConfigKey(), updateType);

            return true;
        }
        catch (Exception e) {

            log.error("Platform config refresh could not be applied: class = [{}], key = [{}], {}",
                    entry.getConfigClass(), entry.getConfigKey(), e.getMessage(), e);

            return false;
        }
    }
}
