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

import org.finos.tracdap.api.PlatformConfigListRequest;
import org.finos.tracdap.api.PlatformConfigListResponse;
import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.api.internal.PlatformConfigUpdate;
import org.finos.tracdap.api.internal.ReceivedCode;
import org.finos.tracdap.api.internal.ReceivedStatus;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.config.ServiceConfig;
import org.finos.tracdap.metadata.PlatformConfigEntry;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;


class PlatformConfigReconcilerTest {

    private static final GrpcConcern NO_CONCERNS = () -> "no_concerns";

    private FakeMetadataApi metadataApi;
    private Server server;
    private ManagedChannel channel;
    private InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient;
    private PluginRegistry registry;

    @BeforeEach
    void setup() throws Exception {

        metadataApi = new FakeMetadataApi();
        server = ServerBuilder.forPort(0).addService(metadataApi).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        metaClient = InternalMetadataApiGrpc.newBlockingStub(channel);
        registry = new PluginRegistry();
    }

    @AfterEach
    void shutdown() throws Exception {

        channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }

    private PlatformConfigReconciler reconciler() {
        return new PlatformConfigReconciler(registry, metaClient, NO_CONCERNS, Duration.ofSeconds(60));
    }

    @Test
    void firstRefresh_appliesEveryEntry() {

        var listener = registerListener("license");
        metadataApi.put("license", "license", 1);

        var reconciler = reconciler();
        reconciler.refresh();

        Assertions.assertEquals(1, listener.updates.size());
        Assertions.assertEquals(ConfigUpdateType.CREATE, listener.updates.get(0).getUpdateType());
        Assertions.assertEquals("license", listener.updates.get(0).getConfigEntry().getConfigKey());

        reconciler.refresh();

        Assertions.assertEquals(1, listener.updates.size());
    }

    @Test
    void missedChanges_appliedOnNextRefresh() {

        var listener = registerListener("tenant");
        metadataApi.put("tenant", "TENANT_1", 1);
        metadataApi.put("tenant", "TENANT_2", 1);

        var reconciler = reconciler();
        reconciler.refresh();
        listener.updates.clear();

        metadataApi.put("tenant", "TENANT_1", 2);
        metadataApi.remove("tenant", "TENANT_2");
        metadataApi.put("tenant", "TENANT_3", 1);

        reconciler.refresh();

        var updates = Map.of(
                "TENANT_1", ConfigUpdateType.UPDATE,
                "TENANT_2", ConfigUpdateType.DELETE,
                "TENANT_3", ConfigUpdateType.CREATE);

        Assertions.assertEquals(3, listener.updates.size());

        for (var update : listener.updates) {
            var key = update.getConfigEntry().getConfigKey();
            Assertions.assertEquals(updates.get(key), update.getUpdateType(), key);
        }
    }

    @Test
    void failedUpdate_retriedOnNextRefresh() {

        var listener = registerListener("license");
        listener.failing = true;
        metadataApi.put("license", "license", 1);

        var reconciler = reconciler();
        reconciler.refresh();

        Assertions.assertEquals(1, listener.updates.size());

        listener.failing = false;
        reconciler.refresh();

        Assertions.assertEquals(2, listener.updates.size());

        reconciler.refresh();

        Assertions.assertEquals(2, listener.updates.size());
    }

    @Test
    void failedList_otherClassesStillRefreshed() {

        var failing = registerListener("identity");
        var working = registerListener("license");
        metadataApi.put("identity", "U1", 1);
        metadataApi.put("license", "license", 1);
        metadataApi.failingClasses.add("identity");

        var reconciler = reconciler();
        reconciler.refresh();

        Assertions.assertEquals(0, failing.updates.size());
        Assertions.assertEquals(1, working.updates.size());

        metadataApi.failingClasses.clear();
        reconciler.refresh();

        Assertions.assertEquals(1, failing.updates.size());
    }

    @Test
    void classWithoutListener_notListed() {

        registerListener("license");
        metadataApi.put("other", "x", 1);

        reconciler().refresh();

        Assertions.assertEquals(List.of("license"), metadataApi.listedClasses);
    }

    @Test
    void refreshInterval_fromServiceProperties() {

        var unset = ServiceConfig.newBuilder().build();
        var set = ServiceConfig.newBuilder().putProperties(ConfigKeys.PLATFORM_CONFIG_REFRESH_INTERVAL, "15").build();
        var off = ServiceConfig.newBuilder().putProperties(ConfigKeys.PLATFORM_CONFIG_REFRESH_INTERVAL, "0").build();

        Assertions.assertEquals(Duration.ofSeconds(60), PlatformConfigReconciler.refreshInterval("data", unset));
        Assertions.assertEquals(Duration.ofSeconds(15), PlatformConfigReconciler.refreshInterval("data", set));
        Assertions.assertEquals(Duration.ZERO, PlatformConfigReconciler.refreshInterval("data", off));
    }

    private RecordingListener registerListener(String configClass) {

        var listener = new RecordingListener();
        registry.addNamedInstance(IPlatformConfigListener.class, configClass, listener);
        return listener;
    }

    private static class RecordingListener implements IPlatformConfigListener {

        final List<PlatformConfigUpdate> updates = new CopyOnWriteArrayList<>();
        volatile boolean failing;

        @Override
        public ReceivedStatus applyConfigUpdate(PlatformConfigUpdate update) {

            updates.add(update);

            if (failing)
                throw new IllegalStateException("listener failed");

            return ReceivedStatus.newBuilder().setCode(ReceivedCode.OK).build();
        }
    }

    private static class FakeMetadataApi extends InternalMetadataApiGrpc.InternalMetadataApiImplBase {

        final Map<String, Map<String, PlatformConfigEntry>> entries = new ConcurrentHashMap<>();
        final Set<String> failingClasses = ConcurrentHashMap.newKeySet();
        final List<String> listedClasses = new CopyOnWriteArrayList<>();

        void put(String configClass, String configKey, int version) {

            entries.computeIfAbsent(configClass, x -> new ConcurrentHashMap<>()).put(configKey, PlatformConfigEntry.newBuilder()
                    .setConfigClass(configClass)
                    .setConfigKey(configKey)
                    .setConfigVersion(version)
                    .build());
        }

        void remove(String configClass, String configKey) {
            entries.get(configClass).remove(configKey);
        }

        @Override
        public void listPlatformConfigEntries(PlatformConfigListRequest request, StreamObserver<PlatformConfigListResponse> response) {

            listedClasses.add(request.getConfigClass());

            if (failingClasses.contains(request.getConfigClass())) {
                response.onError(Status.UNAVAILABLE.withDescription("list failed").asRuntimeException());
                return;
            }

            var classEntries = entries.getOrDefault(request.getConfigClass(), Map.of());

            response.onNext(PlatformConfigListResponse.newBuilder().addAllEntries(classEntries.values()).build());
            response.onCompleted();
        }
    }
}
