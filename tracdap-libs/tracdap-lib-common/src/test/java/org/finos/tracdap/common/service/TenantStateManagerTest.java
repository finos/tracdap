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

import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.*;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.ETenantNotFound;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.config.TenantConfigMap;
import org.finos.tracdap.metadata.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


class TenantStateManagerTest {

    private static final GrpcConcern NO_CONCERNS = () -> "no_concerns";

    @TempDir
    Path tempDir;

    private FakeMetadataApi metadataApi;
    private Server server;
    private ManagedChannel channel;
    private InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient;
    private ConfigManager configManager;
    private ExecutorService executor;

    @BeforeEach
    void setup() throws Exception {

        metadataApi = new FakeMetadataApi();
        server = ServerBuilder.forPort(0).addService(metadataApi).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        metaClient = InternalMetadataApiGrpc.newBlockingStub(channel);

        var plugins = new PluginManager();
        plugins.registerExtensions();
        plugins.initConfigPlugins();
        configManager = new ConfigManager("trac-platform.yaml", tempDir, plugins);

        executor = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void shutdown() throws Exception {

        executor.shutdownNow();
        channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    void startup_brokenTenantSkipped() {

        metadataApi.tenants.addAll(List.of("GOOD", "BROKEN"));

        var manager = new TestManager(null);
        manager.failingTenants.add("BROKEN");
        manager.init();

        Assertions.assertNotNull(manager.getTenant("GOOD"));
        Assertions.assertThrows(ETenantNotFound.class, () -> manager.getTenant("BROKEN"));

        // A lookup miss does not retry a tenant that failed at startup
        manager.failingTenants.clear();
        Assertions.assertThrows(ETenantNotFound.class, () -> manager.getTenant("BROKEN"));
        Assertions.assertEquals(1, manager.initCount("BROKEN"));

        // A config update brings it back
        var status = manager.applyConfigUpdate(resourceUpdate("BROKEN"));
        Assertions.assertEquals(ReceivedCode.OK, status.getCode());
        Assertions.assertNotNull(manager.getTenant("BROKEN"));
        Assertions.assertTrue(manager.discoveredLive("BROKEN"));
    }

    @Test
    void update_unknownTenantDiscovered() {

        var manager = new TestManager(null);
        manager.init();

        metadataApi.tenants.add("NEW_TENANT");

        var status = manager.applyConfigUpdate(resourceUpdate("NEW_TENANT"));

        Assertions.assertEquals(ReceivedCode.OK, status.getCode());
        Assertions.assertNotNull(manager.getTenant("NEW_TENANT"));
        Assertions.assertEquals(1, manager.initCount("NEW_TENANT"));
        Assertions.assertTrue(manager.discoveredLive("NEW_TENANT"));

        // The update that triggered discovery is already in the loaded config, it is not applied again
        Assertions.assertEquals(0, manager.getTenant("NEW_TENANT").created.size());

        // Later updates are applied as normal
        manager.applyConfigUpdate(resourceUpdate("NEW_TENANT"));
        Assertions.assertEquals(1, manager.getTenant("NEW_TENANT").created.size());
    }

    @Test
    void update_nonexistentTenantIgnored() {

        var manager = new TestManager(null);
        manager.init();

        var status = manager.applyConfigUpdate(resourceUpdate("GHOST_TENANT"));

        Assertions.assertEquals(ReceivedCode.IGNORED, status.getCode());
        Assertions.assertThrows(ETenantNotFound.class, () -> manager.getTenant("GHOST_TENANT"));
    }

    @Test
    void update_simultaneousUpdatesDiscoverOnce() throws Exception {

        var manager = new TestManager(null);
        manager.init();

        metadataApi.tenants.add("NEW_TENANT");
        metadataApi.listTenantsGate = new CountDownLatch(1);

        var updates = new ArrayList<Future<ReceivedStatus>>();

        for (var i = 0; i < 3; i++)
            updates.add(executor.submit(() -> manager.applyConfigUpdate(resourceUpdate("NEW_TENANT"))));

        // Hold the first discovery open while the other updates arrive
        waitFor(() -> metadataApi.listTenantsCalls.get() > 0);
        Thread.sleep(200);
        metadataApi.listTenantsGate.countDown();

        for (var update : updates)
            Assertions.assertEquals(ReceivedCode.OK, update.get(10, TimeUnit.SECONDS).getCode());

        Assertions.assertEquals(1, manager.initCount("NEW_TENANT"));
        Assertions.assertEquals(1, metadataApi.listTenantsCalls.get() - 1);
    }

    @Test
    void lookupMiss_discoveredInsideLookup() {

        var manager = new TestManager(null);
        manager.init();

        metadataApi.tenants.add("NEW_TENANT");

        Assertions.assertNotNull(manager.getTenant("NEW_TENANT"));
        Assertions.assertEquals(1, manager.initCount("NEW_TENANT"));
        Assertions.assertTrue(manager.discoveredLive("NEW_TENANT"));

        Assertions.assertThrows(ETenantNotFound.class, () -> manager.getTenant("GHOST_TENANT"));
    }

    @Test
    void lookupMiss_discoveredOnExecutor() {

        var manager = new TestManager(executor);
        manager.init();

        metadataApi.tenants.add("NEW_TENANT");

        Assertions.assertThrows(ETenantNotFound.class, () -> manager.getTenant("NEW_TENANT"));

        waitFor(() -> manager.initCount("NEW_TENANT") > 0);

        Assertions.assertNotNull(manager.getTenant("NEW_TENANT"));
        Assertions.assertEquals(1, manager.initCount("NEW_TENANT"));
    }

    @Test
    void discoveryFailure_addsNothing() {

        var manager = new TestManager(null);
        manager.init();

        metadataApi.tenants.add("NEW_TENANT");
        manager.failingTenants.add("NEW_TENANT");

        Assertions.assertThrows(IllegalStateException.class, () -> manager.applyConfigUpdate(resourceUpdate("NEW_TENANT")));
        Assertions.assertThrows(ETenantNotFound.class, () -> manager.getTenant("NEW_TENANT"));

        manager.failingTenants.clear();

        var status = manager.applyConfigUpdate(resourceUpdate("NEW_TENANT"));
        Assertions.assertEquals(ReceivedCode.OK, status.getCode());
        Assertions.assertNotNull(manager.getTenant("NEW_TENANT"));
    }

    private static ConfigUpdate resourceUpdate(String tenant) {

        return ConfigUpdate.newBuilder()
                .setTenant(tenant)
                .setUpdateType(ConfigUpdateType.CREATE)
                .setConfigEntry(ConfigEntry.newBuilder()
                        .setConfigClass("trac_resources")
                        .setConfigKey("datasets")
                        .setConfigVersion(1)
                        .setIsLatestConfig(true)
                        .setDetails(ConfigDetails.newBuilder()
                                .setObjectType(ObjectType.RESOURCE)
                                .setResourceType(ResourceType.INTERNAL_STORAGE)))
                .build();
    }

    private static void waitFor(Callable<Boolean> condition) {

        var deadline = Instant.now().plus(Duration.ofSeconds(10));

        try {
            while (!condition.call()) {
                if (Instant.now().isAfter(deadline))
                    Assertions.fail("Timed out waiting for condition");
                Thread.sleep(20);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class TestManager extends TenantStateManager<TestState> {

        final Set<String> failingTenants = ConcurrentHashMap.newKeySet();
        final Map<String, AtomicInteger> initCounts = new ConcurrentHashMap<>();
        final Map<String, Boolean> discoveredLive = new ConcurrentHashMap<>();

        TestManager(Executor discoveryExecutor) {
            super(TenantConfigMap.getDefaultInstance(), configManager, metaClient, NO_CONCERNS, discoveryExecutor);
        }

        @Override
        protected TestState initTenant(String tenantCode, TenantConfig initialConfig, boolean discoveredLive) {

            initCounts.computeIfAbsent(tenantCode, x -> new AtomicInteger()).incrementAndGet();

            if (failingTenants.contains(tenantCode))
                throw new IllegalStateException("Tenant cannot be initialized: " + tenantCode);

            this.discoveredLive.put(tenantCode, discoveredLive);

            return new TestState();
        }

        int initCount(String tenantCode) {
            var count = initCounts.get(tenantCode);
            return count != null ? count.get() : 0;
        }

        boolean discoveredLive(String tenantCode) {
            return discoveredLive.getOrDefault(tenantCode, false);
        }
    }

    private static class TestState extends TenantState {

        final List<ConfigEntry> created = new CopyOnWriteArrayList<>();

        @Override protected boolean configIsRelevant(ConfigEntry entry) { return true; }
        @Override protected void tenantPropertiesUpdated(TenantConfig tenantConfig) {}
        @Override protected void configCreated(ConfigEntry entry, ObjectDefinition definition) { created.add(entry); }
        @Override protected void configUpdated(ConfigEntry entry, ObjectDefinition definition) {}
        @Override protected void configDeleted(ConfigEntry entry) {}
        @Override protected void shutdown() {}
    }

    private static class FakeMetadataApi extends InternalMetadataApiGrpc.InternalMetadataApiImplBase {

        final Set<String> tenants = ConcurrentHashMap.newKeySet();
        final AtomicInteger listTenantsCalls = new AtomicInteger();
        volatile CountDownLatch listTenantsGate;

        @Override
        public void listTenants(ListTenantsRequest request, StreamObserver<ListTenantsResponse> response) {

            listTenantsCalls.incrementAndGet();

            try {
                var gate = listTenantsGate;
                if (gate != null)
                    gate.await(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            var result = ListTenantsResponse.newBuilder();
            tenants.forEach(code -> result.addTenants(TenantInfo.newBuilder().setTenantCode(code)));

            response.onNext(result.build());
            response.onCompleted();
        }

        @Override
        public void listConfigEntries(ConfigListRequest request, StreamObserver<ConfigListResponse> response) {

            response.onNext(ConfigListResponse.getDefaultInstance());
            response.onCompleted();
        }

        @Override
        public void readConfigEntry(ConfigReadRequest request, StreamObserver<ConfigReadResponse> response) {

            var definition = ObjectDefinition.newBuilder()
                    .setObjectType(ObjectType.RESOURCE)
                    .setResource(ResourceDefinition.newBuilder()
                            .setResourceType(ResourceType.INTERNAL_STORAGE)
                            .setProtocol("LOCAL"))
                    .build();

            response.onNext(ConfigReadResponse.newBuilder()
                    .setEntry(request.getEntry())
                    .setDefinition(definition)
                    .build());
            response.onCompleted();
        }
    }
}
