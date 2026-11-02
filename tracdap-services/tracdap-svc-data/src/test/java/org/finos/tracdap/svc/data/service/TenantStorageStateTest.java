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
package org.finos.tracdap.svc.data.service;

import org.finos.tracdap.common.codec.CodecManager;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.metadata.*;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;


class TenantStorageStateTest {

    @TempDir
    Path tempDir;

    private PluginManager pluginManager;
    private ConfigManager configManager;
    private CodecManager codecManager;
    private NioEventLoopGroup elg;

    @BeforeEach
    void setup() {

        pluginManager = new PluginManager();
        pluginManager.registerExtensions();
        pluginManager.initConfigPlugins();

        configManager = new ConfigManager("trac-platform.yaml", tempDir, pluginManager);
        pluginManager.initRegularPlugins();

        codecManager = new CodecManager(pluginManager, configManager);
        elg = new NioEventLoopGroup(1);
    }

    @AfterEach
    void shutdown() {

        elg.shutdownGracefully();
    }

    @Test
    void startup_missingRootFails() {

        var config = tenantConfig(tempDir.resolve("datasets"));

        Assertions.assertThrows(EStartup.class, () -> tenantState(config, false));
        Assertions.assertFalse(Files.exists(tempDir.resolve("datasets")));
    }

    @Test
    void discoveredLive_missingRootCreated() {

        var config = tenantConfig(tempDir.resolve("datasets"));
        var state = tenantState(config, true);

        Assertions.assertTrue(Files.isDirectory(tempDir.resolve("datasets")));
        Assertions.assertNotNull(state.getStorageManager().getFileStorage("datasets"));

        state.shutdown();
    }

    @Test
    void discoveredLive_missingParentFails() {

        var config = tenantConfig(tempDir.resolve("tenants").resolve("datasets"));

        Assertions.assertThrows(EStartup.class, () -> tenantState(config, true));
        Assertions.assertFalse(Files.exists(tempDir.resolve("tenants")));
    }

    @Test
    void configCreated_missingRootCreated() throws Exception {

        Files.createDirectory(tempDir.resolve("datasets"));
        var state = tenantState(tenantConfig(tempDir.resolve("datasets")), false);

        var newRoot = tempDir.resolve("trac_results");
        state.configCreated(resourceEntry("trac_results"), localStorage(newRoot));

        Assertions.assertTrue(Files.isDirectory(newRoot));
        Assertions.assertNotNull(state.getStorageManager().getFileStorage("trac_results"));

        state.shutdown();
    }

    @Test
    void configUpdated_missingRootCreated() throws Exception {

        Files.createDirectory(tempDir.resolve("datasets"));
        var state = tenantState(tenantConfig(tempDir.resolve("datasets")), false);

        var movedRoot = tempDir.resolve("datasets_moved");
        state.configUpdated(resourceEntry("datasets"), localStorage(movedRoot));

        Assertions.assertTrue(Files.isDirectory(movedRoot));

        state.shutdown();
    }

    @Test
    void configCreated_existingKeyReplaced() throws Exception {

        Files.createDirectory(tempDir.resolve("datasets"));
        var state = tenantState(tenantConfig(tempDir.resolve("datasets")), false);

        // The same storage created twice, as when a discovered tenant receives a notification it already loaded
        state.configCreated(resourceEntry("datasets"), localStorage(tempDir.resolve("datasets")));

        Assertions.assertNotNull(state.getStorageManager().getFileStorage("datasets"));

        state.shutdown();
    }

    private TenantStorageState tenantState(TenantConfig config, boolean discoveredLive) {

        return new TenantStorageState(pluginManager, configManager, codecManager, elg, config, discoveredLive);
    }

    private TenantConfig tenantConfig(Path datasetsRoot) {

        return TenantConfig.newBuilder()
                .putProperties("storage.default.location", "datasets")
                .putProperties("storage.default.format", "ARROW_FILE")
                .putResources("datasets", localStorage(datasetsRoot).getResource())
                .build();
    }

    private static ObjectDefinition localStorage(Path rootPath) {

        var resource = ResourceDefinition.newBuilder()
                .setResourceType(ResourceType.INTERNAL_STORAGE)
                .setProtocol("LOCAL")
                .putProperties("rootPath", rootPath.toString());

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.RESOURCE)
                .setResource(resource)
                .build();
    }

    private static ConfigEntry resourceEntry(String storageKey) {

        return ConfigEntry.newBuilder()
                .setConfigClass("trac_resources")
                .setConfigKey(storageKey)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectType(ObjectType.RESOURCE)
                        .setResourceType(ResourceType.INTERNAL_STORAGE))
                .build();
    }
}
