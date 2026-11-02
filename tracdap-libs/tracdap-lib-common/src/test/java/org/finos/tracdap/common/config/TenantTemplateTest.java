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
package org.finos.tracdap.common.config;

import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.config.PlatformConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;


class TenantTemplateTest {

    private static final String TEMPLATE = String.join("\n",
            "resources:",
            "  datasets:",
            "    resourceType: INTERNAL_STORAGE",
            "    protocol: LOCAL",
            "    properties: { rootPath: \"/mnt/trac/storage/{tenant}\" }",
            "  trac_results:",
            "    resourceType: INTERNAL_STORAGE",
            "    protocol: LOCAL",
            "    properties: { rootPath: \"/mnt/trac/results/{tenant}\" }",
            "properties:",
            "  storage.default.location: datasets",
            "  storage.default.format: ARROW_FILE",
            "  result.storage.location: trac_results",
            "");

    private static final String TEMPLATE_WITH_SECRETS = String.join("\n",
            "resources:",
            "  datasets:",
            "    resourceType: INTERNAL_STORAGE",
            "    protocol: S3",
            "    secrets: { accessKey: \"{tenant}_key\" }",
            "");

    @TempDir
    Path tempDir;

    private ConfigManager configManager;

    @BeforeEach
    void setup() {

        var plugins = new PluginManager();
        plugins.registerExtensions();
        plugins.initConfigPlugins();

        configManager = new ConfigManager("trac-platform.yaml", tempDir, plugins);
    }

    @Test
    void loadTemplate_notConfigured() {

        var platformConfig = PlatformConfig.newBuilder().build();

        Assertions.assertNull(ConfigHelpers.loadTenantTemplate(configManager, platformConfig));
    }

    @Test
    void loadTemplate_appliedPerTenant() throws Exception {

        Files.writeString(tempDir.resolve("trac-tenant-template.yaml"), TEMPLATE);

        var platformConfig = PlatformConfig.newBuilder()
                .putConfig(ConfigKeys.TENANT_TEMPLATE_CONFIG_KEY, "trac-tenant-template.yaml")
                .build();

        var template = ConfigHelpers.loadTenantTemplate(configManager, platformConfig);
        var tenant1 = ConfigHelpers.applyTenantTemplate(template, "TENANT_1");
        var tenant2 = ConfigHelpers.applyTenantTemplate(template, "TENANT_2");

        Assertions.assertEquals("/mnt/trac/storage/TENANT_1", tenant1.getResourcesOrThrow("datasets").getPropertiesOrThrow("rootPath"));
        Assertions.assertEquals("/mnt/trac/results/TENANT_1", tenant1.getResourcesOrThrow("trac_results").getPropertiesOrThrow("rootPath"));
        Assertions.assertEquals("/mnt/trac/storage/TENANT_2", tenant2.getResourcesOrThrow("datasets").getPropertiesOrThrow("rootPath"));

        Assertions.assertEquals("LOCAL", tenant1.getResourcesOrThrow("datasets").getProtocol());
        Assertions.assertEquals("datasets", tenant1.getPropertiesOrThrow(ConfigKeys.STORAGE_DEFAULT_LOCATION));
        Assertions.assertEquals("trac_results", tenant1.getPropertiesOrThrow(ConfigKeys.RESULT_STORAGE_LOCATION));

        // The template itself is unchanged
        Assertions.assertEquals("/mnt/trac/storage/{tenant}", template.getResourcesOrThrow("datasets").getPropertiesOrThrow("rootPath"));
    }

    @Test
    void loadTemplate_secretsRejected() throws Exception {

        Files.writeString(tempDir.resolve("trac-tenant-template.yaml"), TEMPLATE_WITH_SECRETS);

        var platformConfig = PlatformConfig.newBuilder()
                .putConfig(ConfigKeys.TENANT_TEMPLATE_CONFIG_KEY, "trac-tenant-template.yaml")
                .build();

        Assertions.assertThrows(EStartup.class, () -> ConfigHelpers.loadTenantTemplate(configManager, platformConfig));
    }
}
