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

package org.finos.tracdap.svc.amin.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.amin.test.TestConfigWriteHook;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.helpers.PlatformTest;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.finos.tracdap.svc.amin.test.TestConfigWriteHook.*;
import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;
import static org.junit.jupiter.api.Assertions.*;


abstract class ConfigWriteHookApiTest {

    public static final String TRAC_DYNAMIC_CONFIG = "config/trac-dynamic.yaml";
    public static final String TRAC_DYNAMIC_RESOURCES = "config/trac-dynamic-resources.yaml";
    public static final String TRAC_CONFIG_ENV_VAR = "TRAC_CONFIG_FILE";

    protected TracAdminApiGrpc.TracAdminApiBlockingStub adminApi;

    // Include this test case as a unit test
    static class UnitTest extends ConfigWriteHookApiTest {

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_DYNAMIC_CONFIG)
                .runDbDeploy(true)
                .bootstrapTenant(TEST_TENANT, TRAC_DYNAMIC_RESOURCES)
                .startService(TracAdminService.class)
                .startService(TracMetadataService.class)
                .build();

        @BeforeEach
        void setup() {
            adminApi = platform.createClient(ConfigKeys.ADMIN_SERVICE_KEY, TracAdminApiGrpc::newBlockingStub);
            reset();
        }
    }

    // Include this test case for integration against different database backends
    @org.junit.jupiter.api.Tag("integration")
    @org.junit.jupiter.api.Tag("int-metadb")
    static class IntegrationTest extends ConfigWriteHookApiTest {

        private static final String TRAC_CONFIG_ENV_FILE = System.getenv(TRAC_CONFIG_ENV_VAR);

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_ENV_FILE)
                .runDbDeploy(false)
                .addTenant(TEST_TENANT)
                .startService(TracAdminService.class)
                .startService(TracMetadataService.class)
                .build();

        @BeforeEach
        void setup() {
            adminApi = platform.createClient(ConfigKeys.ADMIN_SERVICE_KEY, TracAdminApiGrpc::newBlockingStub);
            reset();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // PLATFORM PATH
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void platformTransformPersisted() {

        var configClass = CLASS_PREFIX + "_platformTransformPersisted";
        createPlatform(configClass, config());
        var entry = createPlatform(configClass, "entry2", config());

        var rtConfig = adminApi.readPlatformConfigObject(PlatformConfigReadRequest.newBuilder().setEntry(entry).build());

        assertEquals("true", rtConfig.getDefinition().getConfig().getPropertiesOrThrow(TRANSFORMED_PROPERTY));
        assertEquals(ConfigUpdateType.CREATE, lastTransform.operation());
        assertNull(lastTransform.tenant());

        // The reader sees the entry already stored, validate runs before the new one is written
        assertEquals(1, lastReaderCount);
    }

    @Test
    void platformValidateSeesPriorAndTransformed() {

        var configClass = CLASS_PREFIX + "_platformValidateSeesPriorAndTransformed";
        var entry = createPlatform(configClass, config());
        var stored = adminApi.readPlatformConfigObject(PlatformConfigReadRequest.newBuilder().setEntry(entry).build());

        var update = config().toBuilder();
        update.getConfigBuilder().putProperties("version", "2");

        adminApi.updatePlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(configClass).setConfigKey("entry1")
                .setPriorEntry(entry).setDefinition(update)
                .build());

        assertEquals(ConfigUpdateType.UPDATE, lastValidate.operation());
        assertEquals(entry.getConfigVersion(), lastValidate.priorPlatformEntry().getConfigVersion());
        assertEquals(stored.getDefinition(), lastValidate.priorDefinition());
        assertEquals("2", lastValidated.getConfig().getPropertiesOrThrow("version"));
        assertEquals("true", lastValidated.getConfig().getPropertiesOrThrow(TRANSFORMED_PROPERTY));
    }

    @Test
    void platformTransformChangingTypeFails() {

        var configClass = CLASS_PREFIX + "_platformTransformChangingTypeFails";
        var entry = createPlatform(configClass, config());

        var request = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(configClass).setConfigKey("entry1")
                .setPriorEntry(entry).setDefinition(config(CHANGE_TYPE_PROPERTY, "true"))
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.updatePlatformConfigObject(request));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @Test
    void platformStalePriorRejectedBeforeTransform() {

        var configClass = CLASS_PREFIX + "_platformStalePriorRejectedBeforeTransform";
        var entry = createPlatform(configClass, config());

        adminApi.updatePlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(configClass).setConfigKey("entry1")
                .setPriorEntry(entry).setDefinition(config())
                .build());

        reset();

        var staleUpdate = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(configClass).setConfigKey("entry1")
                .setPriorEntry(entry).setDefinition(config())
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.updatePlatformConfigObject(staleUpdate));
        assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
        assertNull(lastTransform);
    }

    @Test
    void platformRejectionsMapToStatus() {

        assertPlatformRejection("input", Status.Code.INVALID_ARGUMENT);
        assertPlatformRejection("consistency", Status.Code.FAILED_PRECONDITION);
        assertPlatformRejection("duplicate", Status.Code.ALREADY_EXISTS);
    }

    @Test
    void platformDeleteSeesPrior() {

        var configClass = CLASS_PREFIX + "_platformDeleteSeesPrior";
        var entry = createPlatform(configClass, config());

        adminApi.deletePlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(configClass).setConfigKey("entry1").setPriorEntry(entry)
                .build());

        assertEquals(ConfigUpdateType.DELETE, lastValidate.operation());
        assertNotNull(lastValidate.priorDefinition());
        assertNull(lastValidated);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // TENANT PATH
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void tenantTransformPersisted() {

        var configClass = CLASS_PREFIX + "_tenantTransformPersisted";
        createTenant(configClass, config());
        var entry = createTenant(configClass, "entry2", config());

        var rtConfig = adminApi.readConfigObject(ConfigReadRequest.newBuilder()
                .setTenant(TEST_TENANT).setEntry(entry)
                .build());

        assertEquals("true", rtConfig.getDefinition().getConfig().getPropertiesOrThrow(TRANSFORMED_PROPERTY));
        assertEquals(ConfigUpdateType.CREATE, lastTransform.operation());
        assertEquals(TEST_TENANT, lastTransform.tenant());

        // The reader sees the entry already stored, validate runs before the new one is written
        assertEquals(1, lastReaderCount);
    }

    @Test
    void tenantValidateSeesPriorAndTransformed() {

        var configClass = CLASS_PREFIX + "_tenantValidateSeesPriorAndTransformed";
        var entry = createTenant(configClass, config());

        adminApi.updateConfigObject(ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT).setConfigClass(configClass).setConfigKey("entry1")
                .setPriorEntry(entry).setDefinition(config("version", "2"))
                .build());

        assertEquals(ConfigUpdateType.UPDATE, lastValidate.operation());
        assertEquals(entry.getConfigVersion(), lastValidate.priorEntry().getConfigVersion());
        assertEquals("true", lastValidate.priorDefinition().getConfig().getPropertiesOrThrow(TRANSFORMED_PROPERTY));
        assertEquals("2", lastValidated.getConfig().getPropertiesOrThrow("version"));
    }

    @Test
    void tenantTransformChangingTypeFails() {

        var configClass = CLASS_PREFIX + "_tenantTransformChangingTypeFails";
        var entry = createTenant(configClass, config());

        var request = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT).setConfigClass(configClass).setConfigKey("entry1")
                .setPriorEntry(entry).setDefinition(config(CHANGE_TYPE_PROPERTY, "true"))
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.updateConfigObject(request));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @Test
    void tenantRejectionMapsToStatus() {

        var request = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass(CLASS_PREFIX + "_tenantRejectionMapsToStatus").setConfigKey("entry1")
                .setDefinition(config(REJECT_PROPERTY, "consistency"))
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.createConfigObject(request));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @Test
    void tenantDeleteSeesPriorObject() {

        var configClass = CLASS_PREFIX + "_tenantDeleteSeesPriorObject";
        var entry = createTenant(configClass, config());

        adminApi.deleteConfigObject(ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT).setConfigClass(configClass).setConfigKey("entry1").setPriorEntry(entry)
                .build());

        assertEquals(ConfigUpdateType.DELETE, lastValidate.operation());
        assertEquals("true", lastValidate.priorDefinition().getConfig().getPropertiesOrThrow(TRANSFORMED_PROPERTY));
        assertNull(lastValidated);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS
    // -----------------------------------------------------------------------------------------------------------------

    private void assertPlatformRejection(String reject, Status.Code expectedCode) {

        var request = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(CLASS_PREFIX + "_platformRejectionsMapToStatus").setConfigKey(reject)
                .setDefinition(config(REJECT_PROPERTY, reject))
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.createPlatformConfigObject(request));
        assertEquals(expectedCode, error.getStatus().getCode());
    }

    private PlatformConfigEntry createPlatform(String configClass, ObjectDefinition definition) {
        return createPlatform(configClass, "entry1", definition);
    }

    private PlatformConfigEntry createPlatform(String configClass, String configKey, ObjectDefinition definition) {

        return adminApi.createPlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(configClass).setConfigKey(configKey).setDefinition(definition)
                .build()).getEntry();
    }

    private ConfigEntry createTenant(String configClass, ObjectDefinition definition) {
        return createTenant(configClass, "entry1", definition);
    }

    private ConfigEntry createTenant(String configClass, String configKey, ObjectDefinition definition) {

        return adminApi.createConfigObject(ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT).setConfigClass(configClass).setConfigKey(configKey).setDefinition(definition)
                .build()).getEntry();
    }

    private static ObjectDefinition config() {
        return config("prop", "value");
    }

    private static ObjectDefinition config(String key, String value) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.CONFIG)
                .setConfig(ConfigDefinition.newBuilder()
                .setConfigType(ConfigType.PROPERTIES)
                .putProperties(key, value))
                .build();
    }
}
