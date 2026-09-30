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
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.PlatformConfigEntry;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.amin.test.TestConfigWriteHook;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.finos.tracdap.test.meta.SampleMetadata;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.regex.Pattern;

import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;
import static org.junit.jupiter.api.Assertions.*;


abstract class IdentityConfigApiTest {

    public static final String TRAC_DYNAMIC_CONFIG = "config/trac-dynamic.yaml";
    public static final String TRAC_DYNAMIC_RESOURCES = "config/trac-dynamic-resources.yaml";
    public static final String TRAC_CONFIG_ENV_VAR = "TRAC_CONFIG_FILE";

    private static final Pattern IDENTITY_KEY = Pattern.compile("\\AU[0-9A-HJKMNP-TV-Z]{6}\\Z");

    protected TracAdminApiGrpc.TracAdminApiBlockingStub adminApi;
    protected InternalMetadataApiGrpc.InternalMetadataApiBlockingStub internalApi;

    // Include this test case as a unit test
    static class UnitTest extends IdentityConfigApiTest {

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
            internalApi = platform.metaClientInternalBlocking();
        }
    }

    // Include this test case for integration against different database backends
    @org.junit.jupiter.api.Tag("integration")
    @org.junit.jupiter.api.Tag("int-metadb")
    static class IntegrationTest extends IdentityConfigApiTest {

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
            internalApi = platform.metaClientInternalBlocking();
        }
    }

    @Test
    void createMintsKey() {

        var identity = SampleMetadata.dummyIdentityDef();
        var entry = createIdentity("createMintsKey", identity);

        assertTrue(IDENTITY_KEY.matcher(entry.getConfigKey()).matches(), entry.getConfigKey());
        assertEquals(1, entry.getConfigVersion());

        var rtIdentity = internalApi.readPlatformConfigEntry(readRequest(entry));

        assertEquals(entry.getConfigKey(), rtIdentity.getEntry().getConfigKey());
        assertEquals(identity, rtIdentity.getDefinition());
    }

    @Test
    void createMintsDistinctKeys() {

        var identity = SampleMetadata.dummyIdentityDef();

        var entry1 = createIdentity("createMintsDistinctKeys", identity);
        var entry2 = createIdentity("createMintsDistinctKeys", identity);

        assertNotEquals(entry1.getConfigKey(), entry2.getConfigKey());
    }

    @Test
    void createWithKeyFails() {

        var request = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("createWithKeyFails")
                .setConfigKey("U4K7Q2X")
                .setDefinition(SampleMetadata.dummyIdentityDef())
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.createPlatformConfigObject(request));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void createOtherTypeWithoutKeyFails() {

        var request = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass("createOtherTypeWithoutKeyFails")
                .setDefinition(SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG))
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.createPlatformConfigObject(request));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void tenantConfigRejectsIdentity() {

        var request = ConfigWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setConfigClass("tenantConfigRejectsIdentity")
                .setConfigKey("entry1")
                .setDefinition(SampleMetadata.dummyIdentityDef())
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.createConfigObject(request));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void secretsMaskedOnAdminReads() {

        var identity = SampleMetadata.dummyIdentityDef();
        var entry = createIdentity("secretsMaskedOnAdminReads", identity);

        var adminRead = adminApi.readPlatformConfigObject(readRequest(entry));
        var adminBatch = adminApi.readPlatformConfigBatch(PlatformConfigReadBatchRequest.newBuilder().addEntries(entry).build());
        var internalRead = internalApi.readPlatformConfigEntry(readRequest(entry));

        assertEquals("", adminRead.getDefinition().getIdentity().getSecretsOrThrow("password"));
        assertEquals("", adminBatch.getEntries(0).getDefinition().getIdentity().getSecretsOrThrow("password"));
        assertEquals("sample_password_hash", internalRead.getDefinition().getIdentity().getSecretsOrThrow("password"));
        assertEquals(identity.getIdentity().getPropertiesMap(), adminRead.getDefinition().getIdentity().getPropertiesMap());
    }

    @Test
    void updateWithBlankSecret() {

        var identity = SampleMetadata.dummyIdentityDef();
        var entry = createIdentity("updateWithBlankSecret", identity);

        var masked = adminApi.readPlatformConfigObject(readRequest(entry)).getDefinition();
        var updated = SampleMetadata.nextIdentityDef(masked).toBuilder();
        updated.getIdentityBuilder().putSecrets("password", "");

        var response = adminApi.updatePlatformConfigObject(PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(entry.getConfigClass())
                .setConfigKey(entry.getConfigKey())
                .setPriorEntry(entry)
                .setDefinition(updated)
                .build());

        assertEquals(2, response.getEntry().getConfigVersion());
        assertEquals(entry.getConfigKey(), response.getEntry().getConfigKey());
    }

    @Test
    void updateChangingTypeFails() {

        var entry = createIdentity("updateChangingTypeFails", SampleMetadata.dummyIdentityDef());

        var request = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(entry.getConfigClass())
                .setConfigKey(entry.getConfigKey())
                .setPriorEntry(entry)
                .setDefinition(SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG))
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> adminApi.updatePlatformConfigObject(request));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @Test
    void hookSeesMintedKey() {

        TestConfigWriteHook.reset();

        var entry = createIdentity(TestConfigWriteHook.CLASS_PREFIX + "_identity", SampleMetadata.dummyIdentityDef());

        assertEquals(entry.getConfigKey(), TestConfigWriteHook.lastTransform.configKey());
        assertEquals(entry.getConfigKey(), TestConfigWriteHook.lastValidate.configKey());

        var rtIdentity = internalApi.readPlatformConfigEntry(readRequest(entry));
        assertEquals("true", rtIdentity.getDefinition().getIdentity().getPropertiesOrThrow(TestConfigWriteHook.TRANSFORMED_PROPERTY));
    }

    private PlatformConfigEntry createIdentity(String configClass, ObjectDefinition identity) {

        var request = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(configClass)
                .setDefinition(identity)
                .build();

        return adminApi.createPlatformConfigObject(request).getEntry();
    }

    private static PlatformConfigReadRequest readRequest(PlatformConfigEntry entry) {
        return PlatformConfigReadRequest.newBuilder().setEntry(entry).build();
    }
}
