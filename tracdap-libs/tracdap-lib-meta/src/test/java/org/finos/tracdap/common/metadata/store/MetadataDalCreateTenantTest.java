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
package org.finos.tracdap.common.metadata.store;

import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.exception.ETenantNotFound;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.test.IMetadataStoreTest;
import org.finos.tracdap.common.metadata.test.JdbcIntegration;
import org.finos.tracdap.common.metadata.test.JdbcUnit;
import org.finos.tracdap.metadata.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.finos.tracdap.test.meta.SampleMetadata.*;


abstract class MetadataDalCreateTenantTest implements IMetadataStoreTest {

    private IMetadataStore store;

    public void setStore(IMetadataStore store) {
        this.store = store;
    }

    @ExtendWith(JdbcUnit.class)
    static class UnitTest extends MetadataDalCreateTenantTest {
    }

    @Tag("integration")
    @Tag("int-metadb")
    @ExtendWith(JdbcIntegration.class)
    static class IntegrationTest extends MetadataDalCreateTenantTest {
    }

    @Test
    void createTenant_ok() {

        var tenantCode = newTenantCode();
        var tenantInfo = tenantInfo(tenantCode);

        var resource = dummyTag(dummyResourceDef(), true);
        var config = dummyTag(dummyConfigDef(), true);
        var resourceEntry = configEntry("trac_resources", "storage1", resource);
        var configEntry = configEntry("trac_config", "trac_tenant_config", config);

        var batch = new MetadataBatchUpdate(
                null, null, List.of(resource, config), null, null,
                List.of(resourceEntry, configEntry));

        store.createTenant(tenantInfo, batch);

        var tenants = store.listTenants();
        Assertions.assertTrue(tenants.contains(tenantInfo));

        var resourceEntries = store.listConfigEntries(tenantCode, "trac_resources", false);
        var configEntries = store.listConfigEntries(tenantCode, "trac_config", false);
        Assertions.assertEquals(List.of(resourceEntry), resourceEntries);
        Assertions.assertEquals(List.of(configEntry), configEntries);

        var objects = store.loadObjects(tenantCode, List.of(selectorForTag(resource), selectorForTag(config)));
        Assertions.assertEquals(resource.getDefinition(), objects.get(0).getDefinition());
        Assertions.assertEquals(config.getDefinition(), objects.get(1).getDefinition());
    }

    @Test
    void createTenant_duplicate() {

        var tenantCode = newTenantCode();
        var emptyBatch = new MetadataBatchUpdate(null, null, List.of(), null, null, List.of());

        store.createTenant(tenantInfo(tenantCode), emptyBatch);

        Assertions.assertThrows(EMetadataDuplicate.class, () -> store.createTenant(tenantInfo(tenantCode), emptyBatch));
        Assertions.assertThrows(EMetadataDuplicate.class, () -> store.createTenant(tenantInfo(TEST_TENANT), emptyBatch));
    }

    @Test
    void createTenant_failureLeavesNothing() {

        var tenantCode = newTenantCode();
        var config = dummyTag(dummyConfigDef(), true);
        var entry = configEntry("trac_config", "trac_tenant_config", config);

        // The same config entry twice fails on the second insert, after the tenant row is written
        var badBatch = new MetadataBatchUpdate(null, null, List.of(config), null, null, List.of(entry, entry));

        Assertions.assertThrows(EMetadataDuplicate.class, () -> store.createTenant(tenantInfo(tenantCode), badBatch));

        var tenantCodes = store.listTenants().stream().map(TenantInfo::getTenantCode).collect(Collectors.toList());
        Assertions.assertFalse(tenantCodes.contains(tenantCode));

        Assertions.assertThrows(ETenantNotFound.class, () -> store.listConfigEntries(tenantCode, "trac_config", false));
    }

    @Test
    void createTenant_retryAfterFailure() {

        var tenantCode = newTenantCode();
        var otherTenantCode = newTenantCode();

        var config = dummyTag(dummyConfigDef(), true);
        var entry = configEntry("trac_config", "trac_tenant_config", config);
        var badBatch = new MetadataBatchUpdate(null, null, List.of(config), null, null, List.of(entry, entry));

        Assertions.assertThrows(EMetadataDuplicate.class, () -> store.createTenant(tenantInfo(tenantCode), badBatch));

        // Another tenant takes the tenant ID the failed create had allocated
        var otherConfig = dummyTag(dummyConfigDef(), true);
        var otherEntry = configEntry("trac_config", "other_config", otherConfig);
        var otherBatch = new MetadataBatchUpdate(null, null, List.of(otherConfig), null, null, List.of(otherEntry));
        store.createTenant(tenantInfo(otherTenantCode), otherBatch);

        // The retry must write under its own new tenant ID
        var retryConfig = dummyTag(dummyConfigDef(), true);
        var retryEntry = configEntry("trac_config", "trac_tenant_config", retryConfig);
        var retryBatch = new MetadataBatchUpdate(null, null, List.of(retryConfig), null, null, List.of(retryEntry));
        store.createTenant(tenantInfo(tenantCode), retryBatch);

        // Tenant config written after creation also goes to the right tenant
        var laterConfig = dummyTag(dummyConfigDef(), true);
        var laterEntry = configEntry("trac_resources", "storage1", laterConfig);
        store.saveBatchUpdate(tenantCode, new MetadataBatchUpdate(null, null, List.of(laterConfig), null, null, List.of(laterEntry)));

        Assertions.assertEquals(List.of(retryEntry), store.listConfigEntries(tenantCode, "trac_config", false));
        Assertions.assertEquals(List.of(laterEntry), store.listConfigEntries(tenantCode, "trac_resources", false));
        Assertions.assertEquals(List.of(otherEntry), store.listConfigEntries(otherTenantCode, "trac_config", false));
        Assertions.assertEquals(List.of(), store.listConfigEntries(otherTenantCode, "trac_resources", false));
    }

    private static String newTenantCode() {

        return "CT_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
    }

    private static TenantInfo tenantInfo(String tenantCode) {

        return TenantInfo.newBuilder()
                .setTenantCode(tenantCode)
                .setDescription("Tenant " + tenantCode)
                .build();
    }

    private static ConfigEntry configEntry(String configClass, String configKey, org.finos.tracdap.metadata.Tag configObject) {

        var selector = selectorForTag(configObject);

        var details = ConfigDetails.newBuilder()
                .setObjectSelector(selector)
                .setObjectType(configObject.getDefinition().getObjectType());

        if (configObject.getDefinition().getObjectType() == ObjectType.CONFIG)
            details.setConfigType(configObject.getDefinition().getConfig().getConfigType());

        if (configObject.getDefinition().getObjectType() == ObjectType.RESOURCE)
            details.setResourceType(configObject.getDefinition().getResource().getResourceType());

        return ConfigEntry.newBuilder()
                .setConfigClass(configClass)
                .setConfigKey(configKey)
                .setConfigVersion(1)
                .setConfigTimestamp(MetadataCodec.encodeDatetime(Instant.now()))
                .setIsLatestConfig(true)
                .setDetails(details)
                .build();
    }
}
