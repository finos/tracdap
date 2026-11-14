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

package org.finos.tracdap.svc.data.api;

import org.finos.tracdap.api.PlatformConfigWriteRequest;
import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.api.internal.PlatformConfigUpdate;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.PlatformConfigEntry;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.data.test.TestPlatformConfigExtension;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.finos.tracdap.test.meta.SampleMetadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;

import static org.finos.tracdap.svc.data.test.TestPlatformConfigExtension.TEST_CONFIG_CLASS;
import static org.junit.jupiter.api.Assertions.*;


class PlatformConfigRefreshTest {

    // No admin service, so platform config writes send no notifications
    // The data service refreshes every second, the metadata service not at all

    private static final String REFRESH_CONFIG = "config/trac-platform-config-refresh.yaml";
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(15);

    @RegisterExtension
    static final PlatformTest platform = PlatformTest.forConfig(REFRESH_CONFIG)
            .runDbDeploy(true)
            .startService(TracMetadataService.class)
            .startService(TracDataService.class)
            .build();

    @Test
    void createdEntry_appliedWithoutNotification() throws Exception {

        var entry = createEntry("createdEntry");

        var update = awaitUpdate("createdEntry", ConfigUpdateType.CREATE);

        assertEquals(entry.getConfigVersion(), update.getConfigEntry().getConfigVersion());
    }

    @Test
    void updatedAndDeletedEntry_appliedWithoutNotification() throws Exception {

        var created = createEntry("updatedAndDeletedEntry");
        awaitUpdate("updatedAndDeletedEntry", ConfigUpdateType.CREATE);

        var updated = platform.metaClientInternalBlocking().updatePlatformConfigObject(
                PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(TEST_CONFIG_CLASS)
                .setConfigKey("updatedAndDeletedEntry")
                .setPriorEntry(created)
                .setDefinition(SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG))
                .build())
                .getEntry();

        var update = awaitUpdate("updatedAndDeletedEntry", ConfigUpdateType.UPDATE);
        assertEquals(updated.getConfigVersion(), update.getConfigEntry().getConfigVersion());

        platform.metaClientInternalBlocking().deletePlatformConfigObject(
                PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(TEST_CONFIG_CLASS)
                .setConfigKey("updatedAndDeletedEntry")
                .setPriorEntry(updated)
                .build());

        var delete = awaitUpdate("updatedAndDeletedEntry", ConfigUpdateType.DELETE);
        assertTrue(delete.getConfigEntry().getConfigDeleted());
    }

    private PlatformConfigEntry createEntry(String configKey) {

        var writeRequest = PlatformConfigWriteRequest.newBuilder()
                .setConfigClass(TEST_CONFIG_CLASS)
                .setConfigKey(configKey)
                .setDefinition(SampleMetadata.dummyDefinitionForType(ObjectType.CONFIG))
                .build();

        return platform.metaClientInternalBlocking().createPlatformConfigObject(writeRequest).getEntry();
    }

    private PlatformConfigUpdate awaitUpdate(String configKey, ConfigUpdateType updateType) throws Exception {

        var deadline = Instant.now().plus(WAIT_TIMEOUT);

        while (Instant.now().isBefore(deadline)) {

            var match = TestPlatformConfigExtension.receivedUpdates().stream()
                    .filter(update -> update.getConfigEntry().getConfigKey().equals(configKey))
                    .filter(update -> update.getUpdateType() == updateType)
                    .findFirst();

            if (match.isPresent())
                return match.get();

            Thread.sleep(100);
        }

        return fail(String.format("No %s update received for [%s]", updateType, configKey));
    }
}
