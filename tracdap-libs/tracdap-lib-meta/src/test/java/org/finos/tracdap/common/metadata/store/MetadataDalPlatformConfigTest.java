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

import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.common.metadata.test.IMetadataStoreTest;
import org.finos.tracdap.common.metadata.test.JdbcIntegration;
import org.finos.tracdap.common.metadata.test.JdbcUnit;
import org.finos.tracdap.metadata.PlatformConfigEntry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;


abstract class MetadataDalPlatformConfigTest implements IMetadataStoreTest {

    private static final UserMetadata USER_1 = new UserMetadata("user1", "User One");
    private static final UserMetadata USER_2 = new UserMetadata("user2", "User Two");

    private IMetadataStore store;

    @Override
    public void setStore(IMetadataStore store) {
        this.store = store;
    }

    @ExtendWith(JdbcUnit.class)
    static class UnitTest extends MetadataDalPlatformConfigTest {}

    @Tag("integration")
    @Tag("int-metadb")
    @ExtendWith(JdbcIntegration.class)
    static class IntegrationTest extends MetadataDalPlatformConfigTest {}


    @Test
    void createStampsProvenance() {

        var t1 = testTimestamp();

        store.createPlatformConfigEntry("createStampsProvenance", "entry1", t1, USER_1, value("v1"));

        var record = store.loadPlatformConfigEntry("createStampsProvenance", "entry1", false);

        Assertions.assertEquals(t1, record.createTime());
        Assertions.assertEquals("user1", record.createUserId());
        Assertions.assertEquals("User One", record.createUserName());
        Assertions.assertEquals("user1", record.updateUserId());
        Assertions.assertEquals("User One", record.updateUserName());
    }

    @Test
    void updateCarriesCreateForward() {

        var t1 = testTimestamp();
        var t2 = t1.plusSeconds(1);

        var v1 = store.createPlatformConfigEntry("updateCarriesCreateForward", "entry1", t1, USER_1, value("v1"));
        var v2 = store.updatePlatformConfigEntry(v1, t2, USER_2, value("v2"));

        var record = store.loadPlatformConfigEntry("updateCarriesCreateForward", "entry1", false);

        Assertions.assertEquals(2, record.entry().getConfigVersion());
        Assertions.assertEquals(v2, record.entry());
        Assertions.assertEquals(t1, record.createTime());
        Assertions.assertEquals("user1", record.createUserId());
        Assertions.assertEquals("User One", record.createUserName());
        Assertions.assertEquals("user2", record.updateUserId());
        Assertions.assertEquals("User Two", record.updateUserName());
    }

    @Test
    void deleteCarriesCreateForward() {

        var t1 = testTimestamp();
        var t2 = t1.plusSeconds(1);

        var v1 = store.createPlatformConfigEntry("deleteCarriesCreateForward", "entry1", t1, USER_1, value("v1"));
        store.deletePlatformConfigEntry(v1, t2, USER_2);

        var record = store.loadPlatformConfigEntry("deleteCarriesCreateForward", "entry1", true);

        Assertions.assertTrue(record.entry().getConfigDeleted());
        Assertions.assertEquals(t1, record.createTime());
        Assertions.assertEquals("user1", record.createUserId());
        Assertions.assertEquals("user2", record.updateUserId());
    }

    @Test
    void createAfterDeleteStampsNewCreate() {

        var t1 = testTimestamp();
        var t2 = t1.plusSeconds(1);
        var t3 = t1.plusSeconds(2);

        var v1 = store.createPlatformConfigEntry("createAfterDeleteStampsNewCreate", "entry1", t1, USER_1, value("v1"));
        store.deletePlatformConfigEntry(v1, t2, USER_1);
        store.createPlatformConfigEntry("createAfterDeleteStampsNewCreate", "entry1", t3, USER_2, value("v3"));

        var record = store.loadPlatformConfigEntry("createAfterDeleteStampsNewCreate", "entry1", false);

        Assertions.assertEquals(3, record.entry().getConfigVersion());
        Assertions.assertEquals(t3, record.createTime());
        Assertions.assertEquals("user2", record.createUserId());
    }

    @Test
    void loadBatchInRequestOrder() {

        var t1 = testTimestamp();

        var e1 = store.createPlatformConfigEntry("loadBatchInRequestOrder", "entry1", t1, USER_1, value("v1"));
        var e2 = store.createPlatformConfigEntry("loadBatchInRequestOrder", "entry2", t1, USER_2, value("v2"));
        var e3 = store.createPlatformConfigEntry("loadBatchInRequestOrder_alt", "entry1", t1, USER_1, value("v3"));

        var records = store.loadPlatformConfigEntries(List.of(e3, e1, e2), false);

        Assertions.assertEquals(3, records.size());
        Assertions.assertEquals(e3, records.get(0).entry());
        Assertions.assertEquals(e1, records.get(1).entry());
        Assertions.assertEquals(e2, records.get(2).entry());
        Assertions.assertEquals("v3", text(records.get(0).value()));
        Assertions.assertEquals("v1", text(records.get(1).value()));
        Assertions.assertEquals("v2", text(records.get(2).value()));
        Assertions.assertEquals("user2", records.get(2).createUserId());
    }

    @Test
    void loadBatchReturnsLatest() {

        var t1 = testTimestamp();
        var t2 = t1.plusSeconds(1);

        var v1 = store.createPlatformConfigEntry("loadBatchReturnsLatest", "entry1", t1, USER_1, value("v1"));
        var v2 = store.updatePlatformConfigEntry(v1, t2, USER_2, value("v2"));

        var records = store.loadPlatformConfigEntries(List.of(key("loadBatchReturnsLatest", "entry1")), false);

        Assertions.assertEquals(1, records.size());
        Assertions.assertEquals(v2, records.get(0).entry());
        Assertions.assertEquals("v2", text(records.get(0).value()));
        Assertions.assertEquals("user1", records.get(0).createUserId());
        Assertions.assertEquals("user2", records.get(0).updateUserId());
    }

    @Test
    void loadBatchMissingFails() {

        var t1 = testTimestamp();

        var e1 = store.createPlatformConfigEntry("loadBatchMissingFails", "entry1", t1, USER_1, value("v1"));
        var missing = key("loadBatchMissingFails", "entry2");

        Assertions.assertThrows(EMetadataNotFound.class, () -> store.loadPlatformConfigEntries(List.of(e1, missing), false));
    }

    @Test
    void loadBatchDeleted() {

        var t1 = testTimestamp();
        var t2 = t1.plusSeconds(1);

        var e1 = store.createPlatformConfigEntry("loadBatchDeleted", "entry1", t1, USER_1, value("v1"));
        var e2 = store.createPlatformConfigEntry("loadBatchDeleted", "entry2", t1, USER_1, value("v2"));
        store.deletePlatformConfigEntry(e2, t2, USER_2);

        var keys = List.of(e1, key("loadBatchDeleted", "entry2"));

        Assertions.assertThrows(EMetadataNotFound.class, () -> store.loadPlatformConfigEntries(keys, false));

        var records = store.loadPlatformConfigEntries(keys, true);

        Assertions.assertEquals(2, records.size());
        Assertions.assertFalse(records.get(0).entry().getConfigDeleted());
        Assertions.assertTrue(records.get(1).entry().getConfigDeleted());
        Assertions.assertEquals("user2", records.get(1).updateUserId());
    }

    @Test
    void loadBatchLarge() {

        var t1 = testTimestamp();
        var count = 2500;

        var entries = new ArrayList<PlatformConfigEntry>(count);

        for (var i = 0; i < count; i++)
            entries.add(store.createPlatformConfigEntry("loadBatchLarge", "entry" + i, t1, USER_1, value("v" + i)));

        var reversed = new ArrayList<>(entries);
        java.util.Collections.reverse(reversed);

        var records = store.loadPlatformConfigEntries(reversed, false);

        Assertions.assertEquals(count, records.size());

        for (var i = 0; i < count; i++) {
            Assertions.assertEquals(reversed.get(i), records.get(i).entry());
            Assertions.assertEquals(text(value("v" + (count - 1 - i))), text(records.get(i).value()));
        }
    }

    private static Instant testTimestamp() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    private static PlatformConfigEntry key(String configClass, String configKey) {
        return PlatformConfigEntry.newBuilder().setConfigClass(configClass).setConfigKey(configKey).build();
    }

    private static byte[] value(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
