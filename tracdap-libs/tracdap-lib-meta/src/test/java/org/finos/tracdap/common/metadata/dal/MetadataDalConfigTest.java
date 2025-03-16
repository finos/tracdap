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

package org.finos.tracdap.common.metadata.dal;

import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.test.IDalTestable;
import org.finos.tracdap.common.metadata.test.JdbcIntegration;
import org.finos.tracdap.common.metadata.test.JdbcUnit;
import org.finos.tracdap.metadata.ConfigDetails;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.TagSelector;

import org.finos.tracdap.test.meta.SampleMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Instant;
import java.util.List;
import java.util.UUID;


abstract class MetadataDalConfigTest implements IDalTestable {

    private static final String TEST_TENANT = SampleMetadata.TEST_TENANT;
    private static final String ALT_TEST_TENANT = SampleMetadata.ALT_TEST_TENANT;

    private IMetadataDal dal;

    @Override
    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }

    @ExtendWith(JdbcUnit.class)
    static class UnitTest extends MetadataDalConfigTest {}

    @Tag("integration")
    @Tag("int-metadb")
    @ExtendWith(JdbcIntegration.class)
    static class IntegrationTest extends MetadataDalConfigTest {}


    // -----------------------------------------------------------------------------------------------------------------
    // SAVE CONFIG ENTRIES
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testSaveConfigEntries_ok() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_ok")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        var entry2 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_ok")
                .setConfigKey("entry2")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector2)
                .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1, entry2));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var key2 = ConfigEntry.newBuilder()
                .setConfigClass(entry2.getConfigClass())
                .setConfigKey(entry2.getConfigKey())
                .setConfigVersion(entry2.getConfigVersion())
                .build();

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1, key2));

        Assertions.assertEquals(List.of(entry1, entry2), savedEntries);
    }

    @Test
    void testSaveConfigEntries_duplicate() {

        // Insert a duplicate config key on version 1

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_duplicate")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        Assertions.assertThrows(EMetadataDuplicate.class, () -> dal.saveConfigEntries(TEST_TENANT, List.of(entry1)));
    }

    @Test
    void testSaveConfigEntries_updateOk() {

        // Insert an entry, then update it

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_updateOk")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1v2 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_updateOk")
                .setConfigKey("entry1")
                .setConfigVersion(2)
                .setConfigTimestamp(testTimestamp2)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector2)
                        .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1v2));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var key1v2 = ConfigEntry.newBuilder()
                .setConfigClass(entry1v2.getConfigClass())
                .setConfigKey(entry1v2.getConfigKey())
                .setConfigVersion(entry1v2.getConfigVersion())
                .build();

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1, key1v2));

        var entry1Superseded = entry1.toBuilder()
                .clearIsLatestConfig()
                .build();

        Assertions.assertEquals(List.of(entry1Superseded, entry1v2), savedEntries);
    }

    @Test
    void testSaveConfigEntries_updateDuplicate() {

        // Insert an entry, then update it
        // Then try to insert the same update again

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_updateDuplicate")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1v2 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_updateDuplicate")
                .setConfigKey("entry1")
                .setConfigVersion(2)
                .setConfigTimestamp(testTimestamp2)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector2)
                        .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1v2));

        Assertions.assertThrows(EMetadataDuplicate.class, () -> dal.saveConfigEntries(TEST_TENANT, List.of(entry1v2)));
    }

    @Test
    void testSaveConfigEntries_updateMissingVersion() {

        // Try to insert v2 before v1 (fails)
        // Once v2 is inserted, v2 can be inserted successfully

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_missingVersion")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        var entry1v2 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_missingVersion")
                .setConfigKey("entry1")
                .setConfigVersion(2)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        Assertions.assertThrows(EMetadataNotFound.class, () -> dal.saveConfigEntries(TEST_TENANT, List.of(entry1v2)));

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));
        dal.saveConfigEntries(TEST_TENANT, List.of(entry1v2));

        var key1v2 = ConfigEntry.newBuilder()
                .setConfigClass(entry1v2.getConfigClass())
                .setConfigKey(entry1v2.getConfigKey())
                .setConfigVersion(entry1v2.getConfigVersion())
                .build();

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1v2));

        Assertions.assertEquals(List.of(entry1v2), savedEntries);
    }

    @Test
    void testSaveConfigEntries_updateAfterDelete() {

        // Insert an entry, then update it

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_updateAfterDelete")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());

        var entry1v2 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_updateAfterDelete")
                .setConfigKey("entry1")
                .setConfigVersion(2)
                .setConfigTimestamp(testTimestamp2)
                .setIsLatestConfig(true)
                .setConfigDeleted(true)
                .setDetails(ConfigDetails.getDefaultInstance())
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1v2));

        var key1v2 = ConfigEntry.newBuilder()
                .setConfigClass(entry1v2.getConfigClass())
                .setConfigKey(entry1v2.getConfigKey())
                .setConfigVersion(entry1v2.getConfigVersion())
                .build();

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1v2));

        Assertions.assertEquals(List.of(entry1v2), savedEntries);

        var testTimestamp3 = MetadataCodec.encodeDatetime(Instant.now());

        var entry1v3 = ConfigEntry.newBuilder()
                .setConfigClass("testSaveConfigEntries_updateAfterDelete")
                .setConfigKey("entry1")
                .setConfigVersion(3)
                .setConfigTimestamp(testTimestamp3)
                .setIsLatestConfig(true)
                .setConfigDeleted(false)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1v3));

        var key1v3 = ConfigEntry.newBuilder()
                .setConfigClass(entry1v3.getConfigClass())
                .setConfigKey(entry1v3.getConfigKey())
                .setConfigVersion(entry1v3.getConfigVersion())
                .build();

        var savedEntriesV3 = dal.loadConfigEntries(TEST_TENANT, List.of(key1v3));

        Assertions.assertEquals(List.of(entry1v3), savedEntriesV3);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // LOAD CONFIG ENTRIES
    // -----------------------------------------------------------------------------------------------------------------


    @Test
    void testLoadConfigEntries_ok() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_ok")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        var entry2 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_ok")
                .setConfigKey("entry2")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector2)
                        .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1, entry2));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var key2 = ConfigEntry.newBuilder()
                .setConfigClass(entry2.getConfigClass())
                .setConfigKey(entry2.getConfigKey())
                .setConfigVersion(entry2.getConfigVersion())
                .build();

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1, key2));

        Assertions.assertEquals(List.of(entry1, entry2), savedEntries);
    }

    @Test
    void testLoadConfigEntries_versions() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_versions")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        var entry2 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_versions")
                .setConfigKey("entry2")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector2)
                .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1, entry2));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());

        var selector1V2 = selector1.toBuilder().setObjectVersion(2).build();
        var selector2V2 = selector2.toBuilder().setObjectVersion(2).build();

        var entry1V2 = entry1.toBuilder().setDetails(entry1.getDetails().toBuilder().setObjectSelector(selector1V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();
        var entry2V2 = entry2.toBuilder().setDetails(entry2.getDetails().toBuilder().setObjectSelector(selector2V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1V2, entry2V2));

        var key1v1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var key2v2 = ConfigEntry.newBuilder()
                .setConfigClass(entry2V2.getConfigClass())
                .setConfigKey(entry2V2.getConfigKey())
                .setConfigVersion(entry2V2.getConfigVersion())
                .build();
        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1v1, key2v2));

        var entry1Superseded = entry1.toBuilder().setIsLatestConfig(false).build();

        Assertions.assertEquals(List.of(entry1Superseded, entry2V2), savedEntries);
    }

    @Test
    void testLoadConfigEntries_asOf() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_asOf")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        var entry2 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_asOf")
                .setConfigKey("entry2")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector2)
                        .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1, entry2));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());
        var selector1V2 = selector1.toBuilder().setObjectVersion(2).build();
        var selector2V2 = selector2.toBuilder().setObjectVersion(2).build();
        var entry1V2 = entry1.toBuilder().setDetails(entry1.getDetails().toBuilder().setObjectSelector(selector1V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();
        var entry2V2 = entry2.toBuilder().setDetails(entry2.getDetails().toBuilder().setObjectSelector(selector2V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1V2, entry2V2));

        var key1v1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigTimestamp(testTimestamp)
                .build();

        var key2v2 = ConfigEntry.newBuilder()
                .setConfigClass(entry2V2.getConfigClass())
                .setConfigKey(entry2V2.getConfigKey())
                .setConfigTimestamp(testTimestamp2)
                .build();

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1v1, key2v2));

        var entry1Superseded = entry1.toBuilder().setIsLatestConfig(false).build();

        Assertions.assertEquals(List.of(entry1Superseded, entry2V2), savedEntries);
    }

    @Test
    void testLoadConfigEntries_latest() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_latest")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        var entry2 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_latest")
                .setConfigKey("entry2")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector2)
                        .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1, entry2));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setIsLatestConfig(true)
                .build();

        var key2 = ConfigEntry.newBuilder()
                .setConfigClass(entry2.getConfigClass())
                .setConfigKey(entry2.getConfigKey())
                .setIsLatestConfig(true)
                .build();

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1, key2));

        Assertions.assertEquals(List.of(entry1, entry2), savedEntries);

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());
        var selector1V2 = selector1.toBuilder().setObjectVersion(2).build();
        var entry1V2 = entry1.toBuilder().setDetails(entry1.getDetails().toBuilder().setObjectSelector(selector1V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1V2));

        var savedEntries2 = dal.loadConfigEntries(TEST_TENANT, List.of(key1, key2));

        Assertions.assertEquals(List.of(entry1V2, entry2), savedEntries2);

        var testTimestamp3 = MetadataCodec.encodeDatetime(Instant.now());
        var selector2V2 = selector2.toBuilder().setObjectVersion(2).build();
        var entry2V2 = entry2.toBuilder().setDetails(entry2.getDetails().toBuilder().setObjectSelector(selector2V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp3).build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry2V2));

        var savedEntries3 = dal.loadConfigEntries(TEST_TENANT, List.of(key1, key2));

        Assertions.assertEquals(List.of(entry1V2, entry2V2), savedEntries3);
    }

    @Test
    void testLoadConfigEntries_mixed() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_mixed")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        var entry2 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_mixed")
                .setConfigKey("entry2")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector2)
                        .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1, entry2));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());

        var selector1V2 = selector1.toBuilder().setObjectVersion(2).build();
        var selector2V2 = selector2.toBuilder().setObjectVersion(2).build();

        var entry1V2 = entry1.toBuilder().setDetails(entry1.getDetails().toBuilder().setObjectSelector(selector1V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();
        var entry2V2 = entry2.toBuilder().setDetails(entry2.getDetails().toBuilder().setObjectSelector(selector2V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1V2, entry2V2));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var key2 = ConfigEntry.newBuilder()
                .setConfigClass(entry2V2.getConfigClass())
                .setConfigKey(entry2V2.getConfigKey())
                .setIsLatestConfig(true)
                .build();

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1, key2));
        var entry1Superseded = entry1.toBuilder().setIsLatestConfig(false).build();
        Assertions.assertEquals(List.of(entry1Superseded, entry2V2), savedEntries);

        var key1AsOf = key1.toBuilder().clearIsLatestConfig().setConfigTimestamp(testTimestamp).build();
        var savedEntries2 = dal.loadConfigEntries(TEST_TENANT, List.of(key1AsOf, key2));
        Assertions.assertEquals(List.of(entry1Superseded, entry2V2), savedEntries2);
    }

    @Test
    void testLoadConfigEntries_multiCriteria() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_multiCriteria")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());
        var selector1V2 = selector1.toBuilder().setObjectVersion(2).build();
        var entry1V2 = entry1.toBuilder().setDetails(entry1.getDetails().toBuilder().setObjectSelector(selector1V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1V2));

        // All criteria match - return a single entry

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1V2.getConfigVersion())
                .setConfigTimestamp(entry1V2.getConfigTimestamp())
                .setIsLatestConfig(true)
                .build();

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1));

        Assertions.assertEquals(List.of(entry1V2), savedEntries);

        // Criteria do not all match the same version - should fail to match and throw an error

        var key1Alt = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(1)
                .setConfigTimestamp(entry1V2.getConfigTimestamp())
                .setIsLatestConfig(true)
                .build();

        Assertions.assertThrows(EMetadataNotFound.class, () -> dal.loadConfigEntries(TEST_TENANT, List.of(key1Alt)));
    }

    @Test
    void testLoadConfigEntries_noCriteria() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_noCriteria")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var badKey = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .build();

        Assertions.assertThrows(EMetadataNotFound.class, () -> dal.loadConfigEntries(TEST_TENANT, List.of(badKey)));
    }

    @Test
    void testLoadConfigEntries_deleted() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_deleted")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());

        var entry1v2 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_deleted")
                .setConfigKey("entry1")
                .setConfigVersion(2)
                .setConfigTimestamp(testTimestamp2)
                .setIsLatestConfig(true)
                .setConfigDeleted(true)
                .setDetails(ConfigDetails.getDefaultInstance())
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1v2));

        var key1v2 = ConfigEntry.newBuilder()
                .setConfigClass(entry1v2.getConfigClass())
                .setConfigKey(entry1v2.getConfigKey())
                .setConfigVersion(entry1v2.getConfigVersion())
                .build();

        // Read using fixed version
        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1v2));
        Assertions.assertEquals(List.of(entry1v2), savedEntries);
        Assertions.assertTrue(savedEntries.get(0).getConfigDeleted());

        var key1Latest = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setIsLatestConfig(true)
                .build();

        // Read using latest flag
        var savedEntries2 = dal.loadConfigEntries(TEST_TENANT, List.of(key1Latest));
        Assertions.assertEquals(List.of(entry1v2), savedEntries2);
        Assertions.assertTrue(savedEntries2.get(0).getConfigDeleted());
    }

    @Test
    void testLoadConfigEntries_missingEntry() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_missingEntry")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1));
        Assertions.assertEquals(List.of(entry1), savedEntries);

        var missingKey = key1.toBuilder().setConfigKey("missing_key").build();

        Assertions.assertThrows(EMetadataNotFound.class, () -> dal.loadConfigEntries(TEST_TENANT, List.of(missingKey)));
    }

    @Test
    void testLoadConfigEntries_missingVersion() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntries_missingVersion")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1));
        Assertions.assertEquals(List.of(entry1), savedEntries);

        var missingKey = key1.toBuilder().setConfigVersion(2).build();

        Assertions.assertThrows(EMetadataNotFound.class, () -> dal.loadConfigEntries(TEST_TENANT, List.of(missingKey)));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // LOAD CONFIG ENTRY (SINGLE ITEM)
    // -----------------------------------------------------------------------------------------------------------------


    @Test
    void testLoadConfigEntry_ok() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntry_ok")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var savedEntry = dal.loadConfigEntry(TEST_TENANT, key1);

        Assertions.assertEquals(entry1, savedEntry);
    }

    @Test
    void testLoadConfigEntry_versions() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntry_versions")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());
        var selector1V2 = selector1.toBuilder().setObjectVersion(2).build();
        var entry1V2 = entry1.toBuilder().setDetails(entry1.getDetails().toBuilder().setObjectSelector(selector1V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1V2));

        var key1v1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var key1v2 = ConfigEntry.newBuilder()
                .setConfigClass(entry1V2.getConfigClass())
                .setConfigKey(entry1V2.getConfigKey())
                .setConfigVersion(entry1V2.getConfigVersion())
                .build();

        var savedEntry1 = dal.loadConfigEntry(TEST_TENANT, key1v1);
        var savedEntry2 = dal.loadConfigEntry(TEST_TENANT, key1v2);

        var entry1Superseded = entry1.toBuilder().setIsLatestConfig(false).build();

        Assertions.assertEquals(entry1Superseded, savedEntry1);
        Assertions.assertEquals(entry1V2, savedEntry2);
    }

    @Test
    void testLoadConfigEntry_asOf() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntry_asOf")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());
        var selector1V2 = selector1.toBuilder().setObjectVersion(2).build();
        var entry1V2 = entry1.toBuilder().setDetails(entry1.getDetails().toBuilder().setObjectSelector(selector1V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1V2));

        var key1v1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigTimestamp(testTimestamp)
                .build();

        var key1v2 = ConfigEntry.newBuilder()
                .setConfigClass(entry1V2.getConfigClass())
                .setConfigKey(entry1V2.getConfigKey())
                .setConfigTimestamp(testTimestamp2)
                .build();

        var savedEntry1 = dal.loadConfigEntry(TEST_TENANT, key1v1);
        var savedEntry2 = dal.loadConfigEntry(TEST_TENANT, key1v2);

        var entry1Superseded = entry1.toBuilder().setIsLatestConfig(false).build();

        Assertions.assertEquals(entry1Superseded, savedEntry1);
        Assertions.assertEquals(entry1V2, savedEntry2);
    }

    @Test
    void testLoadConfigEntry_latest() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntry_latest")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setIsLatestConfig(true)
                .build();

        var savedEntry1 = dal.loadConfigEntry(TEST_TENANT, key1);

        Assertions.assertEquals(entry1, savedEntry1);

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());
        var selector1V2 = selector1.toBuilder().setObjectVersion(2).build();
        var entry1V2 = entry1.toBuilder().setDetails(entry1.getDetails().toBuilder().setObjectSelector(selector1V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1V2));

        var savedEntry2 = dal.loadConfigEntry(TEST_TENANT, key1);

        Assertions.assertEquals(entry1V2, savedEntry2);
    }

    @Test
    void testLoadConfigEntry_multiCriteria() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntry_multiCriteria")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());
        var selector1V2 = selector1.toBuilder().setObjectVersion(2).build();
        var entry1V2 = entry1.toBuilder().setDetails(entry1.getDetails().toBuilder().setObjectSelector(selector1V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1V2));

        // All criteria match - return a single entry

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1V2.getConfigVersion())
                .setConfigTimestamp(entry1V2.getConfigTimestamp())
                .setIsLatestConfig(true)
                .build();

        var savedEntry = dal.loadConfigEntry(TEST_TENANT, key1);

        Assertions.assertEquals(entry1V2, savedEntry);

        // Criteria do not all match the same version - nothing should be returned

        var key1Alt = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(1)
                .setConfigTimestamp(entry1V2.getConfigTimestamp())
                .setIsLatestConfig(true)
                .build();

        Assertions.assertThrows(EMetadataNotFound.class, () -> dal.loadConfigEntry(TEST_TENANT, key1Alt));
    }

    @Test
    void testLoadConfigEntry_noCriteria() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntry_noCriteria")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var badKey = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .build();

        Assertions.assertThrows(EMetadataNotFound.class, () -> dal.loadConfigEntry(TEST_TENANT, badKey));
    }

    @Test
    void testLoadConfigEntry_deleted() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntry_deleted")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());

        var entry1v2 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntry_deleted")
                .setConfigKey("entry1")
                .setConfigVersion(2)
                .setConfigTimestamp(testTimestamp2)
                .setIsLatestConfig(true)
                .setConfigDeleted(true)
                .setDetails(ConfigDetails.getDefaultInstance())
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1v2));

        var key1v2 = ConfigEntry.newBuilder()
                .setConfigClass(entry1v2.getConfigClass())
                .setConfigKey(entry1v2.getConfigKey())
                .setConfigVersion(entry1v2.getConfigVersion())
                .build();

        // Read using fixed version
        var savedEntry = dal.loadConfigEntry(TEST_TENANT, key1v2);
        Assertions.assertEquals(entry1v2, savedEntry);
        Assertions.assertTrue(savedEntry.getConfigDeleted());

        var key1Latest = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setIsLatestConfig(true)
                .build();

        // Read using latest flag
        var savedEntry2 = dal.loadConfigEntry(TEST_TENANT, key1Latest);
        Assertions.assertEquals(entry1v2, savedEntry2);
        Assertions.assertTrue(savedEntry2.getConfigDeleted());
    }

    @Test
    void testLoadConfigEntry_missingEntry() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntry_missingEntry")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var savedEntry = dal.loadConfigEntry(TEST_TENANT, key1);
        Assertions.assertEquals(entry1, savedEntry);

        var missingKey = key1.toBuilder().setConfigKey("missing_key").build();

        Assertions.assertThrows(EMetadataNotFound.class, () -> dal.loadConfigEntry(TEST_TENANT, missingKey));
    }

    @Test
    void testLoadConfigEntry_missingVersion() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testLoadConfigEntry_missingVersion")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var savedEntry = dal.loadConfigEntry(TEST_TENANT, key1);
        Assertions.assertEquals(entry1, savedEntry);

        var missingKey = key1.toBuilder().setConfigVersion(2).build();

        Assertions.assertThrows(EMetadataNotFound.class, () -> dal.loadConfigEntry(TEST_TENANT, missingKey));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // LIST CONFIG ENTRIES
    // -----------------------------------------------------------------------------------------------------------------


    @Test
    void testListConfigEntries_ok() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testListConfigEntries_ok")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        var entry2 = ConfigEntry.newBuilder()
                .setConfigClass("testListConfigEntries_ok")
                .setConfigKey("entry2")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector2)
                .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1, entry2));

        var entries = dal.listConfigEntries(TEST_TENANT, "testListConfigEntries_ok", false);

        Assertions.assertEquals(2, entries.size());
        Assertions.assertTrue(entries.stream().anyMatch(e -> e.getConfigKey().equals("entry1")));
        Assertions.assertTrue(entries.stream().anyMatch(e -> e.getConfigKey().equals("entry2")));
    }

    @Test
    void testListConfigEntries_versions() {

        // Entries still listed correctly after version updates
        // Latest version is returned in the listing

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testListConfigEntries_versions")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        var entry2 = ConfigEntry.newBuilder()
                .setConfigClass("testListConfigEntries_versions")
                .setConfigKey("entry2")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector2)
                        .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1, entry2));

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());
        var selector1V2 = selector1.toBuilder().setObjectVersion(2).build();
        var entry1V2 = entry1.toBuilder().setDetails(entry1.getDetails().toBuilder().setObjectSelector(selector1V2)).setConfigVersion(2).setConfigTimestamp(testTimestamp2).build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1V2));

        var entries = dal.listConfigEntries(TEST_TENANT, "testListConfigEntries_versions", false);

        Assertions.assertEquals(2, entries.size());
        Assertions.assertTrue(entries.stream().anyMatch(e -> e.getConfigKey().equals("entry1")));
        Assertions.assertTrue(entries.stream().anyMatch(e -> e.getConfigKey().equals("entry2")));

        var updatedEntry = entries.stream().filter(e -> e.getConfigKey().equals("entry1")).findFirst();
        Assertions.assertTrue(updatedEntry.isPresent());
        Assertions.assertEquals(2, updatedEntry.get().getConfigVersion());
    }

    @Test
    void testListConfigEntries_separateClasses() {

        // List entries respects config class

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testListConfigEntries_separateClasses")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        var entry2 = ConfigEntry.newBuilder()
                .setConfigClass("testListConfigEntries_separateClasses_alt")
                .setConfigKey("entry2")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector2)
                .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1, entry2));

        var entries = dal.listConfigEntries(TEST_TENANT, "testListConfigEntries_separateClasses", false);

        Assertions.assertEquals(1, entries.size());
        Assertions.assertTrue(entries.stream().anyMatch(e -> e.getConfigKey().equals("entry1")));
    }

    @Test
    void testListConfigEntries_deleted() {

        // Include deleted flag working correctly

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testListConfigEntries_deleted")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1)
                        .setObjectType(selector1.getObjectType()))
                .build();

        var entry2 = ConfigEntry.newBuilder()
                .setConfigClass("testListConfigEntries_deleted")
                .setConfigKey("entry2")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector2)
                        .setObjectType(selector2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1, entry2));

        var entriesBeforeDeleting = dal.listConfigEntries(TEST_TENANT, "testListConfigEntries_deleted", false);

        Assertions.assertEquals(2, entriesBeforeDeleting.size());

        var testTimestamp2 = MetadataCodec.encodeDatetime(Instant.now());

        var entry1v2 = ConfigEntry.newBuilder()
                .setConfigClass("testListConfigEntries_deleted")
                .setConfigKey("entry1")
                .setConfigVersion(2)
                .setConfigTimestamp(testTimestamp2)
                .setIsLatestConfig(true)
                .setConfigDeleted(true)
                .setDetails(ConfigDetails.getDefaultInstance())
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1v2));

        var entries = dal.listConfigEntries(TEST_TENANT, "testListConfigEntries_deleted", false);

        Assertions.assertEquals(1, entries.size());
        Assertions.assertTrue(entries.stream().anyMatch(e -> e.getConfigKey().equals("entry2")));

        var entriesIncludingDeleted = dal.listConfigEntries(TEST_TENANT, "testListConfigEntries_deleted", true);

        Assertions.assertEquals(2, entriesIncludingDeleted.size());
        Assertions.assertTrue(entriesIncludingDeleted.stream().anyMatch(e -> e.getConfigKey().equals("entry1")));
        Assertions.assertTrue(entriesIncludingDeleted.stream().anyMatch(e -> e.getConfigKey().equals("entry2")));
    }

    @Test
    void testListConfigEntries_unknown() {

        // List entries respects config class

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testListConfigEntries_unknown")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));

        Assertions.assertThrows(EMetadataNotFound.class, () -> dal.listConfigEntries(TEST_TENANT, "testListConfigEntries_unknown_alt", false));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // TENANT SEPARATION
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testTenantSeparation() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector1T2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testTenantSeparation")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        var entry1T2 = ConfigEntry.newBuilder()
                .setConfigClass("testTenantSeparation")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1T2)
                .setObjectType(selector1T2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));
        dal.saveConfigEntries(ALT_TEST_TENANT, List.of(entry1T2));

        var key1 = ConfigEntry.newBuilder()
                .setConfigClass(entry1.getConfigClass())
                .setConfigKey(entry1.getConfigKey())
                .setConfigVersion(entry1.getConfigVersion())
                .build();

        var savedEntry = dal.loadConfigEntry(TEST_TENANT, key1);
        var savedEntryT2 = dal.loadConfigEntry(ALT_TEST_TENANT, key1);

        Assertions.assertEquals(entry1, savedEntry);
        Assertions.assertEquals(entry1T2, savedEntryT2);

        var savedEntries = dal.loadConfigEntries(TEST_TENANT, List.of(key1));
        var savedEntriesT2 = dal.loadConfigEntries(ALT_TEST_TENANT, List.of(key1));

        Assertions.assertEquals(List.of(entry1), savedEntries);
        Assertions.assertEquals(List.of(entry1T2), savedEntriesT2);
    }

    @Test
    void testTenantSeparation_listConfigEntries() {

        var testTimestamp = MetadataCodec.encodeDatetime(Instant.now());

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var selector1T2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var entry1 = ConfigEntry.newBuilder()
                .setConfigClass("testTenantSeparation_listConfigEntries")
                .setConfigKey("entry1")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                .setObjectSelector(selector1)
                .setObjectType(selector1.getObjectType()))
                .build();

        var entry1T2 = ConfigEntry.newBuilder()
                .setConfigClass("testTenantSeparation_listConfigEntries")
                .setConfigKey("entry2")
                .setConfigVersion(1)
                .setConfigTimestamp(testTimestamp)
                .setIsLatestConfig(true)
                .setDetails(ConfigDetails.newBuilder()
                        .setObjectSelector(selector1T2)
                        .setObjectType(selector1T2.getObjectType()))
                .build();

        dal.saveConfigEntries(TEST_TENANT, List.of(entry1));
        dal.saveConfigEntries(ALT_TEST_TENANT, List.of(entry1T2));

        var entries = dal.listConfigEntries(TEST_TENANT, "testTenantSeparation_listConfigEntries", false);

        Assertions.assertEquals(1, entries.size());
        Assertions.assertTrue(entries.stream().anyMatch(e -> e.getConfigKey().equals("entry1")));

        var entriesT2 = dal.listConfigEntries(ALT_TEST_TENANT, "testTenantSeparation_listConfigEntries", false);

        Assertions.assertEquals(1, entriesT2.size());
        Assertions.assertTrue(entriesT2.stream().anyMatch(e -> e.getConfigKey().equals("entry2")));
    }
}
