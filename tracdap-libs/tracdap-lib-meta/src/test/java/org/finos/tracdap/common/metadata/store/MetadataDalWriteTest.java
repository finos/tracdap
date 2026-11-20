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

import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.metadata.TagSelector;
import org.finos.tracdap.common.db.JdbcDialect;
import org.finos.tracdap.common.exception.ETrac;
import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.exception.EMetadataWrongType;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.UuidFactory;
import org.finos.tracdap.common.metadata.store.jdbc.JdbcMetadataStore;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.finos.tracdap.common.metadata.test.IMetadataStoreTest;
import org.finos.tracdap.common.metadata.test.JdbcUnit;
import org.finos.tracdap.common.metadata.test.JdbcIntegration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.finos.tracdap.test.meta.SampleMetadata.*;
import static org.junit.jupiter.api.Assertions.*;


abstract class MetadataDalWriteTest implements IMetadataStoreTest {

    private IMetadataStore store;
    private DataSource dataSource;
    private JdbcDialect dialect;

    public void setStore(IMetadataStore store) {
        this.store = store;
    }

    @Override
    public void setDataSource(DataSource source, JdbcDialect dialect) {
        this.dataSource = source;
        this.dialect = dialect;
    }

    @ExtendWith(JdbcUnit.class)
    static class UnitTest extends MetadataDalWriteTest {}

    @Tag("integration")
    @Tag("int-metadb")
    @ExtendWith(JdbcIntegration.class)
    static class IntegrationTest extends MetadataDalWriteTest {}

    @Test
    void testSaveNewObject_ok() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 1);

        assertEquals(origTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 1);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, result1);
        assertEquals(multi2, result2);
    }

    @Test
    void testSaveNewObject_multiValuedAttr() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = addMultiValuedAttr(dummyTag(origDef, INCLUDE_HEADER));
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 1);

        assertEquals(origTag, result);

        // Save multiple
        var multi1 = addMultiValuedAttr(dummyTagForObjectType(ObjectType.DATA));
        var multi2 = addMultiValuedAttr(dummyTagForObjectType(ObjectType.MODEL));

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 1);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 1);

        assertEquals(multi1, result1);
        assertEquals(multi2, result2);
    }

    @Test
    void testSaveNewObject_duplicate() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        assertThrows(EMetadataDuplicate.class, () -> store.saveNewObjects(TEST_TENANT, List.of(origTag, origTag)));
        assertThrows(EMetadataNotFound.class, () -> store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        // First insert should succeed if they are run one by one
        assertDoesNotThrow(() -> store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag)));
        assertDoesNotThrow(() -> store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 1));
        assertThrows(EMetadataDuplicate.class, () -> store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag)));
    }

    @Test
    void testSaveNewVersion_ok() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef);
        var nextTag = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 1);

        assertEquals(nextTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = tagForNextObject(multi1, nextDataDef(multi1.getDefinition()), INCLUDE_HEADER);
        var multi2v2 = tagForNextObject(multi2, nextModelDef(multi2.getDefinition()), INCLUDE_HEADER);

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        store.saveNewVersions(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 2, 1);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 2, 1);

        assertEquals(multi1v2, result1);
        assertEquals(multi2v2, result2);
    }

    @Test
    void testSaveNewVersion_multiValuedAttr() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef);
        var nextTag = addMultiValuedAttr(tagForNextObject(origTag, nextDef, INCLUDE_HEADER));

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 1);

        assertEquals(nextTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = addMultiValuedAttr(tagForNextObject(multi1, nextDataDef(multi1.getDefinition()), INCLUDE_HEADER));
        var multi2v2 = addMultiValuedAttr(tagForNextObject(multi2, nextModelDef(multi2.getDefinition()), INCLUDE_HEADER));

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        store.saveNewVersions(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 2, 1);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 2, 1);

        assertEquals(multi1v2, result1);
        assertEquals(multi2v2, result2);
    }

    @Test
    void testSaveNewVersion_duplicate() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var nextDef = nextDataDef(origDef);
        var nextTag = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataDuplicate.class, () -> store.saveNewVersions(TEST_TENANT, List.of(nextTag, nextTag)));
        assertThrows(EMetadataNotFound.class, () -> store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 1));

        // First insert should succeed if they are run one by one
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag));
        assertThrows(EMetadataDuplicate.class, () -> store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag)));

        var loadDup2 = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 1);
        assertEquals(nextTag, loadDup2);
    }

    @Test
    void testSaveNewVersion_missingObject() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDef = nextDataDef(origDef);
        var nextTag = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);

        // Save next version, single, without saving original
        assertThrows(EMetadataNotFound.class, () -> store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextTag)));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);

        // Save next, multiple, one item does not have original
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataNotFound.class, () -> store.saveNewVersions(TEST_TENANT, List.of(nextTag, nextModelTag)));
    }

    @Test
    void testSaveNewVersion_wrongObjectType() {

        var dataTag = dummyTagForObjectType(ObjectType.DATA);
        var nextDataTag = tagForNextObject(dataTag, nextDataDef(dataTag.getDefinition()), INCLUDE_HEADER);

        // Create a model def with the same ID as the data def
        var modelDef = dummyModelDef();
        var rawModelTag = tagForNextObject(dataTag, modelDef, INCLUDE_HEADER);
        var nextModelTag = rawModelTag.toBuilder()
                .setHeader(rawModelTag.getHeader().toBuilder()
                .setObjectType(ObjectType.MODEL))
                .build();

        // Save next version, single, without saving original
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(dataTag));

        assertThrows(EMetadataWrongType.class, () -> store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextModelTag)));
        assertThrows(EMetadataWrongType.class, () -> store.saveNewVersions(TEST_TENANT, List.of(nextDataTag, nextModelTag)));
    }

    @Test
    void testSaveNewTag_ok() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        // Test saving tag v2 against object v2
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextDefTag1));
        store.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 2);

        assertEquals(nextDefTag2, result);

        // Save multiple - this test saves tag v2 against object v1
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = nextTag(multi1, UPDATE_TAG_VERSION);
        var multi2v2 = nextTag(multi2, UPDATE_TAG_VERSION);

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        store.saveNewTags(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 2);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 2);

        assertEquals(multi1v2, result1);
        assertEquals(multi2v2, result2);
    }

    @Test
    void testSaveNewTag_multiValuedAttr() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDefTag1 = tagForNextObject(origTag, nextDataDef(origDef), INCLUDE_HEADER);
        var nextDefTag2 = addMultiValuedAttr(nextTag(nextDefTag1, UPDATE_TAG_VERSION));
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        // Test saving tag v2 against object v2
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextDefTag1));
        store.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2));
        var result = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 2, 2);

        assertEquals(nextDefTag2, result);

        // Save multiple - this test saves tag v2 against object v1
        var multi1 = dummyTagForObjectType(ObjectType.DATA);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

        var multi1v2 = addMultiValuedAttr(nextTag(multi1, UPDATE_TAG_VERSION));
        var multi2v2 = addMultiValuedAttr(nextTag(multi2, UPDATE_TAG_VERSION));

        store.saveNewObjects(TEST_TENANT, List.of(multi1, multi2));
        store.saveNewTags(TEST_TENANT, List.of(multi1v2, multi2v2));

        var result1 = store.loadObject(TEST_TENANT, ObjectType.DATA, UUID.fromString(multi1.getHeader().getObjectId()), 1, 2);
        var result2 = store.loadObject(TEST_TENANT, ObjectType.MODEL, UUID.fromString(multi2.getHeader().getObjectId()), 1, 2);

        assertEquals(multi1v2, result1);
        assertEquals(multi2v2, result2);
    }

    @Test
    void testSaveNewTag_duplicate() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataDuplicate.class, () -> store.saveNewTags(TEST_TENANT, List.of(nextTag, nextTag)));
        assertThrows(EMetadataNotFound.class, () -> store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 2));

        // First insert should succeed if they are run one by one
        store.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag));
        assertThrows(EMetadataDuplicate.class, () -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag)));

        var loadDup2 = store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 2);
        assertEquals(nextTag, loadDup2);
    }

    @Test
    void testSaveNewTag_severalVersionsOfOneObject() {

        var v1 = dummyTag(dummyDataDef(), INCLUDE_HEADER);
        var v2 = tagForNextObject(v1, nextDataDef(v1.getDefinition()), INCLUDE_HEADER);
        var v3 = tagForNextObject(v2, nextDataDef(v2.getDefinition()), INCLUDE_HEADER);
        var objectId = UUID.fromString(v1.getHeader().getObjectId());

        store.saveNewObjects(TEST_TENANT, List.of(v1));
        store.saveNewVersions(TEST_TENANT, List.of(v2));
        store.saveNewVersions(TEST_TENANT, List.of(v3));

        // One tag on each version, in ascending order, in one save
        var newTags = List.of(
                nextTag(v1, UPDATE_TAG_VERSION),
                nextTag(v2, UPDATE_TAG_VERSION),
                nextTag(v3, UPDATE_TAG_VERSION));

        store.saveNewTags(TEST_TENANT, newTags);

        for (var version = 1; version <= 3; version++) {
            var saved = store.loadObject(TEST_TENANT, ObjectType.DATA, objectId, version, 2);
            assertEquals(2, saved.getHeader().getTagVersion());
            assertEquals(newTags.get(version - 1).getAttrsMap(), saved.getAttrsMap());
        }

        // A second save from the same prior tags is refused, nothing is saved
        var staleTags = List.of(
                nextTag(nextTag(v1, UPDATE_TAG_VERSION), UPDATE_TAG_VERSION),
                nextTag(v2, UPDATE_TAG_VERSION));

        assertThrows(EMetadataDuplicate.class, () -> store.saveNewTags(TEST_TENANT, staleTags));
        assertThrows(EMetadataNotFound.class, () -> store.loadObject(TEST_TENANT, ObjectType.DATA, objectId, 1, 3));
    }

    @Test
    void testSaveNewTag_overlappingTransactions() throws Exception {

        var origTag = dummyTag(dummyDataDef(), INCLUDE_HEADER);
        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION);
        var objectId = UUID.fromString(origTag.getHeader().getObjectId());

        store.saveNewObjects(TEST_TENANT, List.of(origTag));

        // The first writer saves its tag and is held before commit, with its transaction open
        var holdNextCommit = new AtomicBoolean(false);
        var firstAtCommit = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var firstStore = new JdbcMetadataStore(dialect, holdAtCommit(dataSource, holdNextCommit, firstAtCommit, releaseFirst));
        firstStore.start();
        holdNextCommit.set(true);

        var executor = Executors.newFixedThreadPool(2);

        try {

            var first = executor.submit(() -> firstStore.saveNewTags(TEST_TENANT, List.of(nextTag)));
            assertTrue(firstAtCommit.await(30, TimeUnit.SECONDS));

            // The second writer saves the same tag version while the first transaction is open
            var second = executor.submit(() -> store.saveNewTags(TEST_TENANT, List.of(nextTag)));

            // Let the second writer reach the database before the first commits
            Thread.sleep(500);
            releaseFirst.countDown();

            assertDoesNotThrow(() -> first.get(30, TimeUnit.SECONDS));

            var error = assertThrows(ExecutionException.class, () -> second.get(30, TimeUnit.SECONDS));
            assertInstanceOf(EMetadataDuplicate.class, error.getCause(), "Overlapping tag save failed with an unexpected error");

            assertEquals(nextTag.getAttrsMap(), store.loadObject(TEST_TENANT, ObjectType.DATA, objectId, 1, 2).getAttrsMap());
        }
        finally {
            releaseFirst.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testSaveNewObject_attrNamesDifferingOnlyInCase() {

        var tag = dummyTag(dummyDataDef(), INCLUDE_HEADER).toBuilder()
                .putAttrs("case_test_attr", MetadataCodec.encodeValue("lower"))
                .putAttrs("CASE_TEST_ATTR", MetadataCodec.encodeValue("upper"))
                .build();

        var objectId = UUID.fromString(tag.getHeader().getObjectId());

        // The schema sets no collation, these databases compare attribute names ignoring case by default
        if (CASE_INSENSITIVE_DIALECTS.contains(dialect)) {
            assertThrows(ETrac.class, () -> store.saveNewObjects(TEST_TENANT, List.of(tag)));
        }
        else {
            store.saveNewObjects(TEST_TENANT, List.of(tag));
            var saved = store.loadObject(TEST_TENANT, ObjectType.DATA, objectId, 1, 1);
            assertEquals("lower", MetadataCodec.decodeStringValue(saved.getAttrsOrThrow("case_test_attr")));
            assertEquals("upper", MetadataCodec.decodeStringValue(saved.getAttrsOrThrow("CASE_TEST_ATTR")));
        }
    }

    private static final List<JdbcDialect> CASE_INSENSITIVE_DIALECTS = List.of(
            JdbcDialect.MYSQL, JdbcDialect.MARIADB, JdbcDialect.SQLSERVER);

    private static DataSource holdAtCommit(
            DataSource source, AtomicBoolean holdNextCommit,
            CountDownLatch atCommit, CountDownLatch release) {

        return (DataSource) Proxy.newProxyInstance(
                DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {

                    var result = invoke(source, method, args);

                    if (!method.getName().equals("getConnection"))
                        return result;

                    var conn = (Connection) result;

                    return Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                            (connProxy, connMethod, connArgs) -> {

                                if (connMethod.getName().equals("commit") && holdNextCommit.compareAndSet(true, false)) {
                                    atCommit.countDown();
                                    if (!release.await(30, TimeUnit.SECONDS))
                                        throw new SQLException("Test writer was not released");
                                }

                                return invoke(conn, connMethod, connArgs);
                            });
                });
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {

        try {
            return method.invoke(target, args);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Test
    void testSaveNewTag_missingObject() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION);

        // Save next tag, single, without saving object
        assertThrows(EMetadataNotFound.class, () -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag)));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);

        // Save next tag, multiple, one item does not have an object
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataNotFound.class, () -> store.saveNewTags(TEST_TENANT, List.of(nextTag, nextModelTag)));
    }

    @Test
    void testSaveNewTag_missingVersion() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var nextDef = nextDataDef(origDef);
        var nextDefTag1 = tagForNextObject(origTag, nextDef, INCLUDE_HEADER);
        var nextDefTag2 = nextTag(nextDefTag1, UPDATE_TAG_VERSION);

        // Save next tag (single) on an unknown object version
        // This is an error, even for tag v1
        // saveNewVersion must be called first
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));
        assertThrows(EMetadataNotFound.class, () -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag1)));
        assertThrows(EMetadataNotFound.class, () -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2)));

        var modelTag = dummyTagForObjectType(ObjectType.MODEL);
        var nextModelTag = tagForNextObject(modelTag, nextModelDef(modelTag.getDefinition()), INCLUDE_HEADER);
        var nextModelTag2 = nextTag(nextModelTag, UPDATE_TAG_VERSION);

        // Save object 1 version 2, and object 2 version 1
        store.saveNewVersions(TEST_TENANT, Collections.singletonList(nextDefTag1));
        store.saveNewObjects(TEST_TENANT, Collections.singletonList(modelTag));

        // Save next tag (multiple), second item is missing the required object version
        assertThrows(EMetadataNotFound.class, () -> store.saveNewTags(TEST_TENANT, List.of(nextDefTag2, nextModelTag2)));

        // Saving the valid tag by itself should not throw
        Assertions.assertDoesNotThrow(() -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextDefTag2)));
    }

    @Test
    void testSaveNewTag_wrongObjectType() {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);
        var origId = UUID.fromString(origTag.getHeader().getObjectId());

        var nextHeader = origTag
                .getHeader()
                .toBuilder()
                .setObjectType(ObjectType.MODEL);

        var nextDef = dummyModelDef();

        var nextTag = nextTag(origTag, UPDATE_TAG_VERSION)
                .toBuilder()
                .setHeader(nextHeader)
                .setDefinition(nextDef)
                .build();

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag));

        assertThrows(EMetadataWrongType.class, () -> store.saveNewTags(TEST_TENANT, Collections.singletonList(nextTag)));
        assertThrows(EMetadataNotFound.class, () -> store.loadObject(TEST_TENANT, ObjectType.DATA, origId, 1, 2));

        var origDef2 = dummyModelDef();
        var origTag2 = dummyTag(origDef2, INCLUDE_HEADER);
        var origId2 = UUID.fromString(origTag2.getHeader().getObjectId());

        var nextTag2 = nextTag(origTag2, UPDATE_TAG_VERSION);

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(origTag2));

        assertThrows(EMetadataWrongType.class, () -> store.saveNewTags(TEST_TENANT, List.of(nextTag, nextTag2)));

        var selector1 = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(origId.toString())
                .setObjectVersion(1)
                .setTagVersion(2)
                .build();

        var selector2 = TagSelector.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(origId2.toString())
                .setObjectVersion(1)
                .setTagVersion(2)
                .build();

        assertThrows(EMetadataNotFound.class, () -> store.loadObjects(TEST_TENANT, List.of(selector1, selector2)));
    }

    @Test
    void testPreallocate_ok() {

        // Save one
        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef, INCLUDE_HEADER);

        store.savePreallocatedIds(TEST_TENANT, List.of(origTag.getHeader()));
        store.savePreallocatedObjects(TEST_TENANT, Collections.singletonList(origTag));
        var result = store.loadObject(TEST_TENANT, selectorForTag(origTag));

        assertEquals(origTag, result);

        // Save multiple
        var multi1 = dummyTagForObjectType(ObjectType.MODEL);
        var multi2 = dummyTagForObjectType(ObjectType.MODEL);

       store.savePreallocatedIds(TEST_TENANT, List.of(multi1.getHeader(), multi2.getHeader()));
       store.savePreallocatedObjects(TEST_TENANT, List.of(multi1, multi2));

        var result1 = store.loadObject(TEST_TENANT, selectorForTag(multi1));
        var result2 = store.loadObject(TEST_TENANT, selectorForTag(multi2));

        assertEquals(multi1, result1);
        assertEquals(multi2, result2);
    }

    @Test
    void testPreallocate_duplicate() {

        var id1 = UuidFactory.DEFAULT.allocate();
        var header1 = TagHeader.newBuilder().setObjectType(ObjectType.DATA).setObjectId(id1.toString()).build();

        store.savePreallocatedIds(TEST_TENANT, List.of(header1));

        assertThrows(EMetadataDuplicate.class, () ->
                store.savePreallocatedIds(TEST_TENANT, List.of(header1)));

        var obj2 = dummyTagForObjectType(ObjectType.MODEL);
        var id3 = UuidFactory.DEFAULT.allocate();
        var header3 = TagHeader.newBuilder().setObjectType(ObjectType.DATA).setObjectId(id3.toString()).build();

        store.saveNewObjects(TEST_TENANT, Collections.singletonList(obj2));

        assertThrows(EMetadataDuplicate.class, () ->
                store.savePreallocatedIds(TEST_TENANT, List.of(obj2.getHeader(), header3)));
    }

    @Test
    void testPreallocate_missingObjectId() {

        var obj1 = dummyTagForObjectType(ObjectType.MODEL);

        assertThrows(EMetadataNotFound.class,
                () -> store.savePreallocatedObjects(TEST_TENANT, Collections.singletonList(obj1)));

        var obj2 = dummyTagForObjectType(ObjectType.DATA);

        store.savePreallocatedIds(TEST_TENANT, List.of(obj2.getHeader()));

        assertThrows(EMetadataNotFound.class,
                () -> store.savePreallocatedObjects(TEST_TENANT, List.of(obj1, obj2)));
    }

    @Test
    void testPreallocate_wrongObjectType() {

        var obj1 = dummyTagForObjectType(ObjectType.MODEL);
        var wrongHeader = obj1.getHeader().toBuilder().setObjectType(ObjectType.DATA).build();

        store.savePreallocatedIds(TEST_TENANT, List.of(wrongHeader));

        assertThrows(EMetadataWrongType.class, () ->
                store.savePreallocatedObjects(TEST_TENANT, Collections.singletonList(obj1)));

        var obj2 = dummyTagForObjectType(ObjectType.DATA);

        store.savePreallocatedIds(TEST_TENANT, List.of(obj2.getHeader()));

        assertThrows(EMetadataWrongType.class, () ->
                store.savePreallocatedObjects(TEST_TENANT, List.of(obj1, obj2)));
    }
}
