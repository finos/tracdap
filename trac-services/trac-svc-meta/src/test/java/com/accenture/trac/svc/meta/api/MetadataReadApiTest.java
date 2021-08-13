/*
 * Copyright 2020 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.accenture.trac.svc.meta.api;

import com.accenture.trac.api.*;
import com.accenture.trac.metadata.*;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.services.MetadataReadService;
import com.accenture.trac.svc.meta.services.MetadataSearchService;
import com.accenture.trac.svc.meta.services.MetadataWriteService;

import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcIntegration;
import com.accenture.trac.svc.meta.test.JdbcUnit;

import com.accenture.trac.svc.meta.test.TestData;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.UUID;

import static com.accenture.trac.svc.meta.test.TestData.*;
import static org.junit.jupiter.api.Assertions.*;


abstract class MetadataReadApiTest implements IDalTestable {

    private IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }

    // Include this test case as a unit test
    @ExtendWith(JdbcUnit.class)
    static class Unit extends MetadataReadApiTest {}

    // Include this test case for integration against different database backends
    @org.junit.jupiter.api.Tag("integration")
    @Tag("int-metadb")
    @ExtendWith(JdbcIntegration.class)
    static class Integration extends MetadataReadApiTest {}

    @Rule
    final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private TracMetadataApiGrpc.TracMetadataApiBlockingStub readApi;
    private TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub writeApi;

    @BeforeEach
    void setup() throws Exception {

        var serverName = InProcessServerBuilder.generateName();

        var readService = new MetadataReadService(dal);
        var writeService = new MetadataWriteService(dal);
        var searchService = new MetadataSearchService(dal);

        var publicApiImpl = new TracMetadataApi(readService, writeService, searchService);
        var trustedApiImpl = new TrustedMetadataApi(readService, writeService, searchService);

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                 .forName(serverName)
                .directExecutor()
                .addService(publicApiImpl)
                .addService(trustedApiImpl)
                .build()
                .start());

        readApi = TracMetadataApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));

        writeApi = TrustedMetadataApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void readObject_ok(ObjectType objectType) {

        var origObj = TestData.dummyDefinitionForType(objectType);
        var attrs = TestData.dummyAttrs();
        var tagUpdates = TestData.tagUpdatesForAttrs(attrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setDefinition(origObj)
                .addAllTagUpdate(tagUpdates)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);
        var objectId = UUID.fromString(tagHeader.getObjectId());
        var selector = selectorForTag(tagHeader);

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selector)
                .build();

        var savedTag = readApi.readObject(readRequest);

        var expectedTag = savedTag.newBuilderForType()
                .setHeader(tagHeader)
                .setDefinition(origObj)
                .putAllAttrs(attrs)
                .build();

        assertEquals(objectId, UUID.fromString(savedTag.getHeader().getObjectId()));
        assertEquals(expectedTag, savedTag);
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
            names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void readBatch_ok(ObjectType objectType) {

        var origObj = TestData.dummyDefinitionForType(objectType);
        var attrs = TestData.dummyAttrs();
        var tagUpdates = TestData.tagUpdatesForAttrs(attrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setDefinition(origObj)
                .addAllTagUpdate(tagUpdates)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);
        var objectId = UUID.fromString(tagHeader.getObjectId());
        var selector = selectorForTag(tagHeader);

        // Create a second object with the same def/attrs, will have a new ID
        var tagHeader2 = writeApi.createObject(writeRequest);
        var objectId2 = UUID.fromString(tagHeader2.getObjectId());
        var selector2 = selectorForTag(tagHeader2);

        var readRequest = MetadataBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addSelector(selector)
                .addSelector(selector2)
                .build();

        var savedTags = readApi.readBatch(readRequest);

        var expectedTag = savedTags.getTag(0).newBuilderForType()
                .setHeader(tagHeader)
                .setDefinition(origObj)
                .putAllAttrs(attrs)
                .build();

        var expectedTag2 = savedTags.getTag(1).newBuilderForType()
                .setHeader(tagHeader2)
                .setDefinition(origObj)
                .putAllAttrs(attrs)
                .build();

        assertEquals(2, savedTags.getTagCount());
        assertEquals(objectId, UUID.fromString(savedTags.getTag(0).getHeader().getObjectId()));
        assertEquals(objectId2, UUID.fromString(savedTags.getTag(1).getHeader().getObjectId()));
        assertEquals(expectedTag, savedTags.getTag(0));
        assertEquals(expectedTag2, savedTags.getTag(1));
    }

    @Test
    void readObject_versions() {

        var v1Obj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var v1Attrs = TestData.dummyAttrs();
        var v1TagUpdates = TestData.tagUpdatesForAttrs(v1Attrs);

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(v1Obj)
                .addAllTagUpdate(v1TagUpdates)
                .build();

        var v1Header = writeApi.createObject(v1WriteRequest);
        var v1Selector = selectorForTag(v1Header);

        // No attr changes for v2
        var v2Obj = TestData.dummyVersionForType(v1Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var v2Header = writeApi.updateObject(v2WriteRequest);

        var t2Attrs = new HashMap<>(v1Attrs);
        t2Attrs.put("v3_attr", MetadataCodec.encodeValue("Use the force"));

        var t2TagUpdate = TagUpdate.newBuilder()
                .setAttrName("v3_attr")
                .setValue(MetadataCodec.encodeValue("Use the force"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdate(t2TagUpdate)
                .build();

        var t2Header = writeApi.updateTag(t2WriteRequest);

        var v1MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selectorForTag(v1Header))
                .build();

        var v2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selectorForTag(v2Header))
                .build();

        var t2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selectorForTag(t2Header))
                .build();

        var v1TagSaved = readApi.readObject(v1MetadataReadRequest);
        var v2TagSaved = readApi.readObject(v2MetadataReadRequest);
        var t2TagSaved = readApi.readObject(t2MetadataReadRequest);

        var v1TagExpected = v1TagSaved.newBuilderForType()
                .setHeader(v1Header)
                .setDefinition(v1Obj)
                .putAllAttrs(v1Attrs)
                .build();

        var v2TagExpected = v2TagSaved.newBuilderForType()
                .setHeader(v2Header)
                .setDefinition(v2Obj)
                .putAllAttrs(v1Attrs)
                .build();

        var t2TagExpected = t2TagSaved.newBuilderForType()
                .setHeader(t2Header)
                .setDefinition(v1Obj)
                .putAllAttrs(t2Attrs)
                .build();

        assertEquals(v1TagExpected, v1TagSaved);
        assertEquals(v2TagExpected, v2TagSaved);
        assertEquals(t2TagExpected, t2TagSaved);
    }

    @Test
    void readBatch_versions() {

        var v1Obj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var v1Attrs = TestData.dummyAttrs();
        var v1TagUpdates = TestData.tagUpdatesForAttrs(v1Attrs);

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(v1Obj)
                .addAllTagUpdate(v1TagUpdates)
                .build();

        var v1Header = writeApi.createObject(v1WriteRequest);
        var v1Selector = selectorForTag(v1Header);

        // No attr changes for v2
        var v2Obj = TestData.dummyVersionForType(v1Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var v2Header = writeApi.updateObject(v2WriteRequest);

        var t2Attrs = new HashMap<>(v1Attrs);
        t2Attrs.put("v3_attr", MetadataCodec.encodeValue("Use the force"));

        var t2TagUpdate = TagUpdate.newBuilder()
                .setAttrName("v3_attr")
                .setValue(MetadataCodec.encodeValue("Use the force"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdate(t2TagUpdate)
                .build();

        var t2Header = writeApi.updateTag(t2WriteRequest);

        var readRequest = MetadataBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addSelector(selectorForTag(v1Header))
                .addSelector(selectorForTag(v2Header))
                .addSelector(selectorForTag(t2Header))
                .build();

        var savedTags = readApi.readBatch(readRequest);
        var v1TagSaved = savedTags.getTag(0);
        var v2TagSaved = savedTags.getTag(1);
        var t2TagSaved = savedTags.getTag(2);

        var v1TagExpected = v1TagSaved.newBuilderForType()
                .setHeader(v1Header)
                .setDefinition(v1Obj)
                .putAllAttrs(v1Attrs)
                .build();

        var v2TagExpected = v2TagSaved.newBuilderForType()
                .setHeader(v2Header)
                .setDefinition(v2Obj)
                .putAllAttrs(v1Attrs)
                .build();

        var t2TagExpected = t2TagSaved.newBuilderForType()
                .setHeader(t2Header)
                .setDefinition(v1Obj)
                .putAllAttrs(t2Attrs)
                .build();

        assertEquals(v1TagExpected, v1TagSaved);
        assertEquals(v2TagExpected, v2TagSaved);
        assertEquals(t2TagExpected, t2TagSaved);
    }

    @Test
    void readObject_asOf() throws Exception {

        var v1Obj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var v1Attrs = TestData.dummyAttrs();
        var v1TagUpdates = TestData.tagUpdatesForAttrs(v1Attrs);

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(v1Obj)
                .addAllTagUpdate(v1TagUpdates)
                .build();

        var v1Header = writeApi.createObject(v1WriteRequest);
        var v1Selector = selectorForTag(v1Header);
        var objectId = UUID.fromString(v1Header.getObjectId());

        Thread.sleep(10);

        // No attr changes for v2
        var v2Obj = TestData.dummyVersionForType(v1Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var v2Header = writeApi.updateObject(v2WriteRequest);

        Thread.sleep(10);

        var t2Attrs = new HashMap<>(v1Attrs);
        t2Attrs.put("v3_attr", MetadataCodec.encodeValue("Use the force"));

        var t2TagUpdate = TagUpdate.newBuilder()
                .setAttrName("v3_attr")
                .setValue(MetadataCodec.encodeValue("Use the force"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdate(t2TagUpdate)
                .build();

        var t2Header = writeApi.updateTag(t2WriteRequest);

        // Build selectors using object and tag as-of times

        var baseSelector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(objectId.toString())
                .build();

        var v1Time = MetadataCodec.decodeDatetime(v1Header.getTagTimestamp()).plusNanos(5000);
        var v2Time = MetadataCodec.decodeDatetime(v2Header.getTagTimestamp()).plusNanos(5000);
        var t2Time = MetadataCodec.decodeDatetime(t2Header.getTagTimestamp()).plusNanos(5000);

        var v1MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(baseSelector.toBuilder()
                .setObjectAsOf(MetadataCodec.encodeDatetime(v1Time))
                .setTagAsOf(MetadataCodec.encodeDatetime(v1Time)))
                .build();

        var v2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(baseSelector.toBuilder()
                .setObjectAsOf(MetadataCodec.encodeDatetime(v2Time))
                .setTagAsOf(MetadataCodec.encodeDatetime(v2Time)))
                .build();

        var t2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(baseSelector.toBuilder()
                .setObjectAsOf(MetadataCodec.encodeDatetime(v1Time))
                .setTagAsOf(MetadataCodec.encodeDatetime(t2Time)))
                .build();

        var v1TagSaved = readApi.readObject(v1MetadataReadRequest);
        var v2TagSaved = readApi.readObject(v2MetadataReadRequest);
        var t2TagSaved = readApi.readObject(t2MetadataReadRequest);

        var v1TagExpected = v1TagSaved.newBuilderForType()
                .setHeader(v1Header)
                .setDefinition(v1Obj)
                .putAllAttrs(v1Attrs)
                .build();

        var v2TagExpected = v2TagSaved.newBuilderForType()
                .setHeader(v2Header)
                .setDefinition(v2Obj)
                .putAllAttrs(v1Attrs)
                .build();

        var t2TagExpected = t2TagSaved.newBuilderForType()
                .setHeader(t2Header)
                .setDefinition(v1Obj)
                .putAllAttrs(t2Attrs)
                .build();

        assertEquals(v1TagExpected, v1TagSaved);
        assertEquals(v2TagExpected, v2TagSaved);
        assertEquals(t2TagExpected, t2TagSaved);
    }

    @Test
    void readBatch_asOf() throws Exception {

        var v1Obj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var v1Attrs = TestData.dummyAttrs();
        var v1TagUpdates = TestData.tagUpdatesForAttrs(v1Attrs);

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(v1Obj)
                .addAllTagUpdate(v1TagUpdates)
                .build();

        var v1Header = writeApi.createObject(v1WriteRequest);
        var v1Selector = selectorForTag(v1Header);
        var objectId = UUID.fromString(v1Header.getObjectId());

        Thread.sleep(10);

        // No attr changes for v2
        var v2Obj = TestData.dummyVersionForType(v1Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var v2Header = writeApi.updateObject(v2WriteRequest);

        Thread.sleep(10);

        var t2Attrs = new HashMap<>(v1Attrs);
        t2Attrs.put("v3_attr", MetadataCodec.encodeValue("Use the force"));

        var t2TagUpdate = TagUpdate.newBuilder()
                .setAttrName("v3_attr")
                .setValue(MetadataCodec.encodeValue("Use the force"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdate(t2TagUpdate)
                .build();

        var t2Header = writeApi.updateTag(t2WriteRequest);

        // Build selectors using object and tag as-of times

        var baseSelector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(objectId.toString())
                .build();

        var v1Time = MetadataCodec.decodeDatetime(v1Header.getTagTimestamp()).plusNanos(5000);
        var v2Time = MetadataCodec.decodeDatetime(v2Header.getTagTimestamp()).plusNanos(5000);
        var t2Time = MetadataCodec.decodeDatetime(t2Header.getTagTimestamp()).plusNanos(5000);

        var v1SelectorAsOf = baseSelector.toBuilder()
                .setObjectAsOf(MetadataCodec.encodeDatetime(v1Time))
                .setTagAsOf(MetadataCodec.encodeDatetime(v1Time));

        var v2SelectorAsOf = baseSelector.toBuilder()
                .setObjectAsOf(MetadataCodec.encodeDatetime(v2Time))
                .setTagAsOf(MetadataCodec.encodeDatetime(v2Time));

        var t2SelectorAsOf = baseSelector.toBuilder()
                .setObjectAsOf(MetadataCodec.encodeDatetime(v1Time))
                .setTagAsOf(MetadataCodec.encodeDatetime(t2Time));

        var readRequest = MetadataBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addSelector(v1SelectorAsOf)
                .addSelector(v2SelectorAsOf)
                .addSelector(t2SelectorAsOf)
                .build();

        var savedTags = readApi.readBatch(readRequest);
        var v1TagSaved = savedTags.getTag(0);
        var v2TagSaved = savedTags.getTag(1);
        var t2TagSaved = savedTags.getTag(2);

        var v1TagExpected = v1TagSaved.newBuilderForType()
                .setHeader(v1Header)
                .setDefinition(v1Obj)
                .putAllAttrs(v1Attrs)
                .build();

        var v2TagExpected = v2TagSaved.newBuilderForType()
                .setHeader(v2Header)
                .setDefinition(v2Obj)
                .putAllAttrs(v1Attrs)
                .build();

        var t2TagExpected = t2TagSaved.newBuilderForType()
                .setHeader(t2Header)
                .setDefinition(v1Obj)
                .putAllAttrs(t2Attrs)
                .build();

        assertEquals(v1TagExpected, v1TagSaved);
        assertEquals(v2TagExpected, v2TagSaved);
        assertEquals(t2TagExpected, t2TagSaved);
    }

    @Test
    void readObject_latest() {

        // Save v1 object

        var v1Obj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var v1Attrs = TestData.dummyAttrs();
        var v1TagUpdates = TestData.tagUpdatesForAttrs(v1Attrs);

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(v1Obj)
                .addAllTagUpdate(v1TagUpdates)
                .build();

        var v1Header = writeApi.createObject(v1WriteRequest);
        var v1Selector = selectorForTag(v1Header);

        // Read V1 using latest / latest selector

        var baseSelector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(v1Header.getObjectId())
                .build();

        var latestSelector = baseSelector.toBuilder()
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var latestMetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(latestSelector)
                .build();

        var v1TagSaved = readApi.readObject(latestMetadataReadRequest);

        var v1TagExpected = v1TagSaved.newBuilderForType()
                .setHeader(v1Header)
                .setDefinition(v1Obj)
                .putAllAttrs(v1Attrs)
                .build();

        assertEquals(v1TagExpected, v1TagSaved);

        // Save V2 object

        var v2Obj = TestData.dummyVersionForType(v1Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var v2Header = writeApi.updateObject(v2WriteRequest);

        // Read v2 object using latest / latest selector

        var v2TagSaved = readApi.readObject(latestMetadataReadRequest);

        var v2TagExpected = v2TagSaved.newBuilderForType()
                .setHeader(v2Header)
                .setDefinition(v2Obj)
                .putAllAttrs(v1Attrs)
                .build();

        assertEquals(v2TagExpected, v2TagSaved);

        // Save T2 tag on V1 object


        var t2Attrs = new HashMap<>(v1Attrs);
        t2Attrs.put("v3_attr", MetadataCodec.encodeValue("Use the force"));

        var t2TagUpdate = TagUpdate.newBuilder()
                .setAttrName("v3_attr")
                .setValue(MetadataCodec.encodeValue("Use the force"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdate(t2TagUpdate)
                .build();

        var t2Header = writeApi.updateTag(t2WriteRequest);

        // Read request using latest / latest - should still return V2 object

        v2TagSaved = readApi.readObject(latestMetadataReadRequest);
        assertEquals(v2TagExpected, v2TagSaved);

        // Read request for object V1 / latest tag

        var t2Selector = baseSelector.toBuilder()
                .setObjectVersion(1)
                .setLatestTag(true);

        var t2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(t2Selector)
                .build();

        var t2TagSaved = readApi.readObject(t2MetadataReadRequest);

        var t2TagExpected = t2TagSaved.newBuilderForType()
                .setHeader(t2Header)
                .setDefinition(v1Obj)
                .putAllAttrs(t2Attrs)
                .build();

        assertEquals(t2TagExpected, t2TagSaved);
    }

    @Test
    void readBatch_latest() {

        // Create a second object to put in batch read requests next to the selectors being tested

        var extraObj = TestData.dummyCustomDef();
        var extraAttrs = TestData.dummyAttrs();
        var extraTagUpdates = TestData.tagUpdatesForAttrs(extraAttrs);

        var extraWriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setDefinition(extraObj)
                .addAllTagUpdate(extraTagUpdates)
                .build();

        var extraHeader = writeApi.createObject(extraWriteRequest);
        var extraSelector = selectorForTag(extraHeader);

        // Save v1 object

        var v1Obj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var v1Attrs = TestData.dummyAttrs();
        var v1TagUpdates = TestData.tagUpdatesForAttrs(v1Attrs);

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(v1Obj)
                .addAllTagUpdate(v1TagUpdates)
                .build();

        var v1Header = writeApi.createObject(v1WriteRequest);
        var v1Selector = selectorForTag(v1Header);

        // Read V1 using latest / latest selector

        var baseSelector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(v1Header.getObjectId())
                .build();

        var latestSelector = baseSelector.toBuilder()
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var latestMetadataReadRequest = MetadataBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addSelector(latestSelector)
                .addSelector(extraSelector)
                .build();

        var batch = readApi.readBatch(latestMetadataReadRequest);
        var v1TagSaved = batch.getTag(0);

        var v1TagExpected = v1TagSaved.newBuilderForType()
                .setHeader(v1Header)
                .setDefinition(v1Obj)
                .putAllAttrs(v1Attrs)
                .build();

        assertEquals(v1TagExpected, v1TagSaved);

        // Save V2 object

        var v2Obj = TestData.dummyVersionForType(v1Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var v2Header = writeApi.updateObject(v2WriteRequest);

        // Read v2 object using latest / latest selector

        batch = readApi.readBatch(latestMetadataReadRequest);
        var v2TagSaved = batch.getTag(0);

        var v2TagExpected = v2TagSaved.newBuilderForType()
                .setHeader(v2Header)
                .setDefinition(v2Obj)
                .putAllAttrs(v1Attrs)
                .build();

        assertEquals(v2TagExpected, v2TagSaved);

        // Save T2 tag on V1 object


        var t2Attrs = new HashMap<>(v1Attrs);
        t2Attrs.put("v3_attr", MetadataCodec.encodeValue("Use the force"));

        var t2TagUpdate = TagUpdate.newBuilder()
                .setAttrName("v3_attr")
                .setValue(MetadataCodec.encodeValue("Use the force"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdate(t2TagUpdate)
                .build();

        var t2Header = writeApi.updateTag(t2WriteRequest);

        // Read request using latest / latest - should still return V2 object

        batch = readApi.readBatch(latestMetadataReadRequest);
        v2TagSaved = batch.getTag(0);
        assertEquals(v2TagExpected, v2TagSaved);

        // Read request for object V1 / latest tag

        var t2Selector = baseSelector.toBuilder()
                .setObjectVersion(1)
                .setLatestTag(true);

        var t2MetadataReadRequest = MetadataBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addSelector(t2Selector)
                .addSelector(extraSelector)
                .build();

        batch = readApi.readBatch(t2MetadataReadRequest);
        var t2TagSaved = batch.getTag(0);

        var t2TagExpected = t2TagSaved.newBuilderForType()
                .setHeader(t2Header)
                .setDefinition(v1Obj)
                .putAllAttrs(t2Attrs)
                .build();

        assertEquals(t2TagExpected, t2TagSaved);
    }

    @Test
    void readObject_missingItems() {

        // Create an object to test with

        var origObj = TestData.dummyDefinitionForType(ObjectType.MODEL);
        var origAttrs = TestData.dummyAttrs();
        var origTagUpdates = TestData.tagUpdatesForAttrs(origAttrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(origObj)
                .addAllTagUpdate(origTagUpdates)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);

        // Random object ID, does not exist at all

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1))
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.readObject(readRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        // Try to read a non-existent version

        var v2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selectorForTag(tagHeader).toBuilder()
                .setObjectVersion(2))
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> readApi.readObject(v2MetadataReadRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());

        // Try to read a non-existent tag

        var t2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selectorForTag(tagHeader).toBuilder()
                .setTagVersion(2))
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error3 = assertThrows(StatusRuntimeException.class, () -> readApi.readObject(t2MetadataReadRequest));
        assertEquals(Status.Code.NOT_FOUND, error3.getStatus().getCode());
    }

    @Test
    void readBatch_missingItems() {

        // Create an object to test with

        var origObj = TestData.dummyDefinitionForType(ObjectType.MODEL);
        var origAttrs = TestData.dummyAttrs();
        var origTagUpdates = TestData.tagUpdatesForAttrs(origAttrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(origObj)
                .addAllTagUpdate(origTagUpdates)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);

        // Include one valid selector in the read batch request
        var tagHeader2 = writeApi.createObject(writeRequest);
        var validSelector = selectorForTag(tagHeader2);

        // Random object ID, does not exist at all

        var readRequest = MetadataBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addSelector(validSelector)
                .addSelector(TagSelector.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1))
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.readBatch(readRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        // Try to read a non-existent version

        var v2MetadataReadRequest = MetadataBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addSelector(validSelector)
                .addSelector(selectorForTag(tagHeader).toBuilder()
                .setObjectVersion(2))
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> readApi.readBatch(v2MetadataReadRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());

        // Try to read a non-existent tag

        var t2MetadataReadRequest = MetadataBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addSelector(validSelector)
                .addSelector(selectorForTag(tagHeader).toBuilder()
                .setTagVersion(2))
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error3 = assertThrows(StatusRuntimeException.class, () -> readApi.readBatch(t2MetadataReadRequest));
        assertEquals(Status.Code.NOT_FOUND, error3.getStatus().getCode());
    }

    @Test
    void readObject_wrongType() {

        // This should result in a failed pre-condition,
        // because failure depends on the contents of the metadata store

        var origObj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var origAttrs = TestData.dummyAttrs();
        var origTagUpdates = TestData.tagUpdatesForAttrs(origAttrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(origObj)
                .addAllTagUpdate(origTagUpdates)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selectorForTag(tagHeader).toBuilder()
                .setObjectType(ObjectType.MODEL))
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.readObject(readRequest));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @Test
    void readBatch_wrongType() {

        // This should result in a failed pre-condition,
        // because failure depends on the contents of the metadata store

        var origObj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var origAttrs = TestData.dummyAttrs();
        var origTagUpdates = TestData.tagUpdatesForAttrs(origAttrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(origObj)
                .addAllTagUpdate(origTagUpdates)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);

        // Include a valid selector in the read batch request
        var tagHeader2 = writeApi.createObject(writeRequest);
        var validSelector = selectorForTag(tagHeader2);

        var readRequest = MetadataBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addSelector(validSelector)
                .addSelector(selectorForTag(tagHeader).toBuilder()
                .setObjectType(ObjectType.MODEL))
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.readBatch(readRequest));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }
}
