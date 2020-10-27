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

import com.accenture.trac.common.api.meta.*;
import com.accenture.trac.common.metadata.*;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;
import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcIntegration;
import com.accenture.trac.svc.meta.test.JdbcUnit;
import com.accenture.trac.svc.meta.logic.MetadataReadLogic;

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
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private MetadataReadApiGrpc.MetadataReadApiBlockingStub readApi;
    private MetadataTrustedWriteApiGrpc.MetadataTrustedWriteApiBlockingStub writeApi;

    @BeforeEach
    void setup() throws Exception {

        var serverName = InProcessServerBuilder.generateName();

        var readLogic = new MetadataReadLogic(dal);
        var readApiImpl = new MetadataReadApi(readLogic);

        var writeLogic = new MetadataWriteLogic(dal);
        var writeApiImpl = new MetadataTrustedWriteApi(writeLogic);

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                 .forName(serverName)
                .directExecutor()
                .addService(readApiImpl)
                .addService(writeApiImpl)
                .build()
                .start());

        readApi = MetadataReadApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));

        writeApi = MetadataTrustedWriteApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void loadTag_ok(ObjectType objectType) {

        var origObj = TestData.dummyDefinitionForType(objectType);
        var attrs = TestData.dummyAttrs();
        var attrOps = TestData.attrOpsAddAll(attrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setDefinition(origObj)
                .putAllAttr(attrOps)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);
        var objectId = MetadataCodec.decode(tagHeader.getObjectId());

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var savedTag = readApi.loadTag(readRequest);

        var expectedTag = savedTag.newBuilderForType()
                .setHeader(tagHeader)
                .setDefinition(origObj)
                .putAllAttr(attrs)
                .build();

        assertEquals(objectId, MetadataCodec.decode(savedTag.getHeader().getObjectId()));
        assertEquals(expectedTag, savedTag);
    }

    @Test
    void loadTag_versioningOk() {

        var v1Obj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var v1Attrs = TestData.dummyAttrs();
        var v1AttrOps = TestData.attrOpsAddAll(v1Attrs);

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(v1Obj)
                .putAllAttr(v1AttrOps)
                .build();

        var v1Header = writeApi.createObject(v1WriteRequest);
        var v1Selector = selectorForTag(v1Header);
        var objectId = MetadataCodec.decode(v1Header.getObjectId());

        // No attr changes for v2
        var v2Obj = TestData.dummyVersionForType(v1Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .build();

        var v2Header = writeApi.updateObject(v2WriteRequest);
        var v2Selector = selectorForTag(v2Header);

        var t2Attrs = new HashMap<>(v1Attrs);
        t2Attrs.put("v3_attr", MetadataCodec.encodeValue("Use the force"));

        var t2AttrOps = Map.of("v3_attr", TagUpdate.newBuilder()
                .setValue(MetadataCodec.encodeValue("Use the force"))
                .build());

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v2Selector)
                .putAllAttr(t2AttrOps)
                .build();

        var t2Header = writeApi.updateTag(t2WriteRequest);

        var v1ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var v2ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(2)
                .setTagVersion(1)
                .build();

        var t2ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .setTagVersion(2)
                .build();

        var v1TagSaved = readApi.loadTag(v1ReadRequest);
        var v2TagSaved = readApi.loadTag(v2ReadRequest);
        var t2TagSaved = readApi.loadTag(t2ReadRequest);

        var v1TagExpected = v1TagSaved.newBuilderForType()
                .setHeader(v1Header)
                .setDefinition(v1Obj)
                .putAllAttr(v1Attrs)
                .build();

        var v2TagExpected = v2TagSaved.newBuilderForType()
                .setHeader(v2Header)
                .setDefinition(v2Obj)
                .putAllAttr(v1Attrs)
                .build();

        var t2TagExpected = t2TagSaved.newBuilderForType()
                .setHeader(t2Header)
                .setDefinition(v2Obj)
                .putAllAttr(t2Attrs)
                .build();

        assertEquals(v1TagExpected, v1TagSaved);
        assertEquals(v2TagExpected, v2TagSaved);
        assertEquals(t2TagExpected, t2TagSaved);
    }

    @Test
    void loadTag_missingItems() {

        // Random object ID, does not exist at all

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setObjectId(MetadataCodec.encode(java.util.UUID.randomUUID()))
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.loadTag(readRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        // Create an object to test with

        var origObj = TestData.dummyDefinitionForType(ObjectType.MODEL);
        var origAttrs = TestData.dummyAttrs();
        var origAttrOps = TestData.attrOpsAddAll(origAttrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(origObj)
                .putAllAttr(origAttrOps)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);
        var objectId = MetadataCodec.decode(tagHeader.getObjectId());

        // Try to read a non-existent version

        var v2ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(2)
                .setTagVersion(1)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> readApi.loadTag(v2ReadRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());

        // Try to read a non-existent tag

        var t2ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .setTagVersion(2)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error3 = assertThrows(StatusRuntimeException.class, () -> readApi.loadTag(t2ReadRequest));
        assertEquals(Status.Code.NOT_FOUND, error3.getStatus().getCode());
    }

    @Test
    void loadTag_wrongType() {

        // This should result in a failed pre-condition,
        // because failure depends on the contents of the metadata store

        var origObj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var origAttrs = TestData.dummyAttrs();
        var origAttrOps = TestData.attrOpsAddAll(origAttrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(origObj)
                .putAllAttr(origAttrOps)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);
        var objectId = MetadataCodec.decode(tagHeader.getObjectId());

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.loadTag(readRequest));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void loadLatestTag_ok(ObjectType objectType) {

        var origObj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var origAttrs = TestData.dummyAttrs();
        var origAttrOps = TestData.attrOpsAddAll(origAttrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setDefinition(origObj)
                .putAllAttr(origAttrOps)
                .build();

        var t1Header = writeApi.createObject(writeRequest);
        var t1Selector = TestData.selectorForTag(t1Header);
        var objectId = MetadataCodec.decode(t1Header.getObjectId());

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .build();

        var t1SavedTag = readApi.loadLatestTag(readRequest);

        var t2Attrs = new HashMap<>(origAttrs);
        t2Attrs.put("v3_attr", MetadataCodec.encodeValue("Use the force"));

        var t2AttrOps = Map.of("v3_attr", TagUpdate.newBuilder()
                .setValue(MetadataCodec.encodeValue("Use the force"))
                .build());

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setPriorVersion(t1Selector)
                .putAllAttr(t2AttrOps)
                .build();

        var t2Header = writeApi.updateTag(t2WriteRequest);
        var t2SavedTag = readApi.loadLatestTag(readRequest);

        var t2ExpectedTag = t2SavedTag.newBuilderForType()
                .setHeader(t2Header)
                .setDefinition(origObj)
                .putAllAttr(t2Attrs)
                .build();

        assertEquals(1, t1SavedTag.getHeader().getTagVersion());
        assertEquals(2, t2SavedTag.getHeader().getTagVersion());
        assertEquals(t2ExpectedTag, t2SavedTag);
    }
    
    @Test
    void loadLatestTag_versioningOk() {

        // Save first version

        var v1Obj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var v1Attrs = TestData.dummyAttrs();
        var v1AttrOps = TestData.attrOpsAddAll(v1Attrs);

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(v1Obj)
                .putAllAttr(v1AttrOps)
                .build();

        var v1Header = writeApi.createObject(v1WriteRequest);
        var v1Selector = TestData.selectorForTag(v1Header);
        var objectId = MetadataCodec.decode(v1Header.getObjectId());

        // Read first version back, should be T1

        var v1ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .build();

        var v1TagSaved = readApi.loadLatestTag(v1ReadRequest);

        var v1Expected = v1TagSaved.newBuilderForType()
                .setHeader(v1Header)
                .setDefinition(v1Obj)
                .putAllAttr(v1Attrs)
                .build();

        assertEquals(1, v1Header.getObjectVersion());
        assertEquals(1, v1Header.getTagVersion());
        assertEquals(v1Expected, v1TagSaved);

        // Save second tag on first version

        var t2Attrs = new HashMap<>(v1Attrs);
        t2Attrs.put("v3_attr", MetadataCodec.encodeValue("Use the force"));

        var t2AttrOps = Map.of("v3_attr", TagUpdate.newBuilder()
                .setValue(MetadataCodec.encodeValue("Use the force"))
                .build());

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .putAllAttr(t2AttrOps)
                .build();

        var t2Header = writeApi.updateTag(t2WriteRequest);
        var t2Selector = selectorForTag(t2Header);

        // Read first version back, should be T2

        var t2TagExpected = v1Expected.toBuilder()
                .setHeader(v1Expected.getHeader().toBuilder()
                .setTagVersion(2))
                .clearAttr()
                .putAllAttr(t2Attrs)
                .build();

        var t2TagSaved = readApi.loadLatestTag(v1ReadRequest);

        assertEquals(1, t2TagSaved.getHeader().getObjectVersion());
        assertEquals(2, t2TagSaved.getHeader().getTagVersion());
        assertEquals(t2TagExpected, t2TagSaved);

        // Save second version of object

        var v2Obj = TestData.dummyVersionForType(v1Obj);

        // V2 attrs are the same as v1 attrs
        // Attr ops removes the additional attribute added for t2
        var v2Attrs = new HashMap<>(v1Attrs);
        var v2AttrOps = t2AttrOps.keySet().stream()
                .collect(Collectors.toMap(
                Function.identity(),
                attrName -> TagUpdate.newBuilder().setOperation(TagOperation.DELETE_ATTR).build()));

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(t2Selector)
                .putAllAttr(v2AttrOps)
                .build();

        var v2Header = writeApi.updateObject(v2WriteRequest);

        // Read second version of object, should be V2 T1

        var v2ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(2)
                .build();

        var v2TagSaved = readApi.loadLatestTag(v2ReadRequest);

        var v2TagExpected = v2TagSaved.newBuilderForType()
                .setHeader(v2Header)
                .setDefinition(v2Obj)
                .putAllAttr(v2Attrs)
                .build();

        assertEquals(2, v2TagSaved.getHeader().getObjectVersion());
        assertEquals(1, v2TagSaved.getHeader().getTagVersion());
        assertEquals(v2TagExpected, v2TagSaved);

        // Now load latest tag on V1 again, should still be V1 T2

        t2TagSaved = readApi.loadLatestTag(v1ReadRequest);
        assertEquals(t2TagExpected, t2TagSaved);
    }

    @Test
    void loadLatestTag_missingItems() {

        // Random object ID, does not exist at all

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setObjectId(MetadataCodec.encode(java.util.UUID.randomUUID()))
                .setObjectVersion(1)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.loadLatestTag(readRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        // Create an object to test with

        var origObj = TestData.dummyDefinitionForType(ObjectType.MODEL);
        var origAttrs = TestData.dummyAttrs();
        var origAttrOps = TestData.attrOpsAddAll(origAttrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(origObj)
                .putAllAttr(origAttrOps)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);
        var objectId = MetadataCodec.decode(tagHeader.getObjectId());

        // Try to read a non-existent version

        var v2ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(2)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> readApi.loadLatestTag(v2ReadRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());
    }

    @Test
    void loadLatestTag_wrongType() {

        // This should result in a failed pre-condition,
        // because failure depends on the contents of the metadata store

        var origObj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var origAttrs = TestData.dummyAttrs();
        var origAttrOps = TestData.attrOpsAddAll(origAttrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(origObj)
                .putAllAttr(origAttrOps)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);
        var objectId = MetadataCodec.decode(tagHeader.getObjectId());

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.loadLatestTag(readRequest));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE, names = {
            "DATA",
            "CUSTOM"})
    void loadLatestObject_ok(ObjectType objectType) {

        // This test only applied for objects that support versioning

        var origObj = TestData.dummyDefinitionForType(objectType);
        var origAttrs = TestData.dummyAttrs();
        var origAttrOps = TestData.attrOpsAddAll(origAttrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setDefinition(origObj)
                .putAllAttr(origAttrOps)
                .build();

        var v1Header = writeApi.createObject(writeRequest);
        var v1Selector = selectorForTag(v1Header);
        var objectId = MetadataCodec.decode(v1Header.getObjectId());

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .build();

        var v2Obj = TestData.dummyVersionForType(origObj);
        var v2Attrs = new HashMap<>(origAttrs);
        v2Attrs.put("v2_attr", MetadataCodec.encodeValue("Use the force"));

        var v2AttrOps = Map.of("v2_attr", TagUpdate.newBuilder()
                .setValue(MetadataCodec.encodeValue("Use the force"))
                .build());

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .putAllAttr(v2AttrOps)
                .build();

        var v2Header = writeApi.updateObject(v2WriteRequest);
        var v2SavedTag = readApi.loadLatestObject(readRequest);

        var v2ExpectedTag = v2SavedTag.newBuilderForType()
                .setHeader(v2Header)
                .setDefinition(v2Obj)
                .putAllAttr(v2Attrs)
                .build();

        assertEquals(2, v2SavedTag.getHeader().getObjectVersion());
        assertEquals(v2ExpectedTag, v2SavedTag);
    }

    @Test
    void loadLatestObject_versionsOk() {

        // Save first version

        var v1Obj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var v1Attrs = TestData.dummyAttrs();
        var v1AttrOps = TestData.attrOpsAddAll(v1Attrs);

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(v1Obj)
                .putAllAttr(v1AttrOps)
                .build();

        var v1Header = writeApi.createObject(v1WriteRequest);
        var v1Selector = selectorForTag(v1Header);
        var objectId = MetadataCodec.decode(v1Header.getObjectId());

        // Read first version back, should be V1 T1

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setObjectId(MetadataCodec.encode(objectId))
                .build();

        var v1Saved = readApi.loadLatestObject(readRequest);

        var v1Expected = v1Saved.newBuilderForType()
                .setHeader(v1Header)
                .setDefinition(v1Obj)
                .putAllAttr(v1Attrs)
                .build();

        assertEquals(1, v1Saved.getHeader().getObjectVersion());
        assertEquals(1, v1Saved.getHeader().getTagVersion());
        assertEquals(v1Expected, v1Saved);

        // Save second tag on first version

        var t2Attrs = new HashMap<>(v1Attrs);
        t2Attrs.put("t2_attr", MetadataCodec.encodeValue("Use the force"));

        var t2AttrOps = Map.of("t2_attr", TagUpdate.newBuilder()
                .setValue(MetadataCodec.encodeValue("Use the force"))
                .build());

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .putAllAttr(t2AttrOps)
                .build();

        var t2Header = writeApi.updateTag(t2WriteRequest);
        var t2Selector = selectorForTag(t2Header);

        // Read first version back, should be T2

        var t2Saved = readApi.loadLatestObject(readRequest);

        var t2Expected = v1Expected.toBuilder()
                .setHeader(v1Expected.getHeader().toBuilder()
                .setTagVersion(2))
                .clearAttr().putAllAttr(t2Attrs)
                .build();

        assertEquals(1, v1Saved.getHeader().getObjectVersion());
        assertEquals(2, v1Saved.getHeader().getTagVersion());
        assertEquals(t2Expected, t2Saved);

        // Save second version of object

        var v2Obj = TestData.dummyVersionForType(v1Obj);

        // V2 attrs are the same as v1 attrs
        // Attr ops removes the additional attribute added for t2
        var v2Attrs = new HashMap<>(v1Attrs);
        var v2AttrOps = t2AttrOps.keySet().stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        attrName -> TagUpdate.newBuilder().setOperation(TagOperation.DELETE_ATTR).build()));

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(t2Selector)
                .setDefinition(v2Obj)
                .putAllAttr(v2AttrOps)
                .build();

        var v2Header = writeApi.updateObject(v2WriteRequest);

        // Read second version of object, should be V2 T1

        var v2Saved = readApi.loadLatestObject(readRequest);

        var v2Expected = v2Saved.newBuilderForType()
                .setHeader(v2Header)
                .setDefinition(v2Obj)
                .putAllAttr(v2Attrs)
                .build();

        assertEquals(2, v1Saved.getHeader().getObjectVersion());
        assertEquals(1, v1Saved.getHeader().getTagVersion());
        assertEquals(v2Expected, v2Saved);

        // Now save V1 T3, latest object should still be V2 T1

        var t3AttrOps = Map.of("t3_attr", TagUpdate.newBuilder()
                .setValue(MetadataCodec.encodeValue("The dark side"))
                .build());

        var t3WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(t2Selector)
                .putAllAttr(t3AttrOps)
                .build();

        // noinspection ResultOfMethodCallIgnored
        writeApi.updateTag(t3WriteRequest);

        v2Saved = readApi.loadLatestObject(readRequest);
        assertEquals(v2Expected, v2Saved);
    }

    @Test
    void loadLatestObject_missingItems() {

        // Random object ID, does not exist at all

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setObjectId(MetadataCodec.encode(java.util.UUID.randomUUID()))
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.loadLatestObject(readRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

    }

    @Test
    void loadLatestObject_wrongType() {

        // This should result in a failed pre-condition,
        // because failure depends on the contents of the metadata store

        var origObj = TestData.dummyDefinitionForType(ObjectType.DATA);
        var origAttrs = TestData.dummyAttrs();
        var origAttrOps = TestData.attrOpsAddAll(origAttrs);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(origObj)
                .putAllAttr(origAttrOps)
                .build();

        var tagHeader = writeApi.createObject(writeRequest);
        var objectId = MetadataCodec.decode(tagHeader.getObjectId());

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setObjectId(MetadataCodec.encode(objectId))
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.loadLatestObject(readRequest));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }
}
