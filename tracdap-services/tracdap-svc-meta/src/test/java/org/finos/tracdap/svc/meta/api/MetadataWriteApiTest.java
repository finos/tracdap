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

package org.finos.tracdap.svc.meta.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.TrustedMetadataApiGrpc;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.finos.tracdap.test.meta.TestData;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.finos.tracdap.test.meta.TestData.*;
import static org.junit.jupiter.api.Assertions.*;


abstract class MetadataWriteApiTest {

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TRAC_CONFIG_ENV_VAR = "TRAC_CONFIG_FILE";

    protected TracMetadataApiGrpc.TracMetadataApiBlockingStub readApi, publicApi;
    protected TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub trustedApi;

    // Include this test case as a unit test
    static class UnitTest extends MetadataWriteApiTest {

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
                .addTenant(TEST_TENANT)
                .startMeta()
                .build();

        @BeforeEach
        void setup() {
            readApi = publicApi = platform.metaClientBlocking();
            trustedApi = platform.metaClientTrustedBlocking();
        }
    }

    // Include this test case for integration against different database backends
    @org.junit.jupiter.api.Tag("integration")
    @org.junit.jupiter.api.Tag("int-metadb")
    static class IntegrationTest extends MetadataWriteApiTest {

        private static final String TRAC_CONFIG_ENV_FILE = System.getenv(TRAC_CONFIG_ENV_VAR);

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_ENV_FILE)
                .addTenant(TEST_TENANT)
                .runDbDeploy(false)
                .startMeta()
                .build();

        @BeforeEach
        void setup() {
            readApi = publicApi = platform.metaClientBlocking();
            trustedApi = platform.metaClientTrustedBlocking();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // WRITE BATCH
    // -----------------------------------------------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
            names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void writeBatch_trustedTypesOk(ObjectType objectType) {

        createObjectBatch_ok(objectType, trustedApi::writeBatch);
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE,
            names = {"SCHEMA", "FLOW", "CUSTOM"})
    void writeBatch_publicTypesOk(ObjectType objectType) {

        // All object types should be either in this test, or publicTypesNotAllowed

        createObjectBatch_ok(objectType, publicApi::writeBatch);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // CREATE OBJECT
    // -----------------------------------------------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void createObject_trustedTypesOk(ObjectType objectType) {

        createObject_ok(objectType, request -> trustedApi.createObject(request));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE,
                names = {"SCHEMA", "FLOW", "CUSTOM"})
    void createObject_publicTypesOk(ObjectType objectType) {

        // All object types should be either in this test, or publicTypesNotAllowed

        createObject_ok(objectType, request -> publicApi.createObject(request));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED", "SCHEMA", "FLOW", "CUSTOM"})
    void createObject_publicTypesNotAllowed(ObjectType objectType) {

        var objToSave = TestData.dummyDefinitionForType(objectType);
        var tagToSave = TestData.dummyTag(objToSave, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(tagToSave.getAttrsMap());

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setDefinition(objToSave)
                .addAllTagUpdates(tagUpdates)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.createObject(writeRequest));
        assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    }

    void createObject_ok(ObjectType objectType, Function<MetadataWriteRequest, TagHeader> saveApiCall) {

        var objToSave = TestData.dummyDefinitionForType(objectType);
        var tagToSave = TestData.dummyTag(objToSave, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(tagToSave.getAttrsMap());

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setDefinition(objToSave)
                .addAllTagUpdates(tagUpdates)
                .build();

        var tagHeader = saveApiCall.apply(writeRequest);
        var objectId = UUID.fromString(tagHeader.getObjectId());

        assertEquals(objectType, tagHeader.getObjectType());
        assertNotNull(objectId);
        assertNotEquals(new UUID(0, 0), objectId);
        assertEquals(1, tagHeader.getObjectVersion());
        assertEquals(1, tagHeader.getTagVersion());

        var expectedTag = tagToSave.toBuilder()
                .setHeader(tagHeader)
                .build();

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(1)
                .setTagVersion(1))
                .build();

        var tagFromStore = readApi.readObject(readRequest);

        assertEquals(expectedTag.getHeader(), tagFromStore.getHeader());
        assertEquals(expectedTag.getDefinition(), tagFromStore.getDefinition());

        for (var attr : expectedTag.getAttrsMap().keySet())
            assertEquals(expectedTag.getAttrsOrThrow(attr), tagFromStore.getAttrsOrThrow(attr));

        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_TIME));
        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_USER_ID));
        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_USER_NAME));
        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_TIME));
        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_ID));
        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_NAME));
    }

    @Test
    void createObject_inconsistentType() {

        // This test is about sending an invalid request, i.e. the payload does not match the write request

        // Make sure both types are allowed on the public API, so we don't get permission denied

        var objToSave = TestData.dummyDefinitionForType(ObjectType.CUSTOM);
        var tagToSave = TestData.dummyTag(objToSave, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(tagToSave.getAttrsMap());

        // Request to save a MODEL, even though the definition is for DATA
        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(objToSave)
                .addAllTagUpdates(tagUpdates)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.createObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.createObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void createObject_invalidContent() {

        var validFlow = TestData.dummyFlowDef();

        // Create a flow with an invalid node graph, this should get picked up by the validation layer

        var brokenEdges = validFlow.getFlow().toBuilder()
                .addEdges(FlowEdge.newBuilder()
                    .setTarget(FlowSocket.newBuilder().setNode("another_absent_node").setSocket("missing_socket"))
                    .setSource(FlowSocket.newBuilder().setNode("node_totally_not_present")))
                .build();

        var invalidFlow = validFlow.toBuilder()
                .setFlow(brokenEdges)
                .build();

        var tagToSave = TestData.dummyTag(invalidFlow, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(tagToSave.getAttrsMap());

        // Try to save the flow with a broken graph, should fail validation
        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(invalidFlow)
                .addAllTagUpdates(tagUpdates)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.createObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.createObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void createObject_invalidAttrs() {

        var objToSave = TestData.dummyDefinitionForType(ObjectType.CUSTOM);
        var validTag = TestData.dummyTag(objToSave, TestData.NO_HEADER);

        var invalidTag = validTag.toBuilder()
                .putAttrs("${escape_key}", MetadataCodec.encodeValue(1.0))
                .build();

        var tagUpdates = TestData.tagUpdatesForAttrs(invalidTag.getAttrsMap());

        // Request to save a MODEL, even though the definition is for DATA
        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setDefinition(objToSave)
                .addAllTagUpdates(tagUpdates)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.createObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> trustedApi.createObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void createObject_reservedAttrs() {

        var objToSave = TestData.dummyDefinitionForType(ObjectType.CUSTOM);
        var validTag = TestData.dummyTag(objToSave, TestData.NO_HEADER);

        var invalidTag = validTag.toBuilder()
                .putAttrs("trac_anything_reserved", MetadataCodec.encodeValue(1.0))
                .build();

        var tagUpdates = TestData.tagUpdatesForAttrs(invalidTag.getAttrsMap());

        // Request to save a MODEL, even though the definition is for DATA
        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setDefinition(objToSave)
                .addAllTagUpdates(tagUpdates)
                .build();

        // Setting reserved attributes is allowed through the trusted API but not the public API

        // At present this is enforced through validation, so it should come back as INVALID_ARGUMENT
        // In the future if public/trusted APIs are unified and reserved attrs are managed with permissions,
        // Then the result would be PERMISSION_DENIED instead

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.createObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        assertDoesNotThrow(() -> trustedApi.createObject(writeRequest));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // CREATE BATCH
    // -----------------------------------------------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED", "SCHEMA", "FLOW", "CUSTOM"})
    void createObjectBatch_publicTypesNotAllowed(ObjectType objectType) {

        List<MetadataWriteRequest> requests = new ArrayList<>();

        for (int i = 0; i < 7; i++) {

            var objToSave = TestData.dummyDefinitionForType(objectType);
            var tagToSave = TestData.dummyTag(objToSave, TestData.NO_HEADER);
            var tagUpdates = TestData.tagUpdatesForAttrs(tagToSave.getAttrsMap());

            requests.add(MetadataWriteRequest.newBuilder()
                    .setObjectType(objectType)
                    .setDefinition(objToSave)
                    .addAllTagUpdates(tagUpdates)
                    .build());
        }

        var writeRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllCreateObjects(requests)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.writeBatch(writeRequest));
        assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    }

    void createObjectBatch_ok(ObjectType objectType, Function<MetadataWriteBatchRequest, MetadataWriteBatchResponse> saveApiCall) {

        List<MetadataWriteRequest> requests = new ArrayList<>();
        List<Tag> tagsToSave = new ArrayList<>();

        for (int i = 0; i < 7; i++) {

            var objToSave = TestData.dummyDefinitionForType(objectType);
            var tagToSave = TestData.dummyTag(objToSave, TestData.NO_HEADER);
            var tagUpdates = TestData.tagUpdatesForAttrs(tagToSave.getAttrsMap());

            requests.add(MetadataWriteRequest.newBuilder()
                    .setObjectType(objectType)
                    .setDefinition(objToSave)
                    .addAllTagUpdates(tagUpdates)
                    .build());

            tagsToSave.add(tagToSave);
        }

        var writeRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllCreateObjects(requests)
                .build();

        var tagHeaders = saveApiCall.apply(writeRequest).getCreateObjectsList();
        assertEquals(7, tagHeaders.size());

        for (int i = 0; i < tagHeaders.size(); i++) {

            var tagHeader = tagHeaders.get(i);
            var tagToSave = tagsToSave.get(i);
            var objectId = UUID.fromString(tagHeader.getObjectId());

            assertEquals(objectType, tagHeader.getObjectType());
            assertNotNull(objectId);
            assertNotEquals(new UUID(0, 0), objectId);
            assertEquals(1, tagHeader.getObjectVersion());
            assertEquals(1, tagHeader.getTagVersion());

            var expectedTag = tagToSave.toBuilder()
                    .setHeader(tagHeader)
                    .build();

            var readRequest = MetadataReadRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSelector(TagSelector.newBuilder()
                            .setObjectType(objectType)
                            .setObjectId(objectId.toString())
                            .setObjectVersion(1)
                            .setTagVersion(1))
                    .build();

            var tagFromStore = readApi.readObject(readRequest);

            assertEquals(expectedTag.getHeader(), tagFromStore.getHeader());
            assertEquals(expectedTag.getDefinition(), tagFromStore.getDefinition());

            for (var attr : expectedTag.getAttrsMap().keySet())
                assertEquals(expectedTag.getAttrsOrThrow(attr), tagFromStore.getAttrsOrThrow(attr));

            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_TIME));
            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_USER_ID));
            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_USER_NAME));
            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_TIME));
            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_ID));
            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_NAME));
        }
    }

    @Test
    void createObjectBatch_inconsistentType() {

        // This test is about sending an invalid request, i.e. the payload does not match the write request

        // Make sure both types are allowed on the public API, so we don't get permission denied

        List<MetadataWriteRequest> requests = new ArrayList<>();

        for (int i = 0; i < 7; i++) {

            var objToSave = TestData.dummyDefinitionForType(ObjectType.CUSTOM);
            var tagToSave = TestData.dummyTag(objToSave, TestData.NO_HEADER);
            var tagUpdates = TestData.tagUpdatesForAttrs(tagToSave.getAttrsMap());

            // Request to save a MODEL, even though the definition is for DATA
            requests.add(MetadataWriteRequest.newBuilder()
                    .setObjectType(ObjectType.FLOW)
                    .setDefinition(objToSave)
                    .addAllTagUpdates(tagUpdates)
                    .build());
        }

        var writeRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllCreateObjects(requests)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.writeBatch(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.writeBatch(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void createObjectBatch_invalidContent() {

        List<MetadataWriteRequest> requests = new ArrayList<>();

        for (int i = 0; i < 7; i++) {

            var validFlow = TestData.dummyFlowDef();

            // Create a flow with an invalid node graph, this should get picked up by the validation layer

            var brokenEdges = validFlow.getFlow().toBuilder()
                    .addEdges(FlowEdge.newBuilder()
                    .setTarget(FlowSocket.newBuilder().setNode("another_absent_node").setSocket("missing_socket"))
                    .setSource(FlowSocket.newBuilder().setNode("node_totally_not_present")))
                    .build();

            var invalidFlow = validFlow.toBuilder()
                    .setFlow(brokenEdges)
                    .build();

            var tagToSave = TestData.dummyTag(invalidFlow, TestData.NO_HEADER);
            var tagUpdates = TestData.tagUpdatesForAttrs(tagToSave.getAttrsMap());

            // Try to save the flow with a broken graph, should fail validation
            requests.add(
                    MetadataWriteRequest.newBuilder()
                    .setObjectType(ObjectType.FLOW)
                    .setDefinition(invalidFlow)
                    .addAllTagUpdates(tagUpdates)
                    .build());
        }

        var writeRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllCreateObjects(requests)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.writeBatch(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.writeBatch(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void createObjectBatch_invalidAttrs() {

        List<MetadataWriteRequest> requests = new ArrayList<>();

        for (int i = 0; i < 7; i++) {

            var objToSave = TestData.dummyDefinitionForType(ObjectType.CUSTOM);
            var validTag = TestData.dummyTag(objToSave, TestData.NO_HEADER);

            var invalidTag = validTag.toBuilder()
                    .putAttrs("${escape_key}", MetadataCodec.encodeValue(1.0))
                    .build();

            var tagUpdates = TestData.tagUpdatesForAttrs(invalidTag.getAttrsMap());

            // Request to save a MODEL, even though the definition is for DATA
            requests.add(
                    MetadataWriteRequest.newBuilder()
                    .setObjectType(ObjectType.CUSTOM)
                    .setDefinition(objToSave)
                    .addAllTagUpdates(tagUpdates)
                    .build());
        }

        var writeRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllCreateObjects(requests)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.writeBatch(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> trustedApi.writeBatch(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void createObjectBatch_reservedAttrs() {

        List<MetadataWriteRequest> requests = new ArrayList<>();

        for (int i = 0; i < 7; i++) {

            var objToSave = TestData.dummyDefinitionForType(ObjectType.CUSTOM);
            var validTag = TestData.dummyTag(objToSave, TestData.NO_HEADER);

            var invalidTag = validTag.toBuilder()
                    .putAttrs("trac_anything_reserved", MetadataCodec.encodeValue(1.0))
                    .build();

            var tagUpdates = TestData.tagUpdatesForAttrs(invalidTag.getAttrsMap());

            // Request to save a MODEL, even though the definition is for DATA
            requests.add(
                    MetadataWriteRequest.newBuilder()
                    .setObjectType(ObjectType.CUSTOM)
                    .setDefinition(objToSave)
                    .addAllTagUpdates(tagUpdates)
                    .build());
        }

        var writeRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllCreateObjects(requests)
                .build();

        // Setting reserved attributes is allowed through the trusted API but not the public API

        // At present this is enforced through validation, so it should come back as INVALID_ARGUMENT
        // In the future if public/trusted APIs are unified and reserved attrs are managed with permissions,
        // Then the result would be PERMISSION_DENIED instead

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.writeBatch(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        assertDoesNotThrow(() -> trustedApi.writeBatch(writeRequest));
    }
    
    // -----------------------------------------------------------------------------------------------------------------
    // UPDATE OBJECT
    // -----------------------------------------------------------------------------------------------------------------

    // Versioned types, as listed in MetadataConstants.VERSIONED_OBJECT_TYPES
    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE,
                names = {"DATA", "FILE", "STORAGE", "SCHEMA", "CUSTOM"})
    void updateObject_trustedTypesOk(ObjectType objectType) {

        updateObject_ok(objectType, request -> trustedApi.updateObject(request));
    }

    // Versioned types that are also publicly writable
    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE,
                names = {"SCHEMA", "CUSTOM"})
    void updateObject_publicTypesOk(ObjectType objectType) {

        updateObject_ok(objectType, request -> publicApi.updateObject(request));
    }

    // Versioned types that are not publicly writable
    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE,
                names = {"DATA", "FILE", "STORAGE"})
    void updateObject_publicTypesNotAllowed(ObjectType objectType) {

        var v1SavedTag = updateObject_prepareV1(objectType);
        var v1Selector = selectorForTag(v1SavedTag);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    }

    // Object types not included in MetadataConstants.VERSIONED_OBJECT_TYPES
    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED", "DATA", "FILE", "STORAGE", "SCHEMA", "CUSTOM"})
    void updateObject_versionsNotSupported(ObjectType objectType) {

        var v1SavedTag = updateObject_prepareV1(objectType);
        var v1Selector = selectorForTag(v1SavedTag);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // Also check the public API for object types where public write is allowed
        if (objectType.equals(ObjectType.FLOW)) {

            var publicError = assertThrows(StatusRuntimeException.class, () -> publicApi.updateObject(v2WriteRequest));
            assertEquals(Status.Code.INVALID_ARGUMENT, publicError.getStatus().getCode());
        }
    }

    void updateObject_ok(ObjectType objectType, Function<MetadataWriteRequest, TagHeader> saveApiCall) {

        var v1SavedTag = updateObject_prepareV1(objectType);
        var v1Selector = selectorForTag(v1SavedTag);
        var v1ObjectId = UUID.fromString(v1SavedTag.getHeader().getObjectId());

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

        var v2NewAttrName = "update_object_v2_attr_" + objectType.name();
        var v2NewAttrValue = MetadataCodec.encodeValue(1.0);
        var v2AttrUpdate = TagUpdate.newBuilder()
                .setAttrName(v2NewAttrName)
                .setValue(v2NewAttrValue)
                .build();

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .addTagUpdates(v2AttrUpdate)
                .build();

        var v2TagHeader = saveApiCall.apply(v2WriteRequest);
        var v2ObjectId = UUID.fromString(v2TagHeader.getObjectId());

        assertEquals(v1ObjectId, v2ObjectId);
        assertEquals(2, v2TagHeader.getObjectVersion());
        assertEquals(1, v2TagHeader.getTagVersion());

        var expectedTag = Tag.newBuilder()
                .setHeader(v2TagHeader)
                .setDefinition(v2Obj)
                .putAllAttrs(v1SavedTag.getAttrsMap())
                .putAttrs(v2NewAttrName, v2NewAttrValue)
                .build();

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(v2ObjectId.toString())
                .setObjectVersion(2)
                .setTagVersion(1))
                .build();

        var tagFromStore = readApi.readObject(readRequest);

        assertEquals(expectedTag.getHeader(), tagFromStore.getHeader());
        assertEquals(expectedTag.getDefinition(), tagFromStore.getDefinition());

        for (var attr : expectedTag.getAttrsMap().keySet()) {

            // trac_update_  attrs are set on the original tag and changed by the update operation
            if (attr.startsWith("trac_update_"))
                continue;

            assertEquals(expectedTag.getAttrsOrThrow(attr), tagFromStore.getAttrsOrThrow(attr));
        }

        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_TIME));
        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_USER_ID));
        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_USER_NAME));
        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_TIME));
        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_ID));
        assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_NAME));
    }

    Tag updateObject_prepareV1(ObjectType objectType) {

        // Save and retrieve version 1, the saved version will have an object header filled in

        var v1Obj = TestData.dummyDefinitionForType(objectType);
        var v1Tag = TestData.dummyTag(v1Obj, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(v1Tag.getAttrsMap());

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setDefinition(v1Obj)
                .addAllTagUpdates(tagUpdates)
                .build();

        var v1IdResponse = trustedApi.createObject(v1WriteRequest);

        var v1MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(v1IdResponse.getObjectId())
                .setObjectVersion(v1IdResponse.getObjectVersion())
                .setTagVersion(v1IdResponse.getTagVersion()))
                .build();

        return readApi.readObject(v1MetadataReadRequest);
    }

    // For the remaining corner and error cases around versions, we use the CUSTOM object type
    // This is because CUSTOM objects support versions and can be saved in both the public and trusted API

    @Test
    void updateObject_latestUpdated() {

        // Make sure saving a new version updates the 'latest' flag for the object
        // Test both trusted and public APIs

        var v1SavedTag = updateObject_prepareV1(ObjectType.CUSTOM);
        var v1Selector = selectorForTag(v1SavedTag);
        var v1ObjectId = v1SavedTag.getHeader().getObjectId();

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(v1ObjectId)
                .setLatestObject(true)
                .setLatestTag(true))
                .build();

        var v1Latest = readApi.readObject(readRequest);
        assertEquals(1, v1Latest.getHeader().getObjectVersion());

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        // noinspection ResultOfMethodCallIgnored
        trustedApi.updateObject(v2WriteRequest);

        var v2Latest = readApi.readObject(readRequest);
        assertEquals(2, v2Latest.getHeader().getObjectVersion());

        var v2Selector = selectorForTag(v2Latest);
        var v3Obj = TestData.dummyVersionForType(v2Latest.getDefinition());

        var v3WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(v2Selector)
                .setDefinition(v3Obj)
                .build();

        // noinspection ResultOfMethodCallIgnored
        publicApi.updateObject(v3WriteRequest);

        var v3Latest = readApi.readObject(readRequest);
        assertEquals(3, v3Latest.getHeader().getObjectVersion());
    }

    @Test
    void updateObject_inconsistentType() {

        // This test is about sending an invalid request, i.e. the payload does not match the write request

        // Make sure both types are allowed on the public API, so we don't get permission denied

        var v1SavedTag = updateObject_prepareV1(ObjectType.CUSTOM);
        var v1Selector = selectorForTag(v1SavedTag);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void updateObject_wrongType() {

        // This test is about mis-matched types between versions, e.g. V1 is CUSTOM, V2 is DATA
        // This counts as a pre-condition failure rather than invalid input,
        // because it depends on existing data in the metadata store

        // Currently only testing the trusted API, as public does not support two types with versioning

        var v1SavedTag = updateObject_prepareV1(ObjectType.CUSTOM);
        var v2Obj = TestData.dummyDataDef();

        // First attempt - use a prior version selector that does not match the new object
        // This should be an invalid request (it is always invalid, regardless of what is in the DB)

        var v1Selector = selectorForTag(v1SavedTag);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // Second attempt - user a prior version selector that thinks the prior version is the right type
        // This is an invalid pre-condition (it only fails because the request does not match what is in the DB)

        var v1SelectorData = v1Selector.toBuilder()
                .setObjectType(ObjectType.DATA)
                .build();

        var v2WriteRequestData = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1SelectorData)
                .setDefinition(v2Obj)
                .build();

        var error2 = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateObject(v2WriteRequestData));
        assertEquals(Status.Code.FAILED_PRECONDITION, error2.getStatus().getCode());
    }

    @Test
    void updateObject_missingObject() {

        // V1 object is not saved
        var v1Tag = TestData.dummyTagForObjectType(ObjectType.CUSTOM);
        var v1Selector = selectorForTag(v1Tag);

        var v2Obj = TestData.dummyVersionForType(v1Tag.getDefinition());

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());
    }

    @Test
    void updateObject_missingObjectVersion() {

        // V1 object created and saved
        var v1SavedTag = updateObject_prepareV1(ObjectType.CUSTOM);
        var v1Selector = selectorForTag(v1SavedTag);

        // V2 object is not saved
        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());
        var v2Selector = v1Selector.toBuilder().setObjectVersion(2).build();

        var v3Obj = TestData.dummyVersionForType(v2Obj);

        var v3WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(v2Selector)
                .setDefinition(v3Obj)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateObject(v3WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.updateObject(v3WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());
    }

    @Test
    void updateObject_superseded() {

        // V1 object created and saved
        var v1SavedTag = updateObject_prepareV1(ObjectType.CUSTOM);
        var v1Selector = selectorForTag(v1SavedTag);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        // Save V2 once should be ok

        assertDoesNotThrow(() -> trustedApi.updateObject(v2WriteRequest));

        // Trying to create V2 a second time is an error

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.ALREADY_EXISTS, error2.getStatus().getCode());
    }

    @Test
    void updateObject_invalidContent() {

        // V1 object created and saved
        var v1SavedTag = updateObject_prepareV1(ObjectType.DATA);
        var v1Selector = selectorForTag(v1SavedTag);
        var v1Schema = v1SavedTag.getDefinition().getData().getSchema();

        // Create a V2 data definition that is invalid, use an explicit fieldOrder = -1

        var v2Schema = v1Schema.toBuilder()
                .setTable(v1Schema.getTable().toBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("some_new_field")
                        .setFieldType(BasicType.STRING)
                        .setFieldOrder(-1)));

        var v2Obj = v1SavedTag.getDefinition().toBuilder()
                .setData(v1SavedTag.getDefinition().getData()
                    .toBuilder()
                    .setSchema(v2Schema))
                .build();

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void updateObject_invalidAttrs() {

        var v1SavedTag = updateObject_prepareV1(ObjectType.CUSTOM);
        var v1Selector = selectorForTag(v1SavedTag);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

        var tagUpdate = TagUpdate.newBuilder()
                .setAttrName("very\nbroken.attr")
                .setValue(MetadataCodec.encodeValue(1.0))
                .build();

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .addTagUpdates(tagUpdate)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void updateObject_reservedAttrs() {

        var v1SavedTag = updateObject_prepareV1(ObjectType.CUSTOM);
        var v1Selector = selectorForTag(v1SavedTag);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

        var tagUpdate = TagUpdate.newBuilder()
                .setAttrName("trac_anything_reserved")
                .setValue(MetadataCodec.encodeValue(1.0))
                .build();

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .addTagUpdates(tagUpdate)
                .build();

        // Setting reserved attributes is allowed through the trusted API but not the public API

        // At present this is enforced through validation, so it should come back as INVALID_ARGUMENT
        // In the future if public/trusted APIs are unified and reserved attrs are managed with permissions,
        // Then the result would be PERMISSION_DENIED instead

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.updateObject(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        assertDoesNotThrow(() -> trustedApi.updateObject(v2WriteRequest));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // UPDATE BATCH
    // -----------------------------------------------------------------------------------------------------------------

    // Versioned types, as listed in MetadataConstants.VERSIONED_OBJECT_TYPES
    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE,
                names = {"DATA", "FILE", "STORAGE", "SCHEMA", "CUSTOM"})
    void updateObjectBatch_trustedTypesOk(ObjectType objectType) {

        updateObjectBatch_ok(objectType, trustedApi::writeBatch);
    }

    // Versioned types that are also publicly writable
    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE,
                names = {"SCHEMA", "CUSTOM"})
    void updateObjectBatch_publicTypesOk(ObjectType objectType) {

        updateObjectBatch_ok(objectType, publicApi::writeBatch);
    }

    // Versioned types that are not publicly writable
    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE,
                names = {"DATA", "FILE", "STORAGE"})
    void updateObjectBatch_publicTypesNotAllowed(ObjectType objectType) {

        List<MetadataWriteRequest> requests = new ArrayList<>();

        for (int i = 0; i < 7; i++) {

            var v1SavedTag = updateObject_prepareV1(objectType);
            var v1Selector = selectorForTag(v1SavedTag);

            var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

            requests.add(
                    MetadataWriteRequest.newBuilder()
                    .setObjectType(objectType)
                    .setPriorVersion(v1Selector)
                    .setDefinition(v2Obj)
                    .build());
        }

        var v2WriteRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllUpdateObjects(requests)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.writeBatch(v2WriteRequest));
        assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    }

    void updateObjectBatch_ok(ObjectType objectType, Function<MetadataWriteBatchRequest, MetadataWriteBatchResponse> saveApiCall) {

        class RequestData {
            MetadataWriteRequest writeRequest;
            UUID v1ObjectId;
            ObjectDefinition v2Obj;
            Tag v1SavedTag;
            String v2NewAttrName;
            Value v2NewAttrValue;
        }

        List<RequestData> requests = new ArrayList<>();

        for (int i = 0; i < 7; i++) {

            var r = new RequestData();

            r.v1SavedTag = updateObject_prepareV1(objectType);
            var v1Selector = selectorForTag(r.v1SavedTag);
            r.v1ObjectId = UUID.fromString(r.v1SavedTag.getHeader().getObjectId());

            r.v2Obj = TestData.dummyVersionForType(r.v1SavedTag.getDefinition());

            r.v2NewAttrName = "update_batch_v2_attr_" + objectType.name();
            r.v2NewAttrValue = MetadataCodec.encodeValue(1.0);
            var v2AttrUpdate = TagUpdate.newBuilder()
                    .setAttrName(r.v2NewAttrName)
                    .setValue(r.v2NewAttrValue)
                    .build();

            r.writeRequest = MetadataWriteRequest.newBuilder()
                            .setObjectType(objectType)
                            .setPriorVersion(v1Selector)
                            .setDefinition(r.v2Obj)
                            .addTagUpdates(v2AttrUpdate)
                            .build();
            requests.add(r);
        }

        var updateRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllUpdateObjects(requests.stream().map(r -> r.writeRequest).collect(Collectors.toList()))
                .build();

        var v2TagHeaders = saveApiCall.apply(updateRequest).getUpdateObjectsList();

        assertEquals(7, v2TagHeaders.size());

        for (int i = 0; i < v2TagHeaders.size(); i++) {

            var v2TagHeader = v2TagHeaders.get(i);
            var r = requests.get(i);

            var v2ObjectId = UUID.fromString(v2TagHeader.getObjectId());

            assertEquals(r.v1ObjectId, v2ObjectId);
            assertEquals(2, v2TagHeader.getObjectVersion());
            assertEquals(1, v2TagHeader.getTagVersion());

            var expectedTag = Tag.newBuilder()
                    .setHeader(v2TagHeader)
                    .setDefinition(r.v2Obj)
                    .putAllAttrs(r.v1SavedTag.getAttrsMap())
                    .putAttrs(r.v2NewAttrName, r.v2NewAttrValue)
                    .build();

            var readRequest = MetadataReadRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSelector(TagSelector.newBuilder()
                            .setObjectType(objectType)
                            .setObjectId(v2ObjectId.toString())
                            .setObjectVersion(2)
                            .setTagVersion(1))
                    .build();

            var tagFromStore = readApi.readObject(readRequest);

            assertEquals(expectedTag.getHeader(), tagFromStore.getHeader());
            assertEquals(expectedTag.getDefinition(), tagFromStore.getDefinition());

            for (var attr : expectedTag.getAttrsMap().keySet()) {

                // trac_update_  attrs are set on the original tag and changed by the update operation
                if (attr.startsWith("trac_update_"))
                    continue;

                assertEquals(expectedTag.getAttrsOrThrow(attr), tagFromStore.getAttrsOrThrow(attr));
            }

            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_TIME));
            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_USER_ID));
            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_CREATE_USER_NAME));
            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_TIME));
            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_ID));
            assertTrue(tagFromStore.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_NAME));
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // UPDATE TAG
    // -----------------------------------------------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void updateTag_AllTypesOk(ObjectType objectType) {

        var v1SavedTag = updateObject_prepareV1(objectType);
        var v1Selector = selectorForTag(v1SavedTag);
        var v1ObjectId = UUID.fromString(v1SavedTag.getHeader().getObjectId());

        // Write tag update via the trusted API

        var t2AttrName = "extra_attr_v2";
        var t2Update = TagUpdate.newBuilder()
                .setAttrName("extra_attr_v2")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setPriorVersion(v1Selector)
                .addTagUpdates(t2Update)
                .build();

        var t2header = trustedApi.updateTag(t2WriteRequest);

        assertEquals(objectType, t2header.getObjectType());
        assertEquals(v1ObjectId, UUID.fromString(t2header.getObjectId()));
        assertEquals(1, t2header.getObjectVersion());
        assertEquals(2, t2header.getTagVersion());

        var t2ExpectedTag = v1SavedTag.toBuilder()
                .setHeader(t2header)
                .putAttrs(t2AttrName, t2Update.getValue())
                .build();

        var t2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(v1ObjectId.toString())
                .setObjectVersion(1)
                .setTagVersion(2))
                .build();

        var t2SavedTag = readApi.readObject(t2MetadataReadRequest);
        var t2Selector = selectorForTag(t2SavedTag);

        assertEquals(t2ExpectedTag, t2SavedTag);

        // Write tag update via the public API

        var t3AttrName = "extra_attr_v3";
        var t3Update = TagUpdate.newBuilder()
                .setAttrName("extra_attr_v3")
                .setValue(MetadataCodec.encodeValue("Second extra attr"))
                .build();

        var t3WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setPriorVersion(t2Selector)
                .addTagUpdates(t3Update)
                .build();

        var t3Header = publicApi.updateTag(t3WriteRequest);

        assertEquals(objectType, t3Header.getObjectType());
        assertEquals(v1ObjectId, UUID.fromString(t3Header.getObjectId()));
        assertEquals(1, t3Header.getObjectVersion());
        assertEquals(3, t3Header.getTagVersion());

        var t3MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(v1ObjectId.toString())
                .setObjectVersion(1)
                .setTagVersion(3))
                .build();

        var t3ExpectedTag = t2SavedTag.toBuilder()
                .setHeader(t3Header)
                .putAttrs(t3AttrName, t3Update.getValue())
                .build();

        var t3SavedTag = readApi.readObject(t3MetadataReadRequest);

        assertEquals(t3ExpectedTag, t3SavedTag);
    }

    @Test
    void updateTag_isLatestObject_ok() {

        var v1SavedTag = updateObject_prepareV1(ObjectType.DATA);
        var v1Selector = selectorForTag(v1SavedTag);
        var v1Header = v1SavedTag.getHeader();

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var v2Tag = trustedApi.updateObject(v2WriteRequest);

        var v1MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setObjectId(v1Header.getObjectId())
                        .setObjectVersion(1)
                        .setTagVersion(1))
                .build();

        var v2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setObjectId(v1Header.getObjectId())
                        .setObjectVersion(2)
                        .setTagVersion(1))
                .build();

        var v1ReadObject = readApi.readObject(v1MetadataReadRequest);
        var v2ReadObject = readApi.readObject(v2MetadataReadRequest);

        assertTrue(v2ReadObject.getHeader().getIsLatestObject());
        assertTrue(v2Tag.getIsLatestObject());
        assertFalse(v1ReadObject.getHeader().getIsLatestObject());
    }

    @Test
    void updateTag_isLatestTag_ok() {

        var v1SavedTag = updateObject_prepareV1(ObjectType.DATA);
        var v1Selector = selectorForTag(v1SavedTag);
        var v1Header = v1SavedTag.getHeader();

        var t2Update = TagUpdate.newBuilder()
                .setAttrName("extra_attr_v2")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var v1t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdates(t2Update)
                .build();

        var v1t2Tag = trustedApi.updateTag(v1t2WriteRequest);

        var v1t1MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setObjectId(v1Header.getObjectId())
                        .setObjectVersion(1)
                        .setTagVersion(1))
                .build();

        var v1t2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setObjectId(v1Header.getObjectId())
                        .setObjectVersion(1)
                        .setTagVersion(2))
                .build();

        var v1t1ReadHeader = readApi.readObject(v1t1MetadataReadRequest).getHeader();
        var v1t2ReadHeader = readApi.readObject(v1t2MetadataReadRequest).getHeader();

        assertTrue(v1t2ReadHeader.getIsLatestTag());
        assertTrue(v1t2Tag.getIsLatestTag());
        assertFalse(v1t1ReadHeader.getIsLatestTag());
    }

    @Test
    void updateTag_isLatestTagAndObject_ok() {

        var v1SavedTag = updateObject_prepareV1(ObjectType.DATA);
        var v1Selector = selectorForTag(v1SavedTag);
        var v1Header = v1SavedTag.getHeader();

        var t2Update = TagUpdate.newBuilder()
                .setAttrName("extra_attr_v2")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var v1t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdates(t2Update)
                .build();

        var v1t2Tag = trustedApi.updateTag(v1t2WriteRequest);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var v2Tag = trustedApi.updateObject(v2WriteRequest);

        var v1t1MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setObjectId(v1Header.getObjectId())
                        .setObjectVersion(1)
                        .setTagVersion(1))
                .build();

        var v1t2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setObjectId(v1Header.getObjectId())
                        .setObjectVersion(1)
                        .setTagVersion(2))
                .build();

        var v2t1MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setObjectId(v1Header.getObjectId())
                        .setObjectVersion(2)
                        .setTagVersion(1))
                .build();

        var v1t1ReadHeader = readApi.readObject(v1t1MetadataReadRequest).getHeader();
        var v1t2ReadHeader = readApi.readObject(v1t2MetadataReadRequest).getHeader();
        var v2t1ReadHeader = readApi.readObject(v2t1MetadataReadRequest).getHeader();

        assertTrue(v1t2ReadHeader.getIsLatestTag());
        assertFalse(v1t1ReadHeader.getIsLatestTag());
        assertTrue(v2t1ReadHeader.getIsLatestTag());
        assertTrue(v1t2Tag.getIsLatestTag());
        assertTrue(v2Tag.getIsLatestObject());
        assertFalse(v1t1ReadHeader.getIsLatestObject());
        assertFalse(v1t2ReadHeader.getIsLatestObject());
        assertTrue(v2t1ReadHeader.getIsLatestObject());
    }

    @Test
    void updateTag_latestUpdated() {

        var v1SavedTag = updateObject_prepareV1(ObjectType.DATA);
        var v1Selector = selectorForTag(v1SavedTag);
        var v1Header = v1SavedTag.getHeader();

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition());

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(v2Obj)
                .build();

        var v2Header = trustedApi.updateObject(v2WriteRequest);

        assertEquals(ObjectType.DATA, v2Header.getObjectType());
        assertEquals(v1Header.getObjectId(), v2Header.getObjectId());
        assertEquals(2, v2Header.getObjectVersion());
        assertEquals(1, v2Header.getTagVersion());

        var t2Update = TagUpdate.newBuilder()
                .setAttrName("extra_attr_v2")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var v1t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdates(t2Update)
                .build();

        var v1t2Header = trustedApi.updateTag(v1t2WriteRequest);

        assertEquals(ObjectType.DATA, v1t2Header.getObjectType());
        assertEquals(v1Header.getObjectId(), v1t2Header.getObjectId());
        assertEquals(1, v1t2Header.getObjectVersion());
        assertEquals(2, v1t2Header.getTagVersion());

        var v1MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(v1Header.getObjectId())
                .setObjectVersion(1)
                .setLatestTag(true))
                .build();

        var v2MetadataReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(v1Header.getObjectId())
                .setObjectVersion(2)
                .setLatestTag(true))
                .build();

        var v1LatestTag = readApi.readObject(v1MetadataReadRequest);
        var v2latestTag = readApi.readObject(v2MetadataReadRequest);

        assertEquals(2, v1LatestTag.getHeader().getTagVersion());
        assertEquals(1, v2latestTag.getHeader().getTagVersion());
    }

    @Test
    void updateTag_inconsistentType() {

        // This test is about sending an invalid request, i.e. the payload does not match the write request

        // Make sure both types are allowed on the public API, so we don't get permission denied

        var v1SavedTag = updateObject_prepareV1(ObjectType.DATA);
        var v1Selector = selectorForTag(v1SavedTag);

        var t2Update = TagUpdate.newBuilder()
                .setAttrName("extra_attr_v2")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setPriorVersion(v1Selector)
                .addTagUpdates(t2Update)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateTag(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.updateTag(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }



    @Test
    void updateTag_wrongType() {

        // This test is about mis-matched types between versions, e.g. V1 is CUSTOM, V2 is DATA
        // This counts as a pre-condition failure rather than invalid input,
        // because it depends on existing data in the metadata store

        // Currently only testing the trusted API, as public does not support two types with versioning

        var v1SavedTag = updateObject_prepareV1(ObjectType.CUSTOM);

        var v1Selector = selectorForTag(v1SavedTag).toBuilder()
                .setObjectType(ObjectType.DATA)
                .build();

        var t2Update = TagUpdate.newBuilder()
                .setAttrName("extra_attr_v2")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdates(t2Update)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateTag(t2WriteRequest));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.updateTag(t2WriteRequest));
        assertEquals(Status.Code.FAILED_PRECONDITION, error2.getStatus().getCode());
    }

    @Test
    void updateTag_missingObject() {

        var v1Obj = TestData.dummyDataDef();
        var v1Tag = TestData.dummyTag(v1Obj, INCLUDE_HEADER);
        var v1Selector = selectorForTag(v1Tag);

        var t2Update = TagUpdate.newBuilder()
                .setAttrName("extra_attr_v2")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdates(t2Update)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateTag(t2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.updateTag(t2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());
    }

    @Test
    void updateTag_missingObjectVersion() {

        var v1SavedTag = updateObject_prepareV1(ObjectType.DATA);

        var v2Selector = selectorForTag(v1SavedTag).toBuilder()
                .setObjectVersion(2)
                .build();

        var t2Update = TagUpdate.newBuilder()
                .setAttrName("extra_attr_v2")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var v2t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v2Selector)
                .addTagUpdates(t2Update)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateTag(v2t2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.updateTag(v2t2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());
    }

    @Test
    void updateTag_missingTagVersion() {

        var v1SavedTag = updateObject_prepareV1(ObjectType.DATA);

        var t2Selector = selectorForTag(v1SavedTag).toBuilder()
                .setTagVersion(2)
                .build();

        var t2Update = TagUpdate.newBuilder()
                .setAttrName("extra_attr_v2")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var t3WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(t2Selector)
                .addTagUpdates(t2Update)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateTag(t3WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.updateTag(t3WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());
    }

    @Test
    void updateTag_superseded() {

        var v1SavedTag = updateObject_prepareV1(ObjectType.DATA);
        var v1Selector = selectorForTag(v1SavedTag);

        var t2Update = TagUpdate.newBuilder()
                .setAttrName("extra_attr_v2")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var v2t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdates(t2Update)
                .build();

        // Saving tag version 2 should succeed the first time

        assertDoesNotThrow(() -> trustedApi.updateTag(v2t2WriteRequest));

        // Trying to save tag version 2 a second time is an error

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateTag(v2t2WriteRequest));
        assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.updateTag(v2t2WriteRequest));
        assertEquals(Status.Code.ALREADY_EXISTS, error2.getStatus().getCode());
    }

    @Test
    void updateTag_invalidAttrs() {

        var v1SavedTag = updateObject_prepareV1(ObjectType.CUSTOM);
        var v1Selector = selectorForTag(v1SavedTag);

        var t2Update = TagUpdate.newBuilder()
                .setAttrName("no-hyphens")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(v1Selector)
                .addTagUpdates(t2Update)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.updateTag(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var error2 = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateTag(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void updateTag_reservedAttrs() {

        var v1SavedTag = updateObject_prepareV1(ObjectType.CUSTOM);
        var v1Selector = selectorForTag(v1SavedTag);

        var t2Update = TagUpdate.newBuilder()
                .setAttrName("trac_anything_reserved")
                .setValue(MetadataCodec.encodeValue("First extra attr"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(v1Selector)
                .addTagUpdates(t2Update)
                .build();

        // Setting reserved attributes is allowed through the trusted API but not the public API

        // At present this is enforced through validation, so it should come back as INVALID_ARGUMENT
        // In the future if public/trusted APIs are unified and reserved attrs are managed with permissions,
        // Then the result would be PERMISSION_DENIED instead

        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.updateTag(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        assertDoesNotThrow(() -> trustedApi.updateTag(t2WriteRequest));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // UPDATE BATCH TAG
    // -----------------------------------------------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void updateTagBatch_AllTypesOk(ObjectType objectType) {

        class RequestData {

            MetadataWriteRequest writeRequest;

            UUID v1ObjectId;
            Tag v1SavedTag;

            String t2AttrName;
            TagUpdate t2Update;
            public Tag t2SavedTag;
            TagSelector t2Selector;

            public String t3AttrName;
            public MetadataWriteRequest t3WriteRequest;
            public TagUpdate t3Update;

        }

        List<RequestData> requests = new ArrayList<>();

        for (int i = 0; i < 7; i++) {
            var r = new RequestData();

            r.v1SavedTag = updateObject_prepareV1(objectType);
            var v1Selector = selectorForTag(r.v1SavedTag);
            r.v1ObjectId = UUID.fromString(r.v1SavedTag.getHeader().getObjectId());

            // Write tag update via the trusted API

            r.t2AttrName = "extra_attr_v2";
            r.t2Update = TagUpdate.newBuilder()
                    .setAttrName("extra_attr_v2")
                    .setValue(MetadataCodec.encodeValue("First extra attr"))
                    .build();

            r.writeRequest = MetadataWriteRequest.newBuilder()
                    .setObjectType(objectType)
                    .setPriorVersion(v1Selector)
                    .addTagUpdates(r.t2Update)
                    .build();

            requests.add(r);
        }

        var t2WriteRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllUpdateTags(requests.stream().map(r -> r.writeRequest).collect(Collectors.toList()))
                .build();

        var t2headers = trustedApi.writeBatch(t2WriteRequest).getUpdateTagsList();

        assertEquals(7, t2headers.size());

        for (int i = 0; i < 7; i++) {

            var r = requests.get(i);
            var t2header = t2headers.get(i);

            assertEquals(objectType, t2header.getObjectType());
            assertEquals(r.v1ObjectId, UUID.fromString(t2header.getObjectId()));
            assertEquals(1, t2header.getObjectVersion());
            assertEquals(2, t2header.getTagVersion());

            var t2ExpectedTag = r.v1SavedTag.toBuilder()
                    .setHeader(t2header)
                    .putAttrs(r.t2AttrName, r.t2Update.getValue())
                    .build();

            var t2MetadataReadRequest = MetadataReadRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSelector(TagSelector.newBuilder()
                            .setObjectType(objectType)
                            .setObjectId(r.v1ObjectId.toString())
                            .setObjectVersion(1)
                            .setTagVersion(2))
                    .build();

            r.t2SavedTag = readApi.readObject(t2MetadataReadRequest);
            r.t2Selector = selectorForTag(r.t2SavedTag);

            assertEquals(t2ExpectedTag, r.t2SavedTag);
        }

        // Write tag update via the public API
        for (int i = 0; i < 7; i++) {
            var r = requests.get(i);

            r.t3AttrName = "extra_attr_v3";
            r.t3Update = TagUpdate.newBuilder()
                    .setAttrName("extra_attr_v3")
                    .setValue(MetadataCodec.encodeValue("Second extra attr"))
                    .build();

            r.t3WriteRequest = MetadataWriteRequest.newBuilder()
                    .setObjectType(objectType)
                    .setPriorVersion(r.t2Selector)
                    .addTagUpdates(r.t3Update)
                    .build();
        }

        var t3WriteRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllUpdateTags(
                        requests.stream().map(r -> r.t3WriteRequest).collect(Collectors.toList())
                )
                .build();

        var t3Headers = publicApi.writeBatch(t3WriteRequest).getUpdateTagsList();

        assertEquals(7, t3Headers.size());

        for (int i = 0; i < 7; i++) {

            var r = requests.get(i);
            var t3Header = t3Headers.get(i);

            assertEquals(objectType, t3Header.getObjectType());
            assertEquals(r.v1ObjectId, UUID.fromString(t3Header.getObjectId()));
            assertEquals(1, t3Header.getObjectVersion());
            assertEquals(3, t3Header.getTagVersion());

            var t3MetadataReadRequest = MetadataReadRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSelector(TagSelector.newBuilder()
                            .setObjectType(objectType)
                            .setObjectId(r.v1ObjectId.toString())
                            .setObjectVersion(1)
                            .setTagVersion(3))
                    .build();

            var t3ExpectedTag = r.t2SavedTag.toBuilder()
                    .setHeader(t3Header)
                    .putAttrs(r.t3AttrName, r.t3Update.getValue())
                    .build();

            var t3SavedTag = readApi.readObject(t3MetadataReadRequest);

            assertEquals(t3ExpectedTag, t3SavedTag);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // PREALLOCATION
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void preallocateObject_ok() {

        // Simple round trip
        // Preallocate an ID, save an object to that ID and read it back

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        var preallocateHeader = trustedApi.preallocateId(preallocateRequest);
        var preallocateSelector = selectorForTag(preallocateHeader);

        var newObject = TestData.dummyDefinitionForType(ObjectType.DATA);
        var newTag = TestData.dummyTag(newObject, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(newTag.getAttrsMap());

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(preallocateSelector)
                .setDefinition(newObject)
                .addAllTagUpdates(tagUpdates)
                .build();

        var tagHeader = trustedApi.createPreallocatedObject(writeRequest);

        assertEquals(preallocateHeader.getObjectId(), tagHeader.getObjectId());

        var expectedTag = newTag.toBuilder()
                .setHeader(tagHeader)
                .build();

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(preallocateHeader.getObjectId())
                .setObjectVersion(1)
                .setTagVersion(1))
                .build();

        var savedTag = readApi.readObject(readRequest);

        assertEquals(expectedTag.getHeader(), savedTag.getHeader());
        assertEquals(expectedTag.getDefinition(), savedTag.getDefinition());

        for (var attr : expectedTag.getAttrsMap().keySet())
            assertEquals(expectedTag.getAttrsOrThrow(attr), savedTag.getAttrsOrThrow(attr));

        assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_CREATE_TIME));
        assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_CREATE_USER_ID));
        assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_CREATE_USER_NAME));
        assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_UPDATE_TIME));
        assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_ID));
        assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_NAME));
    }

    @Test
    void preallocateObject_idNotReserved() {

        // To save a preallocated object, the ID must first be reserved
        // If the ID is not reserved, that is an item not found error

        var newObjectId = UUID.randomUUID();
        var selector = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(newObjectId.toString())
                .setObjectVersion(0)
                .setTagVersion(0)
                .build();

        var newObject = TestData.dummyDefinitionForType(ObjectType.DATA);
        var newTag = TestData.dummyTag(newObject, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(newTag.getAttrsMap());

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(selector)
                .setDefinition(newObject)
                .addAllTagUpdates(tagUpdates)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.createPreallocatedObject(writeRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void preallocateObject_saveDuplicate() {

        // A preallocated ID can only be saved once
        // If the preallocated ID has already been used, that is a duplicate item error (already exists)

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        var preallocateHeader = trustedApi.preallocateId(preallocateRequest);
        var preallocateSelector = selectorForTag(preallocateHeader);

        var newObject = TestData.dummyDefinitionForType(ObjectType.DATA);
        var newTag = TestData.dummyTag(newObject, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(newTag.getAttrsMap());

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(preallocateSelector)
                .setDefinition(newObject)
                .addAllTagUpdates(tagUpdates)
                .build();

        // The first save request should succeed
        var tagHeader = trustedApi.createPreallocatedObject(writeRequest);
        assertEquals(preallocateHeader.getObjectId(), tagHeader.getObjectId());

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.createPreallocatedObject(writeRequest));
        assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
    }

    @Test
    void preallocateObject_saveWrongType() {

        // When a preallocated ID is reserved, the object type for that ID is set
        // If the object type saved does not match the preallocated type, that is a failed precondition

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        var preallocateHeader = trustedApi.preallocateId(preallocateRequest);
        var preallocateSelector = TagSelector.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(preallocateHeader.getObjectId())
                .setObjectVersion(0)
                .setTagVersion(0)
                .build();

        var newObject = TestData.dummyDefinitionForType(ObjectType.MODEL);
        var newTag = TestData.dummyTag(newObject, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(newTag.getAttrsMap());

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setPriorVersion(preallocateSelector)
                .setDefinition(newObject)
                .addAllTagUpdates(tagUpdates)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.createPreallocatedObject(writeRequest));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @Test
    void preallocateObject_saveInconsistentType() {

        // A save request where the object type does not match the write request type is considered invalid
        // This is just as true for preallocation as it is for a regular save

        // The difference between this and the previous test is
        // In this case, the save request is inconsistent within itself, so the request is invalid
        // In the prior case, the request could be valid depending on what is in the metadata store

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        var preallocateHeader = trustedApi.preallocateId(preallocateRequest);
        var preallocateSelector = selectorForTag(preallocateHeader);

        var newObject = TestData.dummyDefinitionForType(ObjectType.MODEL);
        var newTag = TestData.dummyTag(newObject, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(newTag.getAttrsMap());

        // Attempt one: object type matches definition, does not match selector

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setPriorVersion(preallocateSelector)
                .setDefinition(newObject)
                .addAllTagUpdates(tagUpdates)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.createPreallocatedObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // Attempt two: object type matches selector, does not match definition

        var writeRequest2 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(preallocateSelector)
                .setDefinition(newObject)
                .addAllTagUpdates(tagUpdates)
                .build();

        var error2 = assertThrows(StatusRuntimeException.class, () -> trustedApi.createPreallocatedObject(writeRequest2));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());

        // Attempt three: selector matches definition, does not match object type

        var selector3 = preallocateSelector.toBuilder()
                .setObjectType(ObjectType.MODEL)
                .build();

        var writeRequest3 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(selector3)
                .setDefinition(newObject)
                .addAllTagUpdates(tagUpdates)
                .build();

        var error3 = assertThrows(StatusRuntimeException.class, () -> trustedApi.createPreallocatedObject(writeRequest3));
        assertEquals(Status.Code.INVALID_ARGUMENT, error3.getStatus().getCode());
    }

    @Test
    void preallocateObject_saveInvalidContent() {

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .build();

        var preallocateHeader = trustedApi.preallocateId(preallocateRequest);
        var preallocateSelector = selectorForTag(preallocateHeader);

        var validSchema = TestData.dummySchemaDef();

        // Create a flow with an invalid node graph, this should get picked up by the validation layer

        var brokenTableSchema = validSchema.getSchema().toBuilder()
                .setTable(validSchema.getSchema().getTable().toBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("# invalid_identifier")
                        .setFieldType(BasicType.ARRAY)
                        .setFieldOrder(-1)
                        .setLabel("This is a totally invalid field")))
                .build();

        var invalidSchema = validSchema.toBuilder()
                .setSchema(brokenTableSchema)
                .build();

        var tagToSave = TestData.dummyTag(invalidSchema, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(tagToSave.getAttrsMap());

        // Try to save the flow with a broken graph, should fail validation
        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setPriorVersion(preallocateSelector)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(invalidSchema)
                .addAllTagUpdates(tagUpdates)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.createPreallocatedObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void preallocateObject_updateObjectBeforeSave() {

        // In order to update an object, the first version must exist
        // If the prior version does not exist, that is an item not found error
        // If an ID has been preallocated but nothing saved to it, that does not change the behaviour

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        var preallocateHeader = trustedApi.preallocateId(preallocateRequest);

        // Preallocate header does not set object/tag version (they will be zero)
        var v1Selector = selectorForTag(preallocateHeader).toBuilder()
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var newObject = TestData.dummyDefinitionForType(ObjectType.DATA);
        var newTag = TestData.dummyTag(newObject, TestData.NO_HEADER);
        var tagUpdates = TestData.tagUpdatesForAttrs(newTag.getAttrsMap());

        var newVersionRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .setDefinition(newObject)
                .addAllTagUpdates(tagUpdates)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateObject(newVersionRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void preallocateObject_updateTagBeforeSave() {

        // In order to update a tag, the object and tag must exist
        // If the prior tag does not exist, that is an item not found error
        // If an ID has been preallocated but nothing saved to it, that does not change the behaviour

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        var preallocateHeader = trustedApi.preallocateId(preallocateRequest);

        // Preallocate header does not set object/tag version (they will be zero)
        var v1Selector = selectorForTag(preallocateHeader).toBuilder()
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var t2TagUpdate = TagUpdate.newBuilder()
                .setAttrName("some_new_attr")
                .setValue(MetadataCodec.encodeValue(2.0))
                .build();

        var newTagRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(v1Selector)
                .addTagUpdates(t2TagUpdate)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.updateTag(newTagRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void preallocateObject_readBeforeSave() {

        // An object cannot be read if it has not been saved, even if the ID is preallocated
        // This should look like a regular item not found error

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        var preallocateHeader = trustedApi.preallocateId(preallocateRequest);

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(preallocateHeader.getObjectId())
                .setObjectVersion(1)
                .setTagVersion(1))
                .build();

        // Try reading with explicit version / tag, latest tag and latest version

        var error = assertThrows(StatusRuntimeException.class, () -> readApi.readObject(readRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    // -----------------------------------------------------------------------------------------------------------------
    // PREALLOCATION BATCH
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void preallocateObjectBatch_ok() {

        // Simple round trip
        // Preallocate IDs, save objects to those IDs and read then back

        var preallocateRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllPreallocateIds(
                        IntStream.range(0, 13)
                                .mapToObj(i -> MetadataWriteRequest.newBuilder()
                                        .setObjectType(ObjectType.DATA)
                                        .build())
                                .collect(Collectors.toList())
                )
                .build();

        var preallocatedHeaders = trustedApi.writeBatch(preallocateRequest).getPreallocateIdsList();
        assertEquals(13, preallocatedHeaders.size());

        class RequestData {
            Tag newTag;
            MetadataWriteRequest writeRequest;
        }

        var requestsData = preallocatedHeaders.stream().map(preallocateHeader -> {

            var r = new RequestData();
            var preallocateSelector = selectorForTag(preallocateHeader);

            var newObject = TestData.dummyDefinitionForType(ObjectType.DATA);
            r.newTag = TestData.dummyTag(newObject, TestData.NO_HEADER);
            var tagUpdates = TestData.tagUpdatesForAttrs(r.newTag.getAttrsMap());

            r.writeRequest = MetadataWriteRequest.newBuilder()
                    .setObjectType(ObjectType.DATA)
                    .setPriorVersion(preallocateSelector)
                    .setDefinition(newObject)
                    .addAllTagUpdates(tagUpdates)
                    .build();

            return r;

        }).collect(Collectors.toList());

        var writeRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllCreatePreallocatedObjects(requestsData.stream().map(r -> r.writeRequest).collect(Collectors.toList()))
                .build();

        var tagHeaders = trustedApi.writeBatch(writeRequest).getCreatePreallocatedObjectsList();
        assertEquals(13, tagHeaders.size());

        for (int i = 0; i < 13; i++) {

            var preallocateHeader = preallocatedHeaders.get(i);
            var r = requestsData.get(i);
            var tagHeader = tagHeaders.get(i);

            assertEquals(preallocateHeader.getObjectId(), tagHeader.getObjectId());

            var expectedTag = r.newTag.toBuilder()
                    .setHeader(tagHeader)
                    .build();

            var readRequest = MetadataReadRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSelector(TagSelector.newBuilder()
                            .setObjectType(ObjectType.DATA)
                            .setObjectId(preallocateHeader.getObjectId())
                            .setObjectVersion(1)
                            .setTagVersion(1))
                    .build();

            var savedTag = readApi.readObject(readRequest);

            assertEquals(expectedTag.getHeader(), savedTag.getHeader());
            assertEquals(expectedTag.getDefinition(), savedTag.getDefinition());

            for (var attr : expectedTag.getAttrsMap().keySet())
                assertEquals(expectedTag.getAttrsOrThrow(attr), savedTag.getAttrsOrThrow(attr));

            assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_CREATE_TIME));
            assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_CREATE_USER_ID));
            assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_CREATE_USER_NAME));
            assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_UPDATE_TIME));
            assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_ID));
            assertTrue(savedTag.containsAttrs(MetadataConstants.TRAC_UPDATE_USER_NAME));
        }
    }

    @Test
    void preallocateObjectBatch_idNotReserved() {

        // To save a preallocated object, the ID must first be reserved
        // If the ID is not reserved, that is an item not found error

        var writeRequests = IntStream.range(0, 3).mapToObj(i -> {

            var newObjectId = UUID.randomUUID();
            var selector = TagSelector.newBuilder()
                    .setObjectType(ObjectType.DATA)
                    .setObjectId(newObjectId.toString())
                    .setObjectVersion(0)
                    .setTagVersion(0)
                    .build();

            var newObject = TestData.dummyDefinitionForType(ObjectType.DATA);
            var newTag = TestData.dummyTag(newObject, TestData.NO_HEADER);
            var tagUpdates = TestData.tagUpdatesForAttrs(newTag.getAttrsMap());

            return MetadataWriteRequest.newBuilder()
                    .setObjectType(ObjectType.DATA)
                    .setPriorVersion(selector)
                    .setDefinition(newObject)
                    .addAllTagUpdates(tagUpdates)
                    .build();

        }).collect(Collectors.toList());

        var writeBatchRequest = MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addAllCreatePreallocatedObjects(writeRequests)
                .build();

        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.writeBatch(writeBatchRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }
}