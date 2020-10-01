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
import com.accenture.trac.svc.meta.logic.MetadataReadLogic;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import java.util.UUID;
import java.util.function.Function;

import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcH2Impl;
import com.accenture.trac.svc.meta.test.TestData;

import org.junit.Rule;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static com.accenture.trac.svc.meta.test.TestData.*;
import static org.junit.jupiter.api.Assertions.*;


@ExtendWith(JdbcH2Impl.class)
public class MetadataWriteApiTest implements IDalTestable {

    private IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }

    @Rule
    final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private MetadataPublicWriteApiGrpc.MetadataPublicWriteApiBlockingStub publicApi;
    private MetadataTrustedWriteApiGrpc.MetadataTrustedWriteApiBlockingStub trustedApi;
    private MetadataReadApiGrpc.MetadataReadApiBlockingStub readApi;

    @BeforeEach
    void setup() throws Exception {

        var serverName = InProcessServerBuilder.generateName();

        var writeLogic = new MetadataWriteLogic(dal);
        var publicApiImpl = new MetadataPublicWriteApi(writeLogic);
        var trustedApiImpl = new MetadataTrustedWriteApi(writeLogic);

        var readLogic = new MetadataReadLogic(dal);
        var readApiImpl = new MetadataReadApi(readLogic);

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(publicApiImpl)
                .addService(trustedApiImpl)
                .addService(readApiImpl)
                .build()
                .start());

        trustedApi = MetadataTrustedWriteApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));

        publicApi = MetadataPublicWriteApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));

        readApi = MetadataReadApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void saveNewObject_trustedTypesOk(ObjectType objectType) {

        saveNewObject_ok(objectType, request -> trustedApi.saveNewObject(request));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE, names = {
            "FLOW",
            "CUSTOM"})
    void saveNewObject_publicTypesOk(ObjectType objectType) {

        // All object types should be either in this test, or publicTypesNotAllowed

        saveNewObject_ok(objectType, request -> publicApi.saveNewObject(request));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
            names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED", "FLOW", "CUSTOM"})
    void saveNewObject_publicTypesNotAllowed(ObjectType objectType) {

        var objToSave = TestData.dummyDefinitionForType(objectType, TestData.NO_HEADER);
        var tagToSave = TestData.dummyTag(objToSave);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setTag(tagToSave)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewObject(writeRequest));
        assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    }

    void saveNewObject_ok(ObjectType objectType, Function<MetadataWriteRequest, IdResponse> saveApiCall) {

        var objToSave = TestData.dummyDefinitionForType(objectType, TestData.NO_HEADER);
        var tagToSave = TestData.dummyTag(objToSave);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setTag(tagToSave)
                .build();

        var idResponse = saveApiCall.apply(writeRequest);
        var objectId = MetadataCodec.decode(idResponse.getObjectId());

        assertNotNull(objectId);
        assertNotEquals(new UUID(0, 0), objectId);
        assertEquals(1, idResponse.getObjectVersion());
        assertEquals(1, idResponse.getTagVersion());

        var expectedHeader = ObjectHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(idResponse.getObjectId())
                .setObjectVersion(1);

        var expectedObj = objToSave.toBuilder()
                .setHeader(expectedHeader)
                .build();

        var expectedTag = tagToSave.toBuilder()
                .setTagVersion(1)
                .setDefinition(expectedObj)
                .build();

        MetadataReadRequest readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var tagFromStore = readApi.loadTag(readRequest);

        assertEquals(expectedTag, tagFromStore);
    }

    @Test
    void saveNewObject_headerNotNull() {

        var objToSave = TestData.dummyDefinitionForType(ObjectType.CUSTOM, TestData.NO_HEADER);

        // Saving object headers is not allowed, they will be generated by the metadata service
        // Even if the header is empty it should be rejected
        objToSave = objToSave.toBuilder()
                .setHeader(ObjectHeader.newBuilder().build())
                .build();

        var tagToSave = TestData.dummyTag(objToSave);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(tagToSave)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void saveNewObject_inconsistentType() {

        // This test is about sending an invalid request, i.e. the payload does not match the write request

        // Make sure both types are allowed on the public API, so we don't get permission denied

        var objToSave = TestData.dummyDefinitionForType(ObjectType.CUSTOM, TestData.NO_HEADER);
        var tagToSave = TestData.dummyTag(objToSave);

        // Request to save a MODEL, even though the definition is for DATA
        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setTag(tagToSave)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    @Disabled("Object definition content validation is not implemented yet")
    void saveNewObject_invalidContent() {

        var validFlow = TestData.dummyFlowDef(TestData.NO_HEADER);

        // Create a flow with an invalid node graph, this should get picked up by the validation layer

        var brokenEdges = validFlow.getFlow().toBuilder()
                .addEdge(FlowEdge.newBuilder()
                    .setHead(FlowSocket.newBuilder().setNode("another_absent_node").setSocket("missing_socket"))
                    .setTail(FlowSocket.newBuilder().setNode("node_totally_not_present")))
                .build();

        var invalidFlow = validFlow.toBuilder()
                .setFlow(brokenEdges)
                .build();

        var tagToSave = TestData.dummyTag(invalidFlow);

        // Try to save the flow with a broken graph, should fail validation
        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setTag(tagToSave)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void saveNewObject_invalidAttrs() {

        var objToSave = TestData.dummyDefinitionForType(ObjectType.CUSTOM, TestData.NO_HEADER);
        var validTag = TestData.dummyTag(objToSave);

        var invalidTag = validTag.toBuilder()
                .putAttr("${escape_key}", MetadataCodec.encodeValue(1.0))
                .build();

        // Request to save a MODEL, even though the definition is for DATA
        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(invalidTag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void saveNewObject_reservedAttrs() {

        var objToSave = TestData.dummyDefinitionForType(ObjectType.CUSTOM, TestData.NO_HEADER);
        var validTag = TestData.dummyTag(objToSave);

        var invalidTag = validTag.toBuilder()
                .putAttr("trac_anything_reserved", MetadataCodec.encodeValue(1.0))
                .build();

        // Request to save a MODEL, even though the definition is for DATA
        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(invalidTag)
                .build();

        // Setting reserved attributes is allowed through the trusted API but not the public API

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewObject(writeRequest));
        assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());

        assertDoesNotThrow(() -> trustedApi.saveNewObject(writeRequest));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // OBJECT VERSIONS
    // -----------------------------------------------------------------------------------------------------------------


    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE, names = {"DATA", "CUSTOM"})
    void saveNewVersion_trustedTypesOk(ObjectType objectType) {

        saveNewVersion_ok(objectType, request -> trustedApi.saveNewVersion(request));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.INCLUDE, names = {"CUSTOM"})
    void saveNewVersion_publicTypesOk(ObjectType objectType) {

        saveNewVersion_ok(objectType, request -> publicApi.saveNewVersion(request));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
            names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED", "FLOW", "CUSTOM"})
    void saveNewVersion_publicTypesNotAllowed(ObjectType objectType) {

        var v1SavedTag = saveNewVersion_prepareV1(objectType);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), TestData.KEEP_ORIGINAL_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setTag(v2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
            names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED", "DATA", "CUSTOM"})
    void saveNewVersion_versionsNotSupported(ObjectType objectType) {

        var v1SavedTag = saveNewVersion_prepareV1(objectType);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), TestData.KEEP_ORIGINAL_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setTag(v2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    void saveNewVersion_ok(ObjectType objectType, Function<MetadataWriteRequest, IdResponse> saveApiCall) {

        var v1SavedTag = saveNewVersion_prepareV1(objectType);
        var v1ObjectId = MetadataCodec.decode(v1SavedTag.getDefinition().getHeader().getObjectId());

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), TestData.KEEP_ORIGINAL_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setTag(v2Tag)
                .build();

        var v2IdResponse = saveApiCall.apply(v2WriteRequest);
        var v2ObjectId = MetadataCodec.decode(v2IdResponse.getObjectId());

        assertEquals(v1ObjectId, v2ObjectId);
        assertEquals(2, v2IdResponse.getObjectVersion());
        assertEquals(1, v2IdResponse.getTagVersion());

        var expectedHeader = ObjectHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(v2IdResponse.getObjectId())
                .setObjectVersion(2);

        var expectedObj = v2Obj.toBuilder()
                .setHeader(expectedHeader)
                .build();

        var expectedTag = v2Tag.toBuilder()
                .setTagVersion(1)
                .setDefinition(expectedObj)
                .build();

        MetadataReadRequest readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(v2ObjectId))
                .setObjectVersion(2)
                .setTagVersion(1)
                .build();

        var tagFromStore = readApi.loadTag(readRequest);

        assertEquals(expectedTag, tagFromStore);
    }

    Tag saveNewVersion_prepareV1(ObjectType objectType) {

        // Save and retrieve version 1, the saved version will have an object header filled in

        var v1Obj = TestData.dummyDefinitionForType(objectType, TestData.NO_HEADER);
        var v1Tag = TestData.dummyTag(v1Obj);

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setTag(v1Tag)
                .build();

        var v1IdResponse = trustedApi.saveNewObject(v1WriteRequest);

        var v1ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setObjectId(v1IdResponse.getObjectId())
                .setObjectVersion(v1IdResponse.getObjectVersion())
                .setTagVersion(v1IdResponse.getTagVersion())
                .build();

        return readApi.loadTag(v1ReadRequest);
    }

    // For the remaining corner and error cases around versions, we use the CUSTOM object type
    // This is because CUSTOM objects support versions and can be saved in both the public and trusted API

    @Test
    void saveNewVersion_latestUpdated() {

        // Make sure saving a new version updates the 'latest' flag for the object
        // Test both trusted and public APIs

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.CUSTOM);
        var v1ObjectId = v1SavedTag.getDefinition().getHeader().getObjectId();

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(v1ObjectId)
                .build();

        var v1Latest = readApi.loadLatestObject(readRequest);
        assertEquals(1, v1Latest.getDefinition().getHeader().getObjectVersion());

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), TestData.KEEP_ORIGINAL_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(v2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        trustedApi.saveNewVersion(v2WriteRequest);

        var v2Latest = readApi.loadLatestObject(readRequest);
        assertEquals(2, v2Latest.getDefinition().getHeader().getObjectVersion());

        var v3Obj = TestData.dummyVersionForType(v2Latest.getDefinition(), TestData.KEEP_ORIGINAL_HEADER);
        var v3Tag = TestData.dummyTag(v3Obj);

        var v3WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(v3Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        publicApi.saveNewVersion(v3WriteRequest);

        var v3Latest = readApi.loadLatestObject(readRequest);
        assertEquals(3, v3Latest.getDefinition().getHeader().getObjectVersion());
    }

    @Test
    void saveNewVersion_headerIsNull() {

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.CUSTOM);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), TestData.KEEP_ORIGINAL_HEADER)
                .toBuilder()
                .clearHeader()
                .build();

        var v2Tag = TestData.dummyTag(v2Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(v2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void saveNewVersion_inconsistentType() {

        // This test is about sending an invalid request, i.e. the payload does not match the write request

        // Make sure both types are allowed on the public API, so we don't get permission denied

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.CUSTOM);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), TestData.KEEP_ORIGINAL_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setTag(v2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void saveNewVersion_wrongType() {

        // This test is about mis-matched types between versions, e.g. V1 is CUSTOM, V2 is DATA
        // This counts as a pre-condition failure rather than invalid input,
        // because it depends on existing data in the metadata store

        // Currently only testing the trusted API, as public does not support two types with versioning

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.CUSTOM);

        var v2HeaderPreSave = v1SavedTag.getDefinition().getHeader()
                .toBuilder()
                .setObjectType(ObjectType.DATA);

        var v2Obj = TestData.dummyDataDef(NO_HEADER).toBuilder()
                .setHeader(v2HeaderPreSave)
                .build();

        var v2Tag = TestData.dummyTag(v2Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(v2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @Test
    void saveNewVersion_missingObject() {

        // V1 object is not saved
        var v1Ojb = TestData.dummyCustomDef(TestData.INCLUDE_HEADER);

        var v2Obj = TestData.dummyVersionForType(v1Ojb, TestData.KEEP_ORIGINAL_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(v2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());
    }

    @Test
    void saveNewVersion_missingObjectVersion() {

        // V1 object created and saved
        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.CUSTOM);

        // V2 object is not saved
        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), TestData.UPDATE_HEADER);

        var v3Obj = TestData.dummyVersionForType(v2Obj, TestData.KEEP_ORIGINAL_HEADER);
        var v3Tag = TestData.dummyTag(v3Obj);

        var v3WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(v3Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(v3WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(v3WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());
    }

    @Test
    void saveNewVersion_superseded() {

        // V1 object created and saved
        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.CUSTOM);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), TestData.KEEP_ORIGINAL_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(v2Tag)
                .build();

        // Save V2 once should be ok

        assertDoesNotThrow(() -> trustedApi.saveNewVersion(v2WriteRequest));

        // Trying to create V2 a second time is an error

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.ALREADY_EXISTS, error2.getStatus().getCode());
    }

    @Test
    @Disabled("Object definition content validation is not implemented yet")
    void saveNewVersion_invalidContent() {

        // V1 object created and saved
        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.DATA);
        var v1Schema = v1SavedTag.getDefinition().getData().getSchema();

        // Create a V2 data definition that is invalid, use an explicit fieldOrder = -1

        var v2Schema = v1Schema
                .toBuilder()
                .addField(FieldDefinition.newBuilder()
                        .setFieldName("some_new_field")
                        .setFieldType(BasicType.STRING)
                        .setFieldOrder(-1));

        var v2Obj = v1SavedTag.getDefinition().toBuilder()
                .setData(v1SavedTag.getDefinition().getData()
                    .toBuilder()
                    .setSchema(v2Schema))
                .build();

        var v2Tag = TestData.dummyTag(v2Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(v2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void saveNewVersion_invalidAttrs() {

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.CUSTOM);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), TestData.KEEP_ORIGINAL_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);

        var v2ControlledTag = v2Tag.toBuilder()
                .putAttr("very\nbroken.attr", MetadataCodec.encodeValue(1.0))
                .build();

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(v2ControlledTag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void saveNewVersion_reservedAttrs() {

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.CUSTOM);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), TestData.KEEP_ORIGINAL_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);

        var v2ControlledTag = v2Tag.toBuilder()
                .putAttr("trac_anything_reserved", MetadataCodec.encodeValue(1.0))
                .build();

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(v2ControlledTag)
                .build();

        // Setting reserved attributes is allowed through the trusted API but not the public API

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(v2WriteRequest));
        assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());

        assertDoesNotThrow(() -> trustedApi.saveNewVersion(v2WriteRequest));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // TAG VERSIONS
    // -----------------------------------------------------------------------------------------------------------------


    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void saveNewTag_AllTypesOk(ObjectType objectType) {

        var v1SavedTag = saveNewVersion_prepareV1(objectType);
        var v1ObjectId = MetadataCodec.decode(v1SavedTag.getDefinition().getHeader().getObjectId());

        var t2Tag = v1SavedTag.toBuilder()
                .putAttr("extra_attr_v2", MetadataCodec.encodeValue("First extra attr"))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setTag(t2Tag)
                .build();

        var t2IdResponse = trustedApi.saveNewTag(t2WriteRequest);

        assertEquals(v1ObjectId, MetadataCodec.decode(t2IdResponse.getObjectId()));
        assertEquals(1, t2IdResponse.getObjectVersion());
        assertEquals(2, t2IdResponse.getTagVersion());

        var t2ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(v1ObjectId))
                .setObjectVersion(1)
                .setTagVersion(2)
                .build();

        var t2ExpectedTag = t2Tag.toBuilder().setTagVersion(2).build();
        var t2SavedTag = readApi.loadTag(t2ReadRequest);

        assertEquals(t2ExpectedTag, t2SavedTag);

        var t3Tag = t2SavedTag.toBuilder()
                .putAttr("extra_attr_v3", MetadataCodec.encodeValue("Second extra attr"))
                .build();

        var t3WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setTag(t3Tag)
                .build();

        var t3IdResponse = publicApi.saveNewTag(t3WriteRequest);

        assertEquals(v1ObjectId, MetadataCodec.decode(t3IdResponse.getObjectId()));
        assertEquals(1, t3IdResponse.getObjectVersion());
        assertEquals(3, t3IdResponse.getTagVersion());

        var t3ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(v1ObjectId))
                .setObjectVersion(1)
                .setTagVersion(3)
                .build();

        var t3ExpectedTag = t3Tag.toBuilder().setTagVersion(3).build();
        var t3SavedTag = readApi.loadTag(t3ReadRequest);

        assertEquals(t3ExpectedTag, t3SavedTag);
    }

    @Test
    void saveNewTag_latestUpdated() {

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.DATA);
        var v1Header = v1SavedTag.getDefinition().getHeader();

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), TestData.KEEP_ORIGINAL_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(v2Tag)
                .build();

        var v2IdResponse = trustedApi.saveNewVersion(v2WriteRequest);

        assertEquals(v1Header.getObjectId(), v2IdResponse.getObjectId());
        assertEquals(2, v2IdResponse.getObjectVersion());
        assertEquals(1, v2IdResponse.getTagVersion());

        var v1t2Tag = TestData.nextTag(v1SavedTag, KEEP_ORIGINAL_TAG_VERSION);

        var v1t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(v1t2Tag)
                .build();

        var v1t2IdResponse = trustedApi.saveNewTag(v1t2WriteRequest);

        assertEquals(v1Header.getObjectId(), v1t2IdResponse.getObjectId());
        assertEquals(1, v1t2IdResponse.getObjectVersion());
        assertEquals(2, v1t2IdResponse.getTagVersion());

        var v1ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setObjectId(v1Header.getObjectId())
                .setObjectVersion(1)
                .build();

        var v2ReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setObjectId(v1Header.getObjectId())
                .setObjectVersion(2)
                .build();

        var v1LatestTag = readApi.loadLatestTag(v1ReadRequest);
        var v2latestTag = readApi.loadLatestTag(v2ReadRequest);

        assertEquals(2, v1LatestTag.getTagVersion());
        assertEquals(1, v2latestTag.getTagVersion());
    }

    @Test
    void saveNewTag_headerIsNull() {

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.DATA);

        var t2Tag = TestData.nextTag(v1SavedTag, KEEP_ORIGINAL_TAG_VERSION)
                .toBuilder()
                .setDefinition(v1SavedTag.getDefinition().toBuilder().clearHeader())
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(t2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewTag(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewTag(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void saveNewTag_tagVersionIsNull() {

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.DATA);

        var t2Tag = TestData.nextTag(v1SavedTag, KEEP_ORIGINAL_TAG_VERSION)
                .toBuilder()
                .clearTagVersion()
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(t2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewTag(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewTag(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void saveNewTag_inconsistentType() {

        // This test is about sending an invalid request, i.e. the payload does not match the write request

        // Make sure both types are allowed on the public API, so we don't get permission denied

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.DATA);

        var t2Tag = TestData.nextTag(v1SavedTag, KEEP_ORIGINAL_TAG_VERSION);

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setTag(t2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewTag(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewTag(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }



    @Test
    void saveNewTag_wrongType() {

        // This test is about mis-matched types between versions, e.g. V1 is CUSTOM, V2 is DATA
        // This counts as a pre-condition failure rather than invalid input,
        // because it depends on existing data in the metadata store

        // Currently only testing the trusted API, as public does not support two types with versioning

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.CUSTOM);

        var t2HeaderPreSave = v1SavedTag.getDefinition().getHeader()
                .toBuilder()
                .setObjectType(ObjectType.DATA);

        var t2Obj = TestData.dummyDataDef(NO_HEADER).toBuilder()
                .setHeader(t2HeaderPreSave)
                .build();

        var t2Tag = TestData.dummyTag(t2Obj);

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(t2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewTag(t2WriteRequest));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @Test
    void saveNewTag_missingObject() {

        var v1Obj = TestData.dummyDataDef(INCLUDE_HEADER);
        var v1Tag = TestData.dummyTag(v1Obj);

        var t2Tag = TestData.nextTag(v1Tag, KEEP_ORIGINAL_TAG_VERSION);

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(t2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewTag(t2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewTag(t2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());
    }

    @Test
    void saveNewTag_missingObjectVersion() {

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.DATA);

        var v2Obj = TestData.dummyVersionForType(v1SavedTag.getDefinition(), UPDATE_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);

        var v2t2Tag = TestData.nextTag(v2Tag, KEEP_ORIGINAL_TAG_VERSION);

        var v2t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(v2t2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewTag(v2t2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewTag(v2t2WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());
    }

    @Test
    void saveNewTag_missingTagVersion() {

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.DATA);

        var t2Tag = v1SavedTag.toBuilder()
                .setTagVersion(v1SavedTag.getTagVersion() + 1)
                .putAttr("extra_attr_v2", MetadataCodec.encodeValue("First extra attr"))
                .build();

        var t3Tag = TestData.nextTag(t2Tag, KEEP_ORIGINAL_TAG_VERSION);

        var t3WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(t3Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewTag(t3WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewTag(t3WriteRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());
    }

    @Test
    void saveNewTag_superseded() {

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.DATA);

        var t2Tag = TestData.nextTag(v1SavedTag, KEEP_ORIGINAL_TAG_VERSION);

        var v2t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(t2Tag)
                .build();

        // Saving tag version 2 should succeed the first time

        assertDoesNotThrow(() -> trustedApi.saveNewTag(v2t2WriteRequest));

        // Trying to save tag version 2 a second time is an error

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewTag(v2t2WriteRequest));
        assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewTag(v2t2WriteRequest));
        assertEquals(Status.Code.ALREADY_EXISTS, error2.getStatus().getCode());
    }

    @Test
    void saveNewTag_invalidAttrs() {

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.CUSTOM);

        var t2Tag = TestData.nextTag(v1SavedTag, KEEP_ORIGINAL_TAG_VERSION);

        var t2ControlledTag = t2Tag.toBuilder()
                .putAttr("no-hyphens", MetadataCodec.encodeValue(1.0))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(t2ControlledTag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(t2WriteRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void saveNewTag_reservedAttrs() {

        var v1SavedTag = saveNewVersion_prepareV1(ObjectType.CUSTOM);

        var t2Tag = TestData.nextTag(v1SavedTag, KEEP_ORIGINAL_TAG_VERSION);

        var t2ControlledTag = t2Tag.toBuilder()
                .putAttr("trac_anything_reserved", MetadataCodec.encodeValue(1.0))
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setTag(t2ControlledTag)
                .build();

        // Setting reserved attributes is allowed through the trusted API but not the public API

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.saveNewVersion(t2WriteRequest));
        assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());

        assertDoesNotThrow(() -> trustedApi.saveNewVersion(t2WriteRequest));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // PREALLOCATE OBJECTS
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void preallocateObject_ok() {

        // Simple round trip
        // Preallocate an ID, save an object to that ID and read it back

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        var preallocateResponse = trustedApi.preallocateId(preallocateRequest);

        // For preallocated objects, the header must be set with the preallocated ID and version = 1
        var newObjectHeader = ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(preallocateResponse.getObjectId())
                .setObjectVersion(1);

        var newObject = TestData.dummyDefinitionForType(ObjectType.DATA, TestData.NO_HEADER)
                .toBuilder()
                .setHeader(newObjectHeader)
                .build();

        var newTag = TestData.dummyTag(newObject);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(newTag)
                .build();

        var writeResponse = trustedApi.savePreallocatedObject(writeRequest);

        assertEquals(preallocateResponse.getObjectId(), writeResponse.getObjectId());

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setObjectId(preallocateResponse.getObjectId())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var savedTag = readApi.loadTag(readRequest);

        assertEquals(newTag, savedTag);
    }

    @Test
    void preallocateObject_headerMissing() {

        // Saving a pre-allocated object with a null header is an invalid request
        // For preallocation, the header has to be set and must match the preallocated ID

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        // noinspection ResultOfMethodCallIgnored
        trustedApi.preallocateId(preallocateRequest);

        var newObject = TestData.dummyDefinitionForType(ObjectType.DATA, TestData.NO_HEADER);
        var newTag = TestData.dummyTag(newObject);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(newTag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.savePreallocatedObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void preallocateObject_headerWrongVersion() {

        // To save a preallocated object, the header must be set with object version = 1
        // A request to save a preallocated object with version != 1 is considered invalid

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        var preallocateResponse = trustedApi.preallocateId(preallocateRequest);

        // For preallocated objects, the header must be set with the preallocated ID and version = 1
        var newObjectHeader = ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(preallocateResponse.getObjectId())
                .setObjectVersion(2);

        var newObject = TestData.dummyDefinitionForType(ObjectType.DATA, TestData.NO_HEADER)
                .toBuilder()
                .setHeader(newObjectHeader)
                .build();

        var newTag = TestData.dummyTag(newObject);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(newTag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.savePreallocatedObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void preallocateObject_idNotReserved() {

        // To save a preallocated object, the ID must first be reserved
        // If the ID is not reserved, that is an item not found error

        var newObjectId = UUID.randomUUID();

        var newObjectHeader = ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(MetadataCodec.encode(newObjectId))
                .setObjectVersion(1);

        var newObject = TestData.dummyDefinitionForType(ObjectType.DATA, TestData.NO_HEADER)
                .toBuilder()
                .setHeader(newObjectHeader)
                .build();

        var newTag = TestData.dummyTag(newObject);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(newTag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.savePreallocatedObject(writeRequest));
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

        var preallocateResponse = trustedApi.preallocateId(preallocateRequest);

        var newObjectHeader = ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(preallocateResponse.getObjectId())
                .setObjectVersion(1);

        var newObject = TestData.dummyDefinitionForType(ObjectType.DATA, TestData.NO_HEADER)
                .toBuilder()
                .setHeader(newObjectHeader)
                .build();

        var newTag = TestData.dummyTag(newObject);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(newTag)
                .build();

        // The first save request should succeed
        var writeResponse = trustedApi.savePreallocatedObject(writeRequest);
        assertEquals(preallocateResponse.getObjectId(), writeResponse.getObjectId());

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.savePreallocatedObject(writeRequest));
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

        var preallocateResponse = trustedApi.preallocateId(preallocateRequest);

        var newObjectHeader = ObjectHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(preallocateResponse.getObjectId())
                .setObjectVersion(1);

        var newObject = TestData.dummyDefinitionForType(ObjectType.MODEL, TestData.NO_HEADER)
                .toBuilder()
                .setHeader(newObjectHeader)
                .build();

        var newTag = TestData.dummyTag(newObject);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setTag(newTag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.savePreallocatedObject(writeRequest));
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

        var preallocateResponse = trustedApi.preallocateId(preallocateRequest);

        var newObjectHeader = ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(preallocateResponse.getObjectId())
                .setObjectVersion(1);

        var newObject = TestData.dummyDefinitionForType(ObjectType.DATA, TestData.NO_HEADER)
                .toBuilder()
                .setHeader(newObjectHeader)
                .build();

        var newTag = TestData.dummyTag(newObject);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setTag(newTag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.savePreallocatedObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    @Disabled("Content validation not implemented yet")
    void preallocateObject_saveInvalidContent() {
        fail();
    }

    @Test
    void preallocateObject_newVersionBeforeSave() {

        // In order to save a new object version, the first version must exist
        // If the prior version does not exist, that is an item not found error
        // If an ID has been preallocated but nothing saved to it, that does not change the behaviour

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        var preallocateResponse = trustedApi.preallocateId(preallocateRequest);

        var v1Header = ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(preallocateResponse.getObjectId())
                .setObjectVersion(1);

        var newObject = TestData.dummyDefinitionForType(ObjectType.DATA, TestData.NO_HEADER)
                .toBuilder()
                .setHeader(v1Header)
                .build();

        var newTag = TestData.dummyTag(newObject);

        var newVersionRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(newTag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewVersion(newVersionRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void preallocateObject_newTagBeforeSave() {

        // In order to save a new tag version, the object and tag must exist
        // If the prior tag does not exist, that is an item not found error
        // If an ID has been preallocated but nothing saved to it, that does not change the behaviour

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .build();

        var preallocateResponse = trustedApi.preallocateId(preallocateRequest);

        var v1Header = ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(preallocateResponse.getObjectId())
                .setObjectVersion(1);

        var v1Object = TestData.dummyDefinitionForType(ObjectType.DATA, TestData.NO_HEADER)
                .toBuilder()
                .setHeader(v1Header)
                .build();

        var newTag = TestData.dummyTag(v1Object);

        var newTagRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(newTag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewTag(newTagRequest));
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

        var preallocateResponse = trustedApi.preallocateId(preallocateRequest);

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setObjectId(preallocateResponse.getObjectId())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        // Try reading with explicit version / tag, latest tag and latest version

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.loadTag(readRequest));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> readApi.loadLatestTag(readRequest));
        assertEquals(Status.Code.NOT_FOUND, error2.getStatus().getCode());

        // noinspection ResultOfMethodCallIgnored
        var error3 = assertThrows(StatusRuntimeException.class, () -> readApi.loadLatestObject(readRequest));
        assertEquals(Status.Code.NOT_FOUND, error3.getStatus().getCode());
    }
}
