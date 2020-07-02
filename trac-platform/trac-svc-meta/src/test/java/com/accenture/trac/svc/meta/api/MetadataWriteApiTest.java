package com.accenture.trac.svc.meta.api;

import com.accenture.trac.common.api.meta.*;
import com.accenture.trac.common.metadata.*;
import com.accenture.trac.svc.meta.dal.IMetadataDal;

import com.accenture.trac.svc.meta.logic.MetadataReadLogic;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;
import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcH2Impl;
import com.accenture.trac.svc.meta.test.TestData;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.UUID;
import java.util.function.Function;

import static com.accenture.trac.svc.meta.test.TestData.TEST_TENANT;
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
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNRECOGNIZED"})
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
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE, names = {
            "UNRECOGNIZED",
            "FLOW",
            "CUSTOM"})
    void saveNewObject_publicTypesNotAllowed(ObjectType objectType) {

        var objToSave = TestData.dummyDefinitionForType(objectType);
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

        var objToSave = TestData.dummyDefinitionForType(objectType);
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

        MetadataReadRequest readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var tagFromStore = readApi.loadTag(readRequest);
        var objFromStore = tagFromStore.getDefinition();

        assertEquals(expectedObj, objFromStore);

        // TODO: Tag comparison
    }

    @Test
    void saveNewObject_headerNotNull() {

        var objToSave = TestData.dummyDefinitionForType(ObjectType.DATA);

        // Saving object headers is not allowed, they will be generated by the metadata service
        // Even if the header is empty it should be rejected
        objToSave = objToSave.toBuilder()
                .setHeader(ObjectHeader.newBuilder().build())
                .build();

        var tagToSave = TestData.dummyTag(objToSave);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(tagToSave)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void saveNewObject_wrongType() {

        var objToSave = TestData.dummyDefinitionForType(ObjectType.DATA);
        var tagToSave = TestData.dummyTag(objToSave);

        // Request to save a MODEL, even though the definition is for DATA
        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setTag(tagToSave)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> trustedApi.saveNewObject(writeRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void saveNewObject_invalidContent() {

        var validFlow = TestData.dummyFlowDef();

        var brokenEdges = validFlow.getFlow().toBuilder()
                .addEdge(FlowEdge.newBuilder()
                    .setStart(FlowSocket.newBuilder().setNode("node_totally_not_present"))
                    .setEnd(FlowSocket.newBuilder().setNode("another_absent_node").setSocket("missing_socket")))
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
    }

    @Test
    void saveNewObject_controlledAttrs() {

        var objToSave = TestData.dummyDefinitionForType(ObjectType.CUSTOM);
        var validTag = TestData.dummyTag(objToSave);

        var invalidTag = validTag.toBuilder()
                .putAttr("trac_anything_reserved", PrimitiveValue.newBuilder().setType(PrimitiveType.FLOAT).setFloatValue(1.0).build())
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

    @Test
    void saveNewVersion_ok() {
        fail();
    }

    @Test
    void saveNewVersion_trustedTypesOk() {
        fail();
    }

    @Test
    void saveNewVersion_publicTypesOk() {
        fail();
    }

    @Test
    void saveNewVersion_publicTypesNotAllowed() {
        fail();
    }

    @Test
    void saveNewVersion_headerIsNull() {
        fail();
    }

    @Test
    void saveNewVersion_missingObject() {
        fail();
    }

    @Test
    void saveNewVersion_missingObjectVersion() {
        fail();
    }

    @Test
    void saveNewVersion_superseded() {
        fail();
    }

    @Test
    void saveNewVersion_wrongType() {
        fail();
    }

    @Test
    void saveNewVersion_invalidContent() {
        fail();
    }

    @Test
    void saveNewVersion_controlledAttrs() {
        fail();
    }

    @Test
    void saveNewTag_ok() {
        fail();
    }

    @Test
    void saveNewTag_allTypesOk() {
        fail();
    }

    @Test
    void saveNewTag_headerIsNull() {
        fail();
    }

    @Test
    void saveNewTag_missingObject() {
        fail();
    }

    @Test
    void saveNewTag_missingObjectVersion() {
        fail();
    }

    @Test
    void saveNewTag_missingTagVersion() {
        fail();
    }

    @Test
    void saveNewTag_superseded() {
        fail();
    }

    @Test
    void saveNewTag_wrongType() {
        fail();
    }

    @Test
    void saveNewTag_controlledAttrs() {
        fail();
    }

    @Test
    void preallocateObject_ok() {
        fail();
    }

    @Test
    void preallocateObject_headerNotNull() {
        fail();
    }

    @Test
    void preallocateObject_saveDuplicate() {
        fail();
    }

    @Test
    void preallocateObject_saveWrongType() {
        fail();
    }

    @Test
    void preallocateObject_saveInvalidContent() {
        fail();
    }

    @Test
    void preallocateObject_newVersionBeforeSave() {
        fail();
    }

    @Test
    void preallocateObject_newTagBeforeSave() {
        fail();
    }

    @Test
    void preallocateObject_readBeforeSave() {
        fail();
    }

    @Test
    void preallocateObject_readLatestBeforeSave() {
        fail();
    }
}
