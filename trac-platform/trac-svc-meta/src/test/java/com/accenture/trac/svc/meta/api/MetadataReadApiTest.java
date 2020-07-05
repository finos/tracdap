package com.accenture.trac.svc.meta.api;

import com.accenture.trac.common.api.meta.MetadataTrustedWriteApiGrpc;
import com.accenture.trac.common.api.meta.MetadataWriteRequest;
import com.accenture.trac.common.metadata.*;
import com.accenture.trac.common.api.meta.MetadataReadApiGrpc;
import com.accenture.trac.common.api.meta.MetadataReadRequest;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;
import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcH2Impl;
import com.accenture.trac.svc.meta.logic.MetadataReadLogic;

import com.accenture.trac.svc.meta.test.TestData;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static com.accenture.trac.svc.meta.test.TestData.*;
import static org.junit.jupiter.api.Assertions.*;


@ExtendWith(JdbcH2Impl.class)
class MetadataReadApiTest implements IDalTestable {

    private IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }

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
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNRECOGNIZED"})
    void loadTag_ok(ObjectType objectType) {

        var origObj = TestData.dummyDefinitionForType(objectType, NO_HEADER);
        var origTag = TestData.dummyTag(origObj);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setTag(origTag)
                .build();

        var idResponse = writeApi.saveNewObject(writeRequest);
        var objectId = MetadataCodec.decode(idResponse.getObjectId());

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        var savedTag = readApi.loadTag(readRequest);

        var expectedTag = origTag.toBuilder()
                .setDefinition(origObj.toBuilder()
                .setHeader(ObjectHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)))
                .setTagVersion(1)
                .build();

        assertEquals(objectId, MetadataCodec.decode(savedTag.getDefinition().getHeader().getObjectId()));
        assertEquals(expectedTag, savedTag);
    }

    @Test
    void loadTag_versioningOk() {

        var v1Obj = TestData.dummyDefinitionForType(ObjectType.DATA, NO_HEADER);
        var v1Tag = TestData.dummyTag(v1Obj);

        var v1WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(v1Tag)
                .build();

        var idResponse = writeApi.saveNewObject(v1WriteRequest);
        var objectId = MetadataCodec.decode(idResponse.getObjectId());

        var v1ObjWithHeader = v1Obj.toBuilder()
                .setHeader(ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1))
                .build();

        var v1TagWithHeader = v1Tag.toBuilder()
                .setTagVersion(1)
                .setDefinition(v1ObjWithHeader)
                .build();

        var v2Obj = TestData.dummyVersionForType(v1ObjWithHeader, TestData.KEEP_ORIGINAL_HEADER);
        var v2Tag = TestData.dummyTag(v2Obj);
        var t2Tag = TestData.nextTag(v1TagWithHeader, KEEP_ORIGINAL_TAG_VERSION);

        var v2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(v2Tag)
                .build();

        var t2WriteRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(t2Tag)
                .build();

        // noinspection ResultOfMethodCallIgnored
        writeApi.saveNewVersion(v2WriteRequest);

        // noinspection ResultOfMethodCallIgnored
        writeApi.saveNewTag(t2WriteRequest);

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

        var expectedHeader = ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(MetadataCodec.encode(objectId));

        var v1TagExpected = v1Tag.toBuilder()
                .setDefinition(v1Obj.toBuilder()
                .setHeader(expectedHeader
                .setObjectVersion(1)))
                .setTagVersion(1)
                .build();

        var v2TagExpected = v2Tag.toBuilder()
                .setDefinition(v2Obj.toBuilder()
                .setHeader(expectedHeader
                .setObjectVersion(2)))
                .setTagVersion(1)
                .build();

        var t2TagExpected = t2Tag.toBuilder()
                .setDefinition(v1Obj.toBuilder()
                .setHeader(expectedHeader
                .setObjectVersion(1)))
                .setTagVersion(2)
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

        var origObj = TestData.dummyDefinitionForType(ObjectType.MODEL, NO_HEADER);
        var origTag = TestData.dummyTag(origObj);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setTag(origTag)
                .build();

        var idResponse = writeApi.saveNewObject(writeRequest);
        var objectId = MetadataCodec.decode(idResponse.getObjectId());

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

        var origObj = TestData.dummyDefinitionForType(ObjectType.DATA, NO_HEADER);
        var origTag = TestData.dummyTag(origObj);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(origTag)
                .build();

        var idResponse = writeApi.saveNewObject(writeRequest);
        var objectId = MetadataCodec.decode(idResponse.getObjectId());

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.MODEL)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> readApi.loadTag(readRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void loadLatestTag_ok() {
        fail();
    }

    @Test
    void loadLatestTag_linkedDefinitions() {
        fail();
    }
    
    @Test
    void loadLatestTag_oldObject() {
        fail();
    }

    @Test
    void loadLatestTag_missingItems() {
        fail();
    }

    @Test
    void loadLatestTag_wrongType() {
        fail();
    }
    
    @Test
    void loadLatestObject_ok() {
        fail();
    }

    @Test
    void loadLatestObject_linkedDefinitions() {
        fail();
    }

    @Test
    void loadLatestObject_missingItems() {
        fail();
    }

    @Test
    void loadLatestObject_wrongType() {
        fail();
    }

    @Test
    void loadPreallocatedButNotSaved() {
        fail();
    }
}
