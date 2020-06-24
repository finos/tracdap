package com.accenture.trac.svc.meta.api;

import com.accenture.trac.common.api.meta.MetadataTrustedWriteApiGrpc;
import com.accenture.trac.common.api.meta.MetadataWriteRequest;
import com.accenture.trac.common.metadata.DataDefinition;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.common.api.meta.MetadataReadApiGrpc;
import com.accenture.trac.common.api.meta.MetadataReadRequest;

import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;
import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcH2Impl;
import com.accenture.trac.svc.meta.logic.MetadataReadLogic;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;

import static com.accenture.trac.svc.meta.dal.MetadataDalTestData.TEST_TENANT;
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

    @Test
    void loadTag_ok() {

        var dataDef = DataDefinition.newBuilder()
                .addStorage("TEST");

        var origTag = Tag.newBuilder()
                .setDataDefinition(dataDef);

        var writeRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(origTag)
                .build();

        var idResponse = writeApi.saveNewObject(writeRequest);

        var objectId = MetadataCodec.decode(idResponse.getObjectId());

        MetadataReadRequest readRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        Tag tag = readApi.loadTag(readRequest);

        assertEquals(objectId, MetadataCodec.decode(tag.getHeader().getId()));
    }

    @Test
    void loadTag_linkedDefinitions() {
        fail();
    }

    @Test
    void loadTag_missingItems() {
        fail();
    }

    @Test
    void loadTag_wrongType() {
        fail();
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
