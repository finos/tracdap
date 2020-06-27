package com.accenture.trac.svc.meta.api;

import com.accenture.trac.common.api.meta.MetadataPublicWriteApiGrpc;
import com.accenture.trac.common.api.meta.MetadataReadApiGrpc;
import com.accenture.trac.common.api.meta.MetadataTrustedWriteApiGrpc;
import com.accenture.trac.svc.meta.dal.IMetadataDal;

import com.accenture.trac.svc.meta.logic.MetadataReadLogic;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;
import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcH2Impl;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.fail;


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

    @Test
    void saveNewObject_ok() {
        fail();
    }

    @Test
    void saveNewObject_trustedTypesOk() {
        fail();
    }

    @Test
    void saveNewObject_publicTypesOk() {
        fail();
    }

    @Test
    void saveNewObject_publicTypesNotAllowed() {
        fail();
    }

    @Test
    void saveNewObject_headerNotNull() {
        fail();
    }

    @Test
    void saveNewObject_wrongType() {
        fail();
    }

    @Test
    void saveNewObject_invalidContent() {
        fail();
    }

    @Test
    void saveNewObject_controlledAttrs() {
        fail();
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
