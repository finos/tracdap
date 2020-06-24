package com.accenture.trac.svc.meta.api;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import trac.common.api.meta.MetadataReadApiGrpc;
import trac.common.api.meta.MetadataReadRequest;
import trac.common.metadata.ObjectType;
import trac.common.metadata.Tag;

import static org.junit.jupiter.api.Assertions.*;


class MetadataReadApiTest {

    @Rule
    final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private MetadataReadApiGrpc.MetadataReadApiBlockingStub readApi;

    @BeforeEach
    void setup() throws Exception {

        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                 .forName(serverName)
                .directExecutor()
                .addService(new MetadataReadApi())
                .build()
                .start());

        readApi = MetadataReadApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
    }

    @Test
    void loadTag_ok() {

        MetadataReadRequest readRequest = MetadataReadRequest.newBuilder()
                .setTenant("")
                .setObjectType(ObjectType.DATA)
                .setObjectId("")
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();

        Tag tag = readApi.loadTag(readRequest);

        assertEquals(tag.getDataDefinition().getHeader().getId(), "");

        fail();
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
