package com.accenture.trac.svc.meta.api;

import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.svc.meta.exception.TracInternalError;
import com.accenture.trac.svc.meta.logic.MetadataReadLogic;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.common.api.meta.*;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.logging.Logger;


public class MetadataReadApi extends MetadataReadApiGrpc.MetadataReadApiImplBase {

    private final Logger log = Logger.getLogger(this.getClass().getName());

    private final MetadataReadLogic logic;

    public MetadataReadApi(MetadataReadLogic logic) {
        this.logic = logic;
    }

    @Override
    public void loadTag(MetadataReadRequest request, StreamObserver<Tag> response) {

        ApiHelpers.wrapUnaryCall(response, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = MetadataCodec.decode(request.getObjectId());
            var objectVersion = request.getObjectVersion();
            var tagVersion = request.getTagVersion();

            return logic.loadTag(tenant, objectType, objectId, objectVersion, tagVersion);
        });
    }

    @Override
    public void loadLatestTag(MetadataReadRequest request, StreamObserver<Tag> responseObserver) {

        ApiHelpers.wrapUnaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = MetadataCodec.decode(request.getObjectId());
            var objectVersion = request.getObjectVersion();

            return logic.loadLatestTag(tenant, objectType, objectId, objectVersion);
        });
    }

    @Override
    public void loadLatestObject(MetadataReadRequest request, StreamObserver<Tag> responseObserver) {

        ApiHelpers.wrapUnaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = MetadataCodec.decode(request.getObjectId());

            return logic.loadLatestObject(tenant, objectType, objectId);
        });
    }

}
