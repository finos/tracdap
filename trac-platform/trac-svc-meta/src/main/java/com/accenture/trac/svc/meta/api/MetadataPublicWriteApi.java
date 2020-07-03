package com.accenture.trac.svc.meta.api;

import com.accenture.trac.common.api.meta.IdResponse;
import com.accenture.trac.common.api.meta.MetadataPublicWriteApiGrpc;
import com.accenture.trac.common.api.meta.MetadataWriteRequest;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class MetadataPublicWriteApi extends MetadataPublicWriteApiGrpc.MetadataPublicWriteApiImplBase {

    // Only a limited set of object types can be created directly by clients
    // Everything else can only be created by the trusted API, i.e. by other TRAC platform components
    public static final List<ObjectType> PUBLIC_TYPES = Arrays.asList(
            ObjectType.FLOW,
            ObjectType.CUSTOM);

    private final MetadataWriteLogic writeLogic;

    public MetadataPublicWriteApi(MetadataWriteLogic writeLogic) {
        this.writeLogic = writeLogic;
    }

    @Override
    public void saveNewObject(MetadataWriteRequest request, StreamObserver<IdResponse> responseObserver) {

        ApiHelpers.wrapUnaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var tag = request.getTag();

            if (!PUBLIC_TYPES.contains(objectType)) {
                var message = String.format("Object type %s cannot be created via the TRAC public API", objectType);
                var status = Status.PERMISSION_DENIED.withDescription(message);
                return CompletableFuture.failedFuture(status.asRuntimeException());
            }

            var saveResult = writeLogic.saveNewObject(tenant, objectType, tag, MetadataWriteLogic.PUBLIC);

            var idResponse = saveResult
                    .thenApply(objectId -> IdResponse.newBuilder()
                    .setObjectId(MetadataCodec.encode(objectId))
                    .setObjectVersion(1)
                    .setTagVersion(1)
                    .build());

            return idResponse;
        });
    }

    @Override
    public void saveNewVersion(MetadataWriteRequest request, StreamObserver<IdResponse> responseObserver) {
        super.saveNewVersion(request, responseObserver);
    }

    @Override
    public void saveNewTag(MetadataWriteRequest request, StreamObserver<IdResponse> responseObserver) {
        super.saveNewTag(request, responseObserver);
    }
}
