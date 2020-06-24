package com.accenture.trac.svc.meta.api;

import com.accenture.trac.common.api.meta.IdResponse;
import com.accenture.trac.common.api.meta.MetadataTrustedWriteApiGrpc;
import com.accenture.trac.common.api.meta.MetadataWriteRequest;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;
import io.grpc.stub.StreamObserver;


public class MetadataTrustedWriteApi extends MetadataTrustedWriteApiGrpc.MetadataTrustedWriteApiImplBase {

    private final MetadataWriteLogic writeLogic;

    public MetadataTrustedWriteApi(MetadataWriteLogic writeLogic) {
        this.writeLogic = writeLogic;
    }

    @Override
    public void saveNewObject(MetadataWriteRequest request, StreamObserver<IdResponse> response) {

        ApiHelpers.wrapUnaryCall(response, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var tag = request.getTag();

            var saveResult = writeLogic.saveNewObject(tenant, objectType, tag);

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

    @Override
    public void preallocateId(MetadataWriteRequest request, StreamObserver<IdResponse> responseObserver) {
        super.preallocateId(request, responseObserver);
    }

    @Override
    public void savePreallocatedObject(MetadataWriteRequest request, StreamObserver<IdResponse> responseObserver) {
        super.savePreallocatedObject(request, responseObserver);
    }
}
