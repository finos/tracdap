package com.accenture.trac.svc.meta.api;

import com.accenture.trac.common.api.meta.IdResponse;
import com.accenture.trac.common.api.meta.MetadataPublicWriteApiGrpc;
import com.accenture.trac.common.api.meta.MetadataWriteRequest;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;
import io.grpc.stub.StreamObserver;


public class MetadataPublicWriteApi extends MetadataPublicWriteApiGrpc.MetadataPublicWriteApiImplBase {

    private final MetadataWriteLogic writeLogic;

    public MetadataPublicWriteApi(MetadataWriteLogic writeLogic) {
        this.writeLogic = writeLogic;
    }

    @Override
    public void saveNewObject(MetadataWriteRequest request, StreamObserver<IdResponse> responseObserver) {
        super.saveNewObject(request, responseObserver);
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
