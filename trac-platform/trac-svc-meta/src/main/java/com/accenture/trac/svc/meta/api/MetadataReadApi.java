package com.accenture.trac.svc.meta.api;

import com.accenture.trac.svc.meta.exception.TracInternalError;
import com.accenture.trac.svc.meta.logic.MetadataReadLogic;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.common.api.meta.*;

import io.grpc.stub.StreamObserver;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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

        wrapUnaryCall(response, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = UUID.fromString(request.getObjectId());
            var objectVersion = request.getObjectVersion();
            var tagVersion = request.getTagVersion();

            return logic.readTag(tenant, objectType, objectId, objectVersion, tagVersion);
        });
    }

    @Override
    public void loadLatestTag(MetadataReadRequest request, StreamObserver<Tag> responseObserver) {

        log.warning(() -> String.format("API request: %s", request.getTenant()));
        responseObserver.onError(new RuntimeException("Not implemented"));
    }

    @Override
    public void loadLatestObject(MetadataReadRequest request, StreamObserver<Tag> responseObserver) {

        log.warning(() -> String.format("API request: %s", request.getTenant()));
        responseObserver.onError(new RuntimeException("Not implemented"));
    }

    private <T> void wrapUnaryCall(StreamObserver<T> response, Supplier<CompletableFuture<T>> futureFunc) {

        try {

            futureFunc.get().handle((result, error) -> {

                if (result != null) {
                    response.onNext(result);
                    response.onCompleted();
                }
                else {
                    response.onError(error != null ? error : new TracInternalError(""));
                }

                return null;
            });
        }
        catch (Exception error) {
            response.onError(error);
        }
    }
}
