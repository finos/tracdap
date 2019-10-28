package trac.svc.meta.api;

import io.grpc.stub.StreamObserver;
import trac.common.api.meta.*;
import trac.common.metadata.Tag;

import java.util.logging.Logger;


public class MetadataReadApi extends MetadataReadApiGrpc.MetadataReadApiImplBase {

    private final Logger log;

    public MetadataReadApi() {
        log = Logger.getLogger(this.getClass().getName());
    }

    @Override
    public void loadTag(MetadataReadRequest request, StreamObserver<Tag> responseObserver) {

        log.warning(() -> String.format("API request: %s", request.getTenant()));
        responseObserver.onError(new RuntimeException("Not implemented"));
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
}
