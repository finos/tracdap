package com.accenture.trac.svc.meta.api;

import com.accenture.trac.svc.meta.exception.TracInternalError;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

public class ApiHelpers {

    public static <T> void wrapUnaryCall(StreamObserver<T> response, Supplier<CompletableFuture<T>> futureFunc) {

        try {

            futureFunc.get().handle((result, error) -> {

                if (result != null) {
                    response.onNext(result);
                    response.onCompleted();
                }
                else if (error instanceof CompletionException) {
                    response.onError(error.getCause() != null ? error.getCause() : new TracInternalError(""));
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
