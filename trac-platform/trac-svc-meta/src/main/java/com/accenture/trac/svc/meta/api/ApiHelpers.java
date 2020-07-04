package com.accenture.trac.svc.meta.api;

import com.accenture.trac.svc.meta.exception.*;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.Map;
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
                else {

                    mapErrorResponse(response, error);
                }

                return null;
            });
        }
        catch (Exception error) {

            mapErrorResponse(response, error);
        }
    }

    public static <T> void mapErrorResponse(StreamObserver<T> response, Throwable error) {

        // Error already as a GRPC status, top level API classes may do this
        if (error instanceof StatusRuntimeException || error instanceof StatusException) {

            response.onError(error);
        }

        // Errors wrapped up by the CompletableFuture API, we want to unwrap them
        else if (error instanceof CompletionException && error.getCause() != null) {

            mapUnwrappedErrorResponse(response, error.getCause());
        }

        // All other regular errors
        else {

            mapUnwrappedErrorResponse(response, error);
        }
    }

    public static <T> void mapUnwrappedErrorResponse(StreamObserver<T> response, Throwable error) {

        var statusCode = error != null
                ? ERROR_MAPPING.getOrDefault(error.getClass(), Status.Code.UNKNOWN)
                : Status.Code.UNKNOWN;

        var errorMessage = statusCode == Status.Code.UNKNOWN
                ? "There was an unexpected internal error in the TRAC platform"
                : error.getMessage();

        var status = Status.fromCode(statusCode)
                .withDescription(errorMessage)
                .withCause(error);

        response.onError(status.asRuntimeException());
    }

    private static final Map<Class<?>, Status.Code> ERROR_MAPPING = Map.of(

            AuthorisationError.class, Status.Code.PERMISSION_DENIED,
            InputValidationError.class, Status.Code.INVALID_ARGUMENT,

            MissingItemError.class, Status.Code.NOT_FOUND,
            DuplicateItemError.class, Status.Code.ALREADY_EXISTS,
            WrongItemTypeError.class, Status.Code.INVALID_ARGUMENT  //,

            // CorruptItemError.class, Status.Code.INVALID_ARGUMENT
    );
}
