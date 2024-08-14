/*
 * Copyright 2022 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.grpc;

import io.grpc.Metadata;
import io.grpc.protobuf.ProtoUtils;
import org.finos.tracdap.api.TracErrorDetails;
import org.finos.tracdap.common.exception.*;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;


public class GrpcErrorMapping {

    private static final Logger log = LoggerFactory.getLogger(GrpcErrorMapping.class);

    private static final Metadata.Key<TracErrorDetails> TRAC_ERROR_DETAILS_KEY =
            Metadata.Key.of("trac-error-details-bin", ProtoUtils.metadataMarshaller(TracErrorDetails.getDefaultInstance()));

    private static final String INTERNAL_ERROR_MESSAGE = "Internal server error";

    public static StatusRuntimeException processError(Throwable error) {

        // Unwrap future/streaming completion errors
        if (error instanceof CompletionException)
            error = error.getCause();

        // Status exceptions will come from failed gRPC calls to other services
        // We probably want to propagate the upstream error message back to the client/user
        // To produce a meaningful stacktrace, wrap the upstream error with one created in this function

        if (error instanceof StatusException) {

            var upstreamError = (StatusException) error;

            return upstreamError.getStatus()
                    .withCause(upstreamError)
                    .asRuntimeException(upstreamError.getTrailers());
        }

        if (error instanceof StatusRuntimeException) {

            var upstreamError = (StatusRuntimeException) error;

            return upstreamError.getStatus()
                    .withCause(upstreamError)
                    .asRuntimeException(upstreamError.getTrailers());
        }

        // For "public" errors, try to look up an error code mapping
        // A public error is an error that has been handled and is considered safe to report to the end client/user

        if (error instanceof ETracPublic) {

            var errorCode = lookupErrorCode(error);
            var details = ((ETracPublic) error).getDetails();

            if (errorCode != null) {

                // Report the error to back to the client with the mapped error code
                // If details are available in the exception, encode the details into the trailers
                // Otherwise build a basic details object and encode that instead

                var trailers = details != null
                        ? detailedErrorTrailers(errorCode, error.getMessage(), details)
                        : basicErrorTrailers(errorCode, error.getMessage());

                return Status.fromCode(errorCode)
                        .withDescription(error.getMessage())
                        .withCause(error)
                        .asRuntimeException(trailers);
            }
            else {

                // If the error code is not recognized, fall back to reporting an internal error
                // Public errors should normally be reported to the client/user, so log this as a warning

                log.warn("No gRPC error code mapping is available for the error {}", error.getClass().getSimpleName());

                var trailers = basicErrorTrailers(Status.Code.INTERNAL, Status.INTERNAL.getDescription());

                return Status.fromCode(Status.Code.INTERNAL)
                        .withDescription(Status.INTERNAL.getDescription())
                        .withCause(error)
                        .asRuntimeException(trailers);
            }
        }

        // Make sure internal errors are always reported as internal and the error description is masked

        if (error instanceof ETracInternal) {

            var trailers = basicErrorTrailers(Status.Code.INTERNAL, INTERNAL_ERROR_MESSAGE);

            return Status.fromCode(Status.Code.INTERNAL)
                    .withDescription(INTERNAL_ERROR_MESSAGE)
                    .withCause(error)
                    .asRuntimeException(trailers);
        }

        // Any error that is not either a TRAC error or a gRPC error means that the error has not been handled
        // These are still reported as internal errors
        // Log an extra message as well, to make it clear that the error was not handled at source

        log.error("An unhandled error has reached the top level error handler", error);

        var trailers = basicErrorTrailers(Status.Code.INTERNAL, INTERNAL_ERROR_MESSAGE);

        return Status.fromCode(Status.Code.INTERNAL)
                .withDescription(INTERNAL_ERROR_MESSAGE)
                .withCause(error)
                .asRuntimeException(trailers);
    }

    // Add a basic error details object to the trailers, with just status code and message

    private static Metadata basicErrorTrailers(Status.Code statusCode, String message) {

        var tracErrorDetails = TracErrorDetails.newBuilder()
                .setCode(statusCode.value())
                .setMessage(message)
                .build();

        var trailers = new Metadata();
        trailers.put(TRAC_ERROR_DETAILS_KEY, tracErrorDetails);

        return trailers;
    }

    // Add full error details from the application code into the error details trailer

    private static Metadata detailedErrorTrailers(Status.Code statusCode, String message, TracErrorDetails details) {

        var tracErrorDetails = TracErrorDetails.newBuilder()
                .setCode(statusCode.value())
                .setMessage(message)
                .addAllItems(details.getItemsList())
                .build();

        var trailers = new Metadata();
        trailers.put(TRAC_ERROR_DETAILS_KEY, tracErrorDetails);

        return trailers;
    }


    // The error mapping needs to be ordered, because more specific errors take precedence over less specific ones
    // Also we need to look for child classes, e.g. EData is the parent of several data error types
    // So, use a list of map entries and look along the list in order to find the error code

    private static Status.Code lookupErrorCode(Throwable error) {

        for (var mapping : ERROR_MAPPING) {

            if (mapping.getKey().isInstance(error))
                return mapping.getValue();
        }

        return null;
    }

    private static final List<Map.Entry<Class<? extends Throwable>, Status.Code>> ERROR_MAPPING = List.of(

            Map.entry(EAuthorization.class, Status.Code.PERMISSION_DENIED),

            Map.entry(EInputValidation.class, Status.Code.INVALID_ARGUMENT),
            Map.entry(EVersionValidation.class, Status.Code.FAILED_PRECONDITION),
            Map.entry(EConsistencyValidation.class, Status.Code.FAILED_PRECONDITION),

            Map.entry(ETenantNotFound.class, Status.Code.NOT_FOUND),

            Map.entry(EMetadataNotFound.class, Status.Code.NOT_FOUND),
            Map.entry(EMetadataDuplicate.class, Status.Code.ALREADY_EXISTS),
            Map.entry(EMetadataWrongType.class, Status.Code.FAILED_PRECONDITION),
            Map.entry(EMetadataBadUpdate.class, Status.Code.FAILED_PRECONDITION),
            Map.entry(EMetadataCorrupt.class, Status.Code.DATA_LOSS),

            Map.entry(EData.class, Status.Code.DATA_LOSS),

            Map.entry(EPluginNotAvailable.class, Status.Code.UNIMPLEMENTED));
}
