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

import com.accenture.trac.common.exception.*;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.finos.tracdap.common.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;


public class GrpcErrorMapping {

    private static final Logger log = LoggerFactory.getLogger(GrpcErrorMapping.class);

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
                    .asRuntimeException();
        }

        if (error instanceof StatusRuntimeException) {

            var upstreamError = (StatusRuntimeException) error;

            return upstreamError.getStatus()
                    .withCause(upstreamError)
                    .asRuntimeException();
        }

        // Make sure internal errors are always reported as internal and the error description is masked

        if (error instanceof ETracInternal) {

            return Status.fromCode(Status.Code.INTERNAL)
                    .withDescription(Status.INTERNAL.getDescription())
                    .withCause(error)
                    .asRuntimeException();
        }

        // For "public" errors, try to look up an error code mapping
        // A public error is an error that has been handled and is considered safe to report to the end client/user

        if (error instanceof ETracPublic) {

            var errorCode = lookupErrorCode(error);

            if (errorCode != null) {

                // If there is an error code mapping, report the error to back to the client with the mapped error code

                return Status.fromCode(errorCode)
                        .withDescription(error.getMessage())
                        .withCause(error)
                        .asRuntimeException();
            }
            else {

                // For anything unrecognized, fall back to an internal error
                // Public errors should normally be reported to the client/user, so log this as a warning

                log.warn("No gRPC error code mapping is available for the error {}", error.getClass().getSimpleName());

                return Status.fromCode(Status.Code.INTERNAL)
                        .withDescription(Status.INTERNAL.getDescription())
                        .withCause(error)
                        .asRuntimeException();
            }
        }

        // Any error that is not either a TRAC error or a gRPC error means that the error has not been handled
        // These are still reported as internal errors
        // Let's log an extra message as well, to make it clear that the error was not handled at source

        log.error("An unhandled error has reached the top level error handler", error);

        return Status.fromCode(Status.Code.INTERNAL)
                .withDescription(Status.INTERNAL.getDescription())
                .withCause(error)
                .asRuntimeException();
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

            Map.entry(ETenantNotFound.class, Status.Code.NOT_FOUND),

            Map.entry(EMetadataNotFound.class, Status.Code.NOT_FOUND),
            Map.entry(EMetadataDuplicate.class, Status.Code.ALREADY_EXISTS),
            Map.entry(EMetadataWrongType.class, Status.Code.FAILED_PRECONDITION),
            Map.entry(EMetadataBadUpdate.class, Status.Code.FAILED_PRECONDITION),
            Map.entry(EMetadataCorrupt.class, Status.Code.DATA_LOSS),

            Map.entry(EData.class, Status.Code.DATA_LOSS),

            Map.entry(EPluginNotAvailable.class, Status.Code.UNIMPLEMENTED));
}
