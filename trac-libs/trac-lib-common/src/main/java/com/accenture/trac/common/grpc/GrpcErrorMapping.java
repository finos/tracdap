/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.common.grpc;

import com.accenture.trac.common.exception.*;

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

    public static StatusRuntimeException processError(Throwable error) {

        // Unwrap future/streaming completion errors
        if (error instanceof CompletionException)
            error = error.getCause();

        // Status runtime exception is a gRPC exception that is already propagated in the event stream
        // This is most likely the result of an error when calling into another TRAC service
        // For now, pass these errors on directly
        // At some point there may (or may not) be benefit in wrapping/transforming upstream errors
        // E.g. to handle particular types of expected exception
        // However a lot of error response translate directly
        // E.g. for metadata not found or permission denied lower down the stack - there is little benefit to wrapping

        if (error instanceof StatusRuntimeException)
            return (StatusRuntimeException) error;

        if (error instanceof StatusException)
            return ((StatusException) error).getStatus().asRuntimeException();

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

    static final List<Map.Entry<Class<? extends Throwable>, Status.Code>> ERROR_MAPPING = List.of(

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
