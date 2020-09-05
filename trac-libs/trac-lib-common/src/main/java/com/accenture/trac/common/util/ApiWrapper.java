/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.common.util;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;


public class ApiWrapper {

    private final Logger log;

    private final Map<Class<? extends Throwable>, Status.Code> errorMapping;

    public ApiWrapper(Class<?> apiClass, Map<Class<? extends Throwable>, Status.Code> errorMapping) {
        this.log = LoggerFactory.getLogger(apiClass);
        this.errorMapping = errorMapping;
    }

    public <T> void unaryCall(StreamObserver<T> response, Supplier<CompletableFuture<T>> futureFunc) {

        var stack = StackWalker.getInstance();
        var method = stack.walk(frames -> frames.skip(1).findFirst());
        var methodName = method.isPresent() ? method.get().getMethodName() : "(unknown API method)";

        try {

            log.info("API CALL START: {}", methodName);

            futureFunc.get().handle((result, error) -> {

                if (result != null) {

                    log.info("API CALL SUCCEEDED: {}", methodName);

                    response.onNext(result);
                    response.onCompleted();
                }
                else {

                    log.error("API CALL FAILED: {}", methodName);

                    mapErrorResponse(response, error);
                }

                return null;
            });
        }
        catch (Exception error) {

            log.error("API CALL FAILED: {}", methodName);

            mapErrorResponse(response, error);
        }
    }

    private <T> void mapErrorResponse(StreamObserver<T> response, Throwable error) {

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

    private <T> void mapUnwrappedErrorResponse(StreamObserver<T> response, Throwable error) {

        var statusCode = error != null
                ? errorMapping.getOrDefault(error.getClass(), Status.Code.UNKNOWN)
                : Status.Code.UNKNOWN;

        var errorMessage = statusCode == Status.Code.UNKNOWN
                ? "There was an unexpected internal error in the TRAC platform"
                : error.getMessage();

        var status = Status.fromCode(statusCode)
                .withDescription(errorMessage)
                .withCause(error);

        response.onError(status.asRuntimeException());
    }
}
