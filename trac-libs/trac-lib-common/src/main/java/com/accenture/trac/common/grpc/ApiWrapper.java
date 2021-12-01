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

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;


public class ApiWrapper {

    private final Logger log;

    public ApiWrapper(Class<?> apiClass) {
        this.log = LoggerFactory.getLogger(apiClass);
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
                    log.error(methodName, error);

                    var status = GrpcErrorMapping.translateErrorStatus(error);
                    response.onError(status.asRuntimeException());
                }

                return null;
            });
        }
        catch (Exception error) {

            log.error("API CALL FAILED: {}", methodName);
            log.error(methodName, error);

            var status = GrpcErrorMapping.translateErrorStatus(error);
            response.onError(status.asRuntimeException());
        }
    }
}
