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

import com.accenture.trac.common.exception.ETracInternal;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;


public class GrpcServerResponseStream<TResponse> implements Flow.Subscriber<TResponse> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final MethodDescriptor<?, TResponse> method;
    private final StreamObserver<TResponse> grpcObserver;

    private final AtomicBoolean subscribed = new AtomicBoolean(false);
    private Flow.Subscription subscription;

    public GrpcServerResponseStream(
            MethodDescriptor<?, TResponse> method,
            StreamObserver<TResponse> grpcObserver) {

        this.method = method;
        this.grpcObserver = grpcObserver;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        var subscribedOk = this.subscribed.compareAndSet(false, true);

        if (!subscribedOk)
            throw new ETracInternal("Multiple subscriptions on gRPC observer wrapper");

        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(TResponse item) {
        grpcObserver.onNext(item);
        subscription.request(1);
    }

    @Override
    public void onError(Throwable error) {

        var grpcError = GrpcErrorMapping.processError(error);

        log.error("SERVER STREAMING CALL FAILED: [{}] {}",
                method.getBareMethodName(),
                grpcError.getMessage(),
                grpcError);

        grpcObserver.onError(grpcError);
        subscription.cancel();
    }

    @Override
    public void onComplete() {

        log.info("SERVER STREAMING CALL SUCCEEDED: [{}]", method.getBareMethodName());

        grpcObserver.onCompleted();
    }
}
