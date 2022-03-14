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

import org.finos.tracdap.common.exception.ETracInternal;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.Flow;


public class GrpcServerResponseStream<TResponse> implements Flow.Subscriber<TResponse> {

    private final StreamObserver<TResponse> grpcObserver;

    private Flow.Subscription subscription;

    public GrpcServerResponseStream(StreamObserver<TResponse> grpcObserver) {

        this.grpcObserver = grpcObserver;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        if (this.subscription != null)
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

        grpcObserver.onError(grpcError);
        subscription.cancel();
    }

    @Override
    public void onComplete() {

        grpcObserver.onCompleted();
    }
}
