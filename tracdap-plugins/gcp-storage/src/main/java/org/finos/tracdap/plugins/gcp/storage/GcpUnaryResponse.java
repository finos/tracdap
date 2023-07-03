/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.plugins.gcp.storage;

import com.google.api.gax.rpc.ApiStreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;


public class GcpUnaryResponse<TResponse> implements ApiStreamObserver<TResponse> {

    private final CompletableFuture<TResponse> result;
    private final AtomicReference<TResponse> valueHolder;

    public GcpUnaryResponse() {
        this.result = new CompletableFuture<>();
        this.valueHolder = new AtomicReference<>();
    }

    public CompletionStage<TResponse> getResult() {
        return result;
    }

    @Override
    public void onNext(TResponse value) {

        if (!valueHolder.compareAndSet(null, value))
            throw new IllegalStateException();
    }

    @Override
    public void onError(Throwable t) {

        result.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {

        var value = valueHolder.get();

        if (value == null)
            throw new IllegalStateException();

        result.complete(value);
    }
}
