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

package com.accenture.trac.common.util;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.stub.StreamObserver;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;


public class Futures {

    public static <T>
    CompletionStage<T> javaFuture(com.google.common.util.concurrent.ListenableFuture<T> guavaFuture) {

        var javaFuture = new CompletableFuture<T>();

        com.google.common.util.concurrent.Futures.addCallback(guavaFuture, new FutureCallback<>() {

            @Override
            public void onSuccess(T result) {
                javaFuture.complete(result);
            }

            @Override
            public void onFailure(Throwable error) {
                javaFuture.completeExceptionally(error);
            }

        }, MoreExecutors.directExecutor());

        return javaFuture;
    }

    public static <T>
    CompletionStage<T> javaFuture(io.netty.util.concurrent.Future<T> nettyFuture) {

        var javaFuture = new CompletableFuture<T>();

        nettyFuture.addListener((GenericFutureListener<Future<T>>) f -> {

            if (f.isSuccess())
                javaFuture.complete(f.getNow());
            else
                javaFuture.completeExceptionally(f.cause());
        });

        return javaFuture;
    }
}
