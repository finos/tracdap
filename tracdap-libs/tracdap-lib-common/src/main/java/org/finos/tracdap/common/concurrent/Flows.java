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

package org.finos.tracdap.common.concurrent;

import org.finos.tracdap.common.concurrent.flow.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;


public class Flows {

    public static <T>
    Flow.Publisher<T> publish(List<T> source) {

        return new SourcePublisher<>(source);
    }

    public static <T>
    Flow.Publisher<T> publish(Stream<T> source) {

        return new SourcePublisher<>(source);
    }

    public static <T>
    Flow.Subscriber<T> waitForSignal(Flow.Subscriber<T> target, CompletionStage<?> signal) {

        return new DelayedSubscriber<>(target, signal);
    }

    public static <T, U>
    Flow.Publisher<U> map(Flow.Publisher<T> source, Function<T, U> mapping) {

        var map = new MapProcessor<>(mapping);
        source.subscribe(map);

        return map;
    }

    public static <T>
    CompletionStage<T> reduce(Flow.Publisher<T> source, BiFunction<T, T, T> func) {

        var result = new CompletableFuture<T>();
        var reduce = new ReduceProcessor<>(func, result, Function.identity());
        source.subscribe(reduce);

        return result;
    }

    public static <T, U>
    CompletionStage<U> fold(Flow.Publisher<T> source, BiFunction<U, T, U> func, U acc) {

        var result = new CompletableFuture<U>();
        var fold = new ReduceProcessor<>(func, result, acc);
        source.subscribe(fold);

        return result;
    }

    public static <T>
    Flow.Processor<T, T> hub(IExecutionContext execCtx) {

        return new HubProcessor<>(execCtx.eventLoopExecutor());
    }

    public static <T>
    CompletionStage<T> first(Flow.Publisher<T> publisher) {

        var firstFuture = new CompletableFuture<T>();

        var subscriber = new FutureFirstItemSubscriber<>(firstFuture);
        publisher.subscribe(subscriber);

        return firstFuture;
    }

    public static <T>
    CompletionStage<List<T>> toList(Flow.Publisher<T> source) {

        return fold(source, (xs, x) -> {
            xs.add(x);
            return xs;
        }, new ArrayList<>());
    }

}
