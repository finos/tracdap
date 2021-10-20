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

package com.accenture.trac.test.concurrent;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;


public class ConcurrentTestHelpers {

    public static void waitFor(Duration timeout, CompletionStage<?>... tasks) {

        waitFor(timeout, Arrays.asList(tasks));
    }

    public static void waitFor(Duration timeout, List<CompletionStage<?>> tasks) {

        var latch = new CountDownLatch(tasks.size());

        for (var task: tasks)
            task.whenComplete((result, error) -> latch.countDown());

        try {
            var complete = latch.await(timeout.getSeconds(), TimeUnit.SECONDS);

            if (!complete)
                throw new RuntimeException("Test timed out");
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Test interrupted", e);
        }
    }

    public static <T> T resultOf(CompletionStage<T> task, boolean unwrap) throws Exception {

        var taskFuture = task.toCompletableFuture();

        if (!taskFuture.isDone())
            throw new RuntimeException("Result of task is not ready");

        // Calling join() will always wrap errors with CompletionError
        // We want to get the exception that was originally used to complete the task
        // A few test cases check for the completion explicitly, they can set unwrap = false

        try {
            return task.toCompletableFuture().join();
        }
        catch (CompletionException e) {

            if (!unwrap)
                throw e;

            var cause = e.getCause();
            throw (cause instanceof Exception) ? (Exception) cause : e;
        }
    }

    public static <T> T resultOf(CompletionStage<T> task) throws Exception {

        return resultOf(task, true);
    }
}
