/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.netty;

import io.netty.util.concurrent.ThreadPerTaskExecutor;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.*;


public class NettyHelpers {

    public static int DEFAULT_MIN_THREAD_POOL_SIZE = 0;
    public static int DEFAULT_MAX_THREAD_POOL_SIZE = 10;
    public static Duration DEFAULT_THREAD_POOL_IDLE_LIMIT = Duration.of(30, ChronoUnit.SECONDS);

    public static ThreadFactory threadFactory(String threadPoolName) {
        return new NettyThreadFactoryWrapper(threadPoolName);
    }

    public static Executor eventLoopExecutor(String threadPoolName) {
        var threadFactory = threadFactory(threadPoolName);
        return new ThreadPerTaskExecutor(threadFactory);
    }

    public static ThreadPoolExecutor threadPoolExecutor(String threadPoolName) {
        return threadPoolExecutor(threadPoolName, DEFAULT_MIN_THREAD_POOL_SIZE, DEFAULT_MAX_THREAD_POOL_SIZE);
    }

    public static ThreadPoolExecutor threadPoolExecutor(String threadPoolName, int minSize, int maxSize) {
        return threadPoolExecutor(threadPoolName, minSize, maxSize, DEFAULT_THREAD_POOL_IDLE_LIMIT.toMillis());
    }

    public static ThreadPoolExecutor threadPoolExecutor(String threadPoolName, int minSize, int maxSize, long idleMs) {
        var threadFactory = threadFactory(threadPoolName);
        var workQueue = new ArrayBlockingQueue<Runnable>(maxSize);  // Double up max pool size with the work queue
        return new ThreadPoolExecutor(
                minSize, maxSize, idleMs, TimeUnit.MILLISECONDS,
                workQueue, threadFactory);
    }
}
