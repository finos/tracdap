/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
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

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * A mechanism to track offloaded tasks back to the event loop that spawned them.
 * <br/>
 *
 * By wrapping an executor using the offload tracker, tasks submitted to the executor
 * can be resolved back to the thread that spawned them. This can be used for event loop
 * resolution on offloaded tasks.
 * <br/>
 *
 * Currently, offload resolution is only one level deep. I.e. if an event loop spawns task
 * A, and task A spawns task B, then A can be resolved back to the event loop but B cannot.
 * Resolution is only available as long as the task is running, once the task completes its
 * thread can no longer be resolved (that thread may take up a new task, or be destroyed).
 */
public class EventLoopOffloadTracker {

    private final ThreadLocal<Thread> offloadMap = new ThreadLocal<>();

    public Executor wrappExecutor(Executor baseExecutor) {
        return new OffloadedExecutor(baseExecutor);
    }

    public ExecutorService wrappExecutorService(ExecutorService baseExecutor) {
        return new OffloadedExecutorService(baseExecutor);
    }

    public Thread offloadCallingThread() {
        return offloadMap.get();
    }

    private void offloadTask(Thread callingThread, Runnable task) {

        try {
            offloadMap.set(callingThread);
            task.run();
        }
        finally {
            offloadMap.remove();
        }
    }

    private class OffloadedExecutor implements Executor {

        private final Executor baseExecutor;

        public OffloadedExecutor(Executor baseExecutor) {
            this.baseExecutor = baseExecutor;
        }

        @Override
        public void execute(@Nonnull Runnable command) {
            var currentThread = Thread.currentThread();
            baseExecutor.execute(() -> offloadTask(currentThread, command));
        }
    }

    private class OffloadedExecutorService extends AbstractExecutorService {

        private final ExecutorService baseExecutor;

        OffloadedExecutorService(ExecutorService baseExecutor) {
            this.baseExecutor = baseExecutor;
        }

        @Override
        public void shutdown() {
            baseExecutor.shutdown();
        }

        @Override
        public @Nonnull List<Runnable> shutdownNow() {
            return baseExecutor.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return baseExecutor.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return baseExecutor.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
            return baseExecutor.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(@Nonnull Runnable command) {
            var currentThread = Thread.currentThread();
            baseExecutor.execute(() -> offloadTask(currentThread, command));
        }
    }
}
