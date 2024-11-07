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

import io.netty.util.concurrent.DefaultThreadFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A light wrapper around Netty's DefaultThreadFactory.
 * <br/>
 *
 * Standardises thread naming and creates a thread group.
 */
class NettyThreadFactoryWrapper implements ThreadFactory {

    private final ThreadFactory internalFactory;

    private final String threadPrefix;
    private final AtomicInteger threadIndex;

    public NettyThreadFactoryWrapper(String poolName) {
        this(poolName, false, Thread.NORM_PRIORITY);
    }

    public NettyThreadFactoryWrapper(String poolName, boolean daemon, int priority) {
        this(poolName, daemon, priority, new ThreadGroup(poolName));
    }

    public NettyThreadFactoryWrapper(String poolName, boolean daemon, int priority, ThreadGroup group) {
        this(poolName, new DefaultThreadFactory(poolName, daemon, priority, group));
    }

    public NettyThreadFactoryWrapper(String poolName, ThreadFactory internalFactory) {
        this.internalFactory = internalFactory;
        this.threadPrefix = poolName;
        this.threadIndex = new AtomicInteger(0);
    }

    @Override
    public Thread newThread(@Nonnull Runnable runnable) {

        var threadName = threadPrefix + "-" + threadIndex.getAndIncrement();
        var thread = internalFactory.newThread(runnable);

        thread.setName(threadName);

        return thread;
    }
}
