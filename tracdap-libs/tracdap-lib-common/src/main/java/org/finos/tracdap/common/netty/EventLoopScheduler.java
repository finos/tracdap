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

import org.finos.tracdap.common.exception.ETracInternal;

import io.netty.util.concurrent.DefaultEventExecutorChooserFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorChooserFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This scheduler tries to schedule events to the same event loop it is called from.
 * <br/>
 *
 * Typically this will be scheduling a Netty client channel to an event loop and it
 * will be called from an existing event loop that is processing a server request.
 * It will attempt to schedule the client on the same event loop. A fallback scheduler
 * can be provided, in case the current event loop cannot be determined or the scheduler
 * is called from a thread outside the event loop group.
 * <br/>
 *
 * Offload tracking is available, this will attempt to track tasks running on offloaded
 * threads back to the service thread that spawned the task. gRPC uses task offloading
 * for various internal operations, including DNS resolution, so it is common for client
 * calls to pass through offloaded threads. The same mechanism can be used to resolve
 * long-running worker tasks spawned by application code.
 */
public class EventLoopScheduler implements EventExecutorChooserFactory {

    public static EventExecutorChooserFactory roundRobin() {
        return DefaultEventExecutorChooserFactory.INSTANCE;
    }

    public static EventLoopScheduler preferLoopAffinity() {
        return new EventLoopScheduler(roundRobin(), null);
    }

    public static EventLoopScheduler preferLoopAffinity(EventLoopOffloadTracker offloadTracker) {
        return new EventLoopScheduler(roundRobin(), offloadTracker);
    }

    public static EventLoopScheduler requireLoopAffinity() {
        return new EventLoopScheduler(null, null);
    }

    public static EventLoopScheduler requireLoopAffinity(EventLoopOffloadTracker offloadTracker) {
        return new EventLoopScheduler(null, offloadTracker);
    }

    private final EventExecutorChooserFactory fallbackFactory;
    private final EventLoopOffloadTracker offloadTracker;

    private EventLoopScheduler(EventExecutorChooserFactory fallbackFactory, EventLoopOffloadTracker offloadTracker) {
        this.fallbackFactory = fallbackFactory;
        this.offloadTracker = offloadTracker;
    }

    @Override
    public EventExecutorChooserFactory.EventExecutorChooser newChooser(EventExecutor[] eventExecutors) {

        var resolver = new EventLoopResolver(eventExecutors, offloadTracker);

        var fallback = fallbackFactory != null
                ? fallbackFactory.newChooser(eventExecutors)
                : null;

        return new Chooser(resolver, fallback);
    }

    private static class Chooser implements EventExecutorChooserFactory.EventExecutorChooser {

        private final Logger log = LoggerFactory.getLogger(getClass());

        private final EventLoopResolver resolver;
        EventExecutorChooserFactory.EventExecutorChooser fallback;

        Chooser(EventLoopResolver resolver, EventExecutorChooserFactory.EventExecutorChooser fallback) {
            this.resolver = resolver;
            this.fallback = fallback;
        }

        @Override
        public EventExecutor next() {

            log.info("Choosing executor from thread: [{}]", Thread.currentThread().getName());

            var callingEventLoop = resolver.callingEvnetLoop(false);

            if (callingEventLoop != null)
                return callingEventLoop;

            if (fallback != null)
                return fallback.next();

            throw new ETracInternal("The current operation is running outside the registered event loop group");
        }
    }
}
