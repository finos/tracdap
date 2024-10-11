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
import org.finos.tracdap.common.exception.EUnexpected;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.OrderedEventExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Resolve the Netty event loop for the current thread.
 * <br/>
 *
 * Two resolution methods are supported, thread ID map and directly checking inEventLoop().
 * The strict flag in the resolve method controls what happens if the event loop cannot be
 * resolved, strict = true will raise ETracInternal, strict = false will return null. This
 * class provides event loop resolution for EventLoopInterceptor and EventLoopScheduler, it
 * can also be called directly.
 * <br/>
 *
 * Offload tracking can be supported by passing in an offload tracker and using the method
 * callingEventLoop(). The method currentEventLoop() will never resolve offloads.
 */
public class EventLoopResolver {

    // Java 19 introduces Thread.threadId() and deprecates Thread.getId()
    // The GET_THREAD_ID method is resolved to one or other depending on the Java version

    private static final Method GET_THREAD_ID = getThreadIdMethod();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<Long, OrderedEventExecutor> register;
    private final EventExecutor[] eventExecutors;
    private final EventLoopOffloadTracker offloadTracker;

    public EventLoopResolver(EventExecutorGroup eventLoopGroup) {
        this(eventLoopGroup, null);
    }

    public EventLoopResolver(EventExecutorGroup eventLoopGroup, EventLoopOffloadTracker offloadTracker) {

        this.register = new ConcurrentHashMap<>();
        this.eventExecutors = null;
        this.offloadTracker = offloadTracker;

        eventLoopGroup.forEach(el -> el.submit(() -> registerEventLoop(el)));
    }

    public EventLoopResolver(EventExecutor[] eventExecutors, EventLoopOffloadTracker offloadTracker) {

        this.register = null;
        this.eventExecutors = eventExecutors;
        this.offloadTracker = offloadTracker;
    }

    private void registerEventLoop(EventExecutor executor) {

        if (executor instanceof OrderedEventExecutor) {
            var threadId = getThreadId(Thread.currentThread());
            register.put(threadId, (OrderedEventExecutor) executor);
        }
    }

    private static long getThreadId(Thread thread) {

        // Invoke the method that was found for GET_THREAD_ID
        // Errors are not expected

        try {
            return (long) GET_THREAD_ID.invoke(thread);
        }
        catch (InvocationTargetException | IllegalAccessException e) {
            throw new EUnexpected(e);
        }
    }

    @SuppressWarnings("all")
    private static Method getThreadIdMethod() {

        // Try to find the best method for getting thread IDs
        // Reflective access to non-existent methods is a warning, so using SuppressWarnings on this method

        try {

            // Thread.threadId() should be available for Java 19+ and is the first choice
            try {
                return Thread.class.getMethod("threadId");
            }
            catch (NoSuchMethodException e) {
                // no-op
            }

            // Thread.getId() is deprecated as of Java 19 and is the fallback choice
            // As of Java 21, Thread.getId() has not been removed
            try {
                return Thread.class.getMethod("getId");
            }
            catch (NoSuchMethodException e) {
                // no-op
            }

            // Errors are not expected, at least one of these methods should be available

            throw new EUnexpected();
        }
        catch (Exception e) {
            throw new EUnexpected(e);
        }
    }

    public OrderedEventExecutor currentEventLoop(boolean strict) {

        return resolveEventLoop(strict, false);
    }

    public OrderedEventExecutor callingEvnetLoop(boolean strict) {

        return resolveEventLoop(strict, true);
    }

    private OrderedEventExecutor resolveEventLoop(boolean strict, boolean includeOffloadMapping) {

        OrderedEventExecutor eventLoop;

        if (register != null) {
            eventLoop = resolveByRegister(includeOffloadMapping);
        }
        else if (eventExecutors != null) {
            eventLoop = (OrderedEventExecutor) resolveByDirectTest(includeOffloadMapping);
        }
        else {
            eventLoop = null;
        }

        if (eventLoop != null)
            return eventLoop;

        if (strict) {
            var message = "The current operation is running outside the registered event loop group";
            log.error(message);
            throw new ETracInternal(message);
        }
        else
            return null;
    }

    private OrderedEventExecutor resolveByRegister(boolean includeOffloadMapping) {

        var currentThreadId = getThreadId(Thread.currentThread());
        var currentEventLoop = register.get(currentThreadId);

        if (currentEventLoop != null)
            return currentEventLoop;

        if (includeOffloadMapping && offloadTracker != null) {
            var callingThread = offloadTracker.offloadCallingThread();
            if (callingThread != null) {
                var callingEventLoop = register.get(callingThread.getId());
                if (callingEventLoop != null)
                    return callingEventLoop;;
            }
        }

        return null;
    }

    private EventExecutor resolveByDirectTest(boolean includeOffloadMapping) {

        for (var eventExecutor : eventExecutors) {
            if (eventExecutor.inEventLoop())
                return eventExecutor;
        }

        if (includeOffloadMapping && offloadTracker != null) {
            var callingThread = offloadTracker.offloadCallingThread();
            if (callingThread != null) {
                for (var eventExecutor : eventExecutors) {
                    if (eventExecutor.inEventLoop(callingThread))
                        return eventExecutor;
                }
            }
        }

        return null;
    }
}
