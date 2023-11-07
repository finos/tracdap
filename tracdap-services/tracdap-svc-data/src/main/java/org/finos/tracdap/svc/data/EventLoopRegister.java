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

package org.finos.tracdap.svc.data;

import io.grpc.*;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class EventLoopRegister {

    // Java 19 introduces Thread.threadId() and deprecates Thread.getId()
    // The GET_THREAD_ID method is resolved to one or other depending on the Java version

    private static final Method GET_THREAD_ID = getThreadIdMethod();

    private final EventLoopGroup elg;
    private final Map<Long, EventLoop> register;

    public EventLoopRegister(EventLoopGroup elg) {

        this.elg = elg;
        this.register = new ConcurrentHashMap<>();

        elg.forEach(el -> el.submit(() -> registerEventLoop(el)));
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

    public EventLoop currentEventLoop(boolean strict) {

        var threadId = getThreadId(Thread.currentThread());
        var el = register.get(threadId);

        if (el != null)
            return el;

        if (!strict)
            return elg.next();

        throw new ETracInternal("The current operation is running outside the registered event loop group");
    }

    public ClientInterceptor clientInterceptor() {

        return new EventLoopInterceptor();
    }

    private void registerEventLoop(EventExecutor executor) {

        if (executor instanceof EventLoop) {
            var threadId = getThreadId(Thread.currentThread());
            register.put(threadId, (EventLoop) executor);
        }
    }

    private class EventLoopInterceptor implements ClientInterceptor {

        @Override public <ReqT, RespT>
        ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method,
                CallOptions callOptions,
                Channel channel) {

            // Enforce strict requirement on the event loop
            // Client interceptor is used when the data service makes calls to other TRAC services
            // Those calls must come back on the same EL, so processing is not split across ELs

            var el = currentEventLoop(/* strict = */ true);
            var options = callOptions.withExecutor(el);

            return channel.newCall(method, options);
        }
    }
}
