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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class EventLoopRegister {

    private final EventLoopGroup elg;
    private final Map<Long, EventLoop> register;

    public EventLoopRegister(EventLoopGroup elg) {

        this.elg = elg;
        this.register = new ConcurrentHashMap<>();

        elg.forEach(el -> el.submit(() -> registerEventLoop(el)));
    }

    public EventLoop currentEventLoop(boolean strict) {

        var threadId = Thread.currentThread().getId();
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
            var threadId = Thread.currentThread().getId();
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
