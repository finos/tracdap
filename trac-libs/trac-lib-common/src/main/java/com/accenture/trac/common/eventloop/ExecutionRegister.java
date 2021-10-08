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

package com.accenture.trac.common.eventloop;

import com.accenture.trac.common.exception.ETracInternal;
import io.grpc.*;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.OrderedEventExecutor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class ExecutionRegister {

    private final EventExecutorGroup executorGroup;
    private final ConcurrentMap<String, OrderedEventExecutor> executors;

    public ExecutionRegister(EventExecutorGroup executorGroup) {
        this.executorGroup = executorGroup;
        this.executors = new ConcurrentHashMap<>();
    }

    public ServerInterceptor registerExecContext() {

        return new RegisterContextInterceptor();
    }

    private IExecutionContext execContextForThread() {

        var eventLoopKey = Thread.currentThread().getName();
        var executor = executors.get(eventLoopKey);

        if (executor == null)
            executor = registerEventLoopKey(eventLoopKey);

        return new ExecutionContext(executor);
    }

    private OrderedEventExecutor registerEventLoopKey(String eventLoopKey) {

        for (var eventExec : executorGroup) {
            if (eventExec.inEventLoop() && eventExec instanceof OrderedEventExecutor) {
                executors.putIfAbsent(eventLoopKey, (OrderedEventExecutor) eventExec);
                return (OrderedEventExecutor) eventExec;
            }
        }

        throw new ETracInternal("Netty event loop manager is running outside of the worker event loop group");
    }

    private class RegisterContextInterceptor implements ServerInterceptor {

        @Override
        public <ReqT, RespT>
        ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {

            var execCtx = execContextForThread();

            var grpcCtx = Context
                    .current()
                    .withValue(ExecutionContext.EXEC_CONTEXT_KEY, execCtx);

            return Contexts.interceptCall(grpcCtx, call, headers, next);
        }
    }
}
