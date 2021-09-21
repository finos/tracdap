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

import io.grpc.*;
import io.netty.util.concurrent.EventExecutor;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;

public class ExecutionContext implements IExecutionContext {

    public static final Context.Key<IExecutionContext> EXEC_CONTEXT_KEY = Context.key("TRAC_EXEC_CONTEXT");

    private final EventExecutor eventLoopExecutor;

    public ExecutionContext(EventExecutor eventLoopExecutor) {
        this.eventLoopExecutor = eventLoopExecutor;
    }

    @Override
    public EventExecutor eventLoopExecutor() {
        return eventLoopExecutor;
    }

    public ServerCallExecutorSupplier eventLoopServerCall() {

        return new ServerCallExecutorSupplier() {

            @Override @Nullable
            public <ReqT, RespT>
            Executor getExecutor(ServerCall<ReqT, RespT> call, Metadata metadata) {

                return eventLoopExecutor;
            }
        };
    }

    public ClientInterceptor eventLoopClientCall() {

        return new ClientInterceptor() {

            @Override
            public <ReqT, RespT>
            ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions,
                    Channel next) {

                var options = callOptions.withExecutor(eventLoopExecutor);
                return next.newCall(method, options);
            }
        };
    }
}
