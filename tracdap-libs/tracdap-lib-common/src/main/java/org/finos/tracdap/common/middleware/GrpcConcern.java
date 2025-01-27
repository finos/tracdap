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

package org.finos.tracdap.common.middleware;

import io.grpc.Context;
import io.grpc.ServerBuilder;
import io.grpc.stub.AbstractStub;


public interface GrpcConcern extends GrpcServerConfig, GrpcClientConfig, BaseConcern {

    @Override
    default ServerBuilder<? extends ServerBuilder<?>>
    configureServer(ServerBuilder<? extends ServerBuilder<?>> serverBuilder) {
        return serverBuilder;
    }

    @Override
    default <TStub extends AbstractStub<TStub>> TStub
    configureClient(TStub clientStub) {
        return clientStub;
    }

    default GrpcClientConfig prepareClientCall(Context callContext) {
        return NOOP_CLIENT_CONFIG;
    }

    GrpcClientConfig NOOP_CLIENT_CONFIG = new GrpcClientConfig() {

        @Override
        public <TStub extends AbstractStub<TStub>> TStub
        configureClient(TStub clientStub) {
            return clientStub;
        }
    };
}
