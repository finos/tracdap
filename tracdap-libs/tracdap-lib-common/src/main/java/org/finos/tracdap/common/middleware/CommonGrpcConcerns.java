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

import io.grpc.ManagedChannelBuilder;
import org.finos.tracdap.common.exception.EUnexpected;

import io.grpc.Context;
import io.grpc.ServerBuilder;
import io.grpc.stub.AbstractStub;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CommonGrpcConcerns extends CommonConcerns<GrpcConcern> implements GrpcConcern {

    public CommonGrpcConcerns(String concernName) {
        super(concernName);
    }

    private CommonGrpcConcerns(String concernName, List<String> stageOrder, Map<String, GrpcConcern> stages) {
        super(concernName, stageOrder, stages);
    }

    @Override
    public GrpcConcern build() {

        var stageOrder = Collections.unmodifiableList(this.stageOrder);
        var stages = Collections.unmodifiableMap(this.stages);

        return new CommonGrpcConcerns(concernName(), stageOrder, stages);
    }

    // Implement GrpcConcern by applying registered concerns in order

    @Override
    public ServerBuilder<? extends ServerBuilder<?>> configureServer(ServerBuilder<? extends ServerBuilder<?>> serverBuilder) {

        // Server concerns can contain interceptors, so they need to be added in reverse order
        // The last interceptor added is the first one fired

        for (var i = stageOrder.size() - 1; i >= 0; i--) {
            var stageName = stageOrder.get(i);
            var stage = stages.get(stageName);
            serverBuilder = stage.configureServer(serverBuilder);
        }

        return serverBuilder;
    }

    @Override
    public <TStub extends AbstractStub<TStub>> TStub configureClient(TStub clientStub) {

        for (var stageName : stageOrder) {
            var stage = stages.get(stageName);
            clientStub = stage.configureClient(clientStub);
        }

        return clientStub;
    }

    @Override
    public ManagedChannelBuilder<? extends ManagedChannelBuilder<?>>
    configureClientChannel(ManagedChannelBuilder<? extends ManagedChannelBuilder<?>> channelBuilder) {

        for (var stageName : stageOrder) {
            var stage = stages.get(stageName);
            channelBuilder = stage.configureClientChannel(channelBuilder);
        }

        return channelBuilder;
    }

    @Override
    public GrpcClientState prepareClientCall(Context callContext) {

        var clientConfigs = new ArrayList<GrpcClientState>();

        for (var stageName : stageOrder) {
            var stage = stages.get(stageName);
            var stageCallConfig = stage.prepareClientCall(callContext);
            clientConfigs.add(stageCallConfig);
        }

        return new ClientCallConcerns(clientConfigs);
    }

    private static class ClientCallConcerns implements GrpcClientState {

        private final List<GrpcClientState> clientConfigs;

        public ClientCallConcerns(List<GrpcClientState> clientConfigs) {
            this.clientConfigs = clientConfigs;
        }

        @Override
        public <TStub extends AbstractStub<TStub>> TStub configureClient(TStub clientStub) {

            for (var config : clientConfigs)
                clientStub = config.configureClient(clientStub);

            return clientStub;
        }

        @Override
        public GrpcClientState restore(GrpcConcern grpcConcern) {

            if (!(grpcConcern instanceof CommonGrpcConcerns))
                throw new EUnexpected();

            var commonConcerns = (CommonGrpcConcerns) grpcConcern;

            for (int i = 0; i < commonConcerns.stageOrder.size(); i++) {

                var concernName = commonConcerns.stageOrder.get(i);
                var concern = commonConcerns.stages.get(concernName);

                var config = clientConfigs.get(i);
                config.restore(concern);
            }

            return this;
        }
    }
}
