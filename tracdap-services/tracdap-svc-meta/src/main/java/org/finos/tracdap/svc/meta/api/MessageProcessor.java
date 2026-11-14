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

package org.finos.tracdap.svc.meta.api;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.finos.tracdap.api.internal.ConfigUpdate;
import org.finos.tracdap.api.internal.InternalMessagingApiGrpc;
import org.finos.tracdap.api.internal.PlatformConfigUpdate;
import org.finos.tracdap.api.internal.ReceivedCode;
import org.finos.tracdap.api.internal.ReceivedStatus;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.common.service.IPlatformConfigListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;


public class MessageProcessor extends InternalMessagingApiGrpc.InternalMessagingApiImplBase {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private final PluginRegistry registry;
    private final Executor listenerExecutor;

    public MessageProcessor(PluginRegistry registry, Executor listenerExecutor) {
        this.registry = registry;
        this.listenerExecutor = listenerExecutor;
    }

    @Override
    public void configUpdate(ConfigUpdate request, StreamObserver<ReceivedStatus> response) {

        // Metadata service does not currently use any dynamic config

        var status = ReceivedStatus.newBuilder()
                .setCode(ReceivedCode.IGNORED)
                .build();

        response.onNext(status);
        response.onCompleted();
    }

    @Override
    public void platformConfigUpdate(PlatformConfigUpdate request, StreamObserver<ReceivedStatus> response) {

        // Listeners call back into this service through the loopback client,
        // so they run outside the primary executor that serves those calls
        var callCtx = Context.current().fork();
        var callExecutor = callCtx.fixedContextExecutor(listenerExecutor);
        callExecutor.execute(() -> platformConfigUpdateOffloaded(request, response));
    }

    public void platformConfigUpdateOffloaded(PlatformConfigUpdate request, StreamObserver<ReceivedStatus> response) {

        try {

            var listener = registry.tryGetNamedInstance(IPlatformConfigListener.class, request.getConfigEntry().getConfigClass());

            var status = listener != null
                    ? listener.applyConfigUpdate(request)
                    : ReceivedStatus.newBuilder().setCode(ReceivedCode.IGNORED).build();

            response.onNext(status);
            response.onCompleted();
        }
        catch (Exception e) {

            log.error("Platform config update failed: class = {}, key = {}, {}",
                    request.getConfigEntry().getConfigClass(),
                    request.getConfigEntry().getConfigKey(),
                    e.getMessage(), e);

            response.onError(e);
        }
    }
}
