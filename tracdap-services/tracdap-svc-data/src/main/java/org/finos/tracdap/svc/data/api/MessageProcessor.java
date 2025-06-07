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

package org.finos.tracdap.svc.data.api;

import org.finos.tracdap.api.internal.*;
import org.finos.tracdap.svc.data.service.TenantStorageManager;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ExecutorService;


public class MessageProcessor extends InternalMessagingApiGrpc.InternalMessagingApiImplBase {

    private final TenantStorageManager tenantState;

    private final ExecutorService offloadExecutor;

    public MessageProcessor(TenantStorageManager tenantState, ExecutorService offloadExecutor) {

        this.tenantState = tenantState;
        this.offloadExecutor = offloadExecutor;
    }

    @Override
    public void configUpdate(ConfigUpdate request, StreamObserver<ReceivedStatus> response) {

        // This call will return immediately and close the server call context
        // Create a forked context, which can live for the duration of the client call
        var callCtx = Context.current().fork();

        // Do not execute config updates on the primary event loop!
        var callExecutor = callCtx.fixedContextExecutor(offloadExecutor);
        callExecutor.execute(() -> configUpdateOffloaded(request, response));
    }

    public void configUpdateOffloaded(ConfigUpdate request, StreamObserver<ReceivedStatus> response) {

        var status = tenantState.applyConfigUpdate(request);

        response.onNext(status);
        response.onCompleted();
    }
}
