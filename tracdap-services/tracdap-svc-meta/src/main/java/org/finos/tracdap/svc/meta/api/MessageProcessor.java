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

import io.grpc.stub.StreamObserver;
import org.finos.tracdap.api.internal.ConfigUpdate;
import org.finos.tracdap.api.internal.InternalMessagingApiGrpc;
import org.finos.tracdap.api.internal.ReceivedCode;
import org.finos.tracdap.api.internal.ReceivedStatus;


public class MessageProcessor extends InternalMessagingApiGrpc.InternalMessagingApiImplBase {

    public MessageProcessor() {}

    @Override
    public void configUpdate(ConfigUpdate request, StreamObserver<ReceivedStatus> response) {

        // Metadata service does not currently use any dynamic config

        var status = ReceivedStatus.newBuilder()
                .setCode(ReceivedCode.IGNORED)
                .build();

        response.onNext(status);
        response.onCompleted();
    }
}
