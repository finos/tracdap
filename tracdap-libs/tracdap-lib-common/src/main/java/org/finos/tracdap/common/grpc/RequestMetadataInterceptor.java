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

package org.finos.tracdap.common.grpc;

import io.grpc.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;


public class RequestMetadataInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT>
    interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        var requestId = UUID.randomUUID().toString();
        var requestTimestamp = Instant.now().atOffset(ZoneOffset.UTC);
        var requestMetadata = new RequestMetadata(requestId, requestTimestamp);

        var context = RequestMetadata.set(Context.current(), requestMetadata);

        return Contexts.interceptCall(context, call, headers, next);
    }
}
