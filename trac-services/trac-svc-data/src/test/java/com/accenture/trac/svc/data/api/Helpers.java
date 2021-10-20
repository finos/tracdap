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

package com.accenture.trac.svc.data.api;

import com.accenture.trac.api.FileReadRequest;
import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.metadata.MetadataUtil;
import com.accenture.trac.common.concurrent.Flows;
import com.accenture.trac.common.util.GrpcStreams;
import com.accenture.trac.metadata.TagHeader;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.accenture.trac.svc.data.api.DataApiTest_Base.TEST_TENANT;

class Helpers {

    static <TReq, TResp>
    void serverStreaming(
            BiConsumer<TReq, StreamObserver<TResp>> grpcMethod,
            TReq request, Flow.Subscriber<TResp> response) {

        var responseGrpc = GrpcStreams.clientResponseStream(response);
        grpcMethod.accept(request, responseGrpc);
    }

    static <TReq, TResp>
    CompletionStage<Void> serverStreamingDiscard(
            BiConsumer<TReq, StreamObserver<TResp>> grpcMethod,
            TReq request, IExecutionContext execCtx) {

        // Server streaming response uses ByteString for binary data
        // ByteString does not need an explicit release

        var msgStream = Flows.<TResp>hub(execCtx);
        var discard = Flows.fold(msgStream, (acc, msg) -> acc, (Void) null);

        var grpcStream = GrpcStreams.clientResponseStream(msgStream);
        grpcMethod.accept(request, grpcStream);

        return discard;
    }

    static <TReq, TResp>
    CompletableFuture<TResp> clientStreaming(
            Function<StreamObserver<TResp>, StreamObserver<TReq>> grpcMethod,
            Flow.Publisher<TReq> requestPublisher) {

        var response = new CompletableFuture<TResp>();

        var responseGrpc = GrpcStreams.clientResponseHandler(response);
        var requestGrpc = grpcMethod.apply(responseGrpc);
        var requestSubscriber = GrpcStreams.clientRequestStream(requestGrpc);

        requestPublisher.subscribe(requestSubscriber);

        return response;
    }

    static <TReq, TResp>
    CompletableFuture<TResp> clientStreaming(
            Function<StreamObserver<TResp>, StreamObserver<TReq>> grpcMethod,
            TReq request) {

        return clientStreaming(grpcMethod, Flows.publish(Stream.of(request)));
    }

    static FileReadRequest readRequest(TagHeader fileId) {

        var fileSelector = MetadataUtil.selectorFor(fileId);

        return FileReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(fileSelector)
                .build();
    }
}
