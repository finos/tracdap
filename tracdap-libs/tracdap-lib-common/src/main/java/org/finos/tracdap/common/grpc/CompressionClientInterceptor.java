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

package org.finos.tracdap.common.grpc;

import com.google.protobuf.MessageLite;
import io.grpc.*;


public class CompressionClientInterceptor implements ClientInterceptor {

    public static final String COMPRESSION_TYPE = "gzip";
    public static final int COMPRESSION_THRESHOLD = 65336;

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT>
    interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

        var nextCall = next.newCall(method, callOptions);

        return new CompressionClientCall<>(nextCall);
    }

    private static class CompressionClientCall<ReqT, RespT> extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {

        public CompressionClientCall(ClientCall<ReqT, RespT> delegate) {
            super(delegate);
        }

        @Override
        public void sendMessage(ReqT message) {

            var msg = (MessageLite) message;

            if (msg.getSerializedSize() > COMPRESSION_THRESHOLD)
                delegate().setMessageCompression(true);

            delegate().sendMessage(message);
        }
    }
}
