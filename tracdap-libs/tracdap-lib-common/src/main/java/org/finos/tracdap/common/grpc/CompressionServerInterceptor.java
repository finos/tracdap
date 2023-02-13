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


public class CompressionServerInterceptor implements ServerInterceptor  {

    public static final String COMPRESSION_TYPE = "gzip";
    public static final int COMPRESSION_THRESHOLD = 65336;

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT>
    interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        call.setCompression(COMPRESSION_TYPE);

        var compressionCall = new CompressionServerCall<>(call);

        return next.startCall(compressionCall, headers);
    }

    private static class CompressionServerCall<ReqT, RespT> extends ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> {

        public CompressionServerCall(ServerCall<ReqT, RespT> delegate) {
            super(delegate);
        }

        @Override
        public void sendMessage(RespT message) {

            var msg = (MessageLite) message;

            if (msg != null)
                delegate().setMessageCompression(msg.getSerializedSize() > COMPRESSION_THRESHOLD);

            delegate().sendMessage(message);
        }
    }
}
