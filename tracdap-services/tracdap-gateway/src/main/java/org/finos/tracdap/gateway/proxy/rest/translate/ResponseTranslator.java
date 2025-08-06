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

package org.finos.tracdap.gateway.proxy.rest.translate;

import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.gateway.proxy.grpc.GrpcUtils;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.function.Supplier;


public class ResponseTranslator {

    private final Supplier<Message.Builder> responseFactory;
    private final IResponseTranslator<Message> bodyTranslator;

    public ResponseTranslator(
            Supplier<Message.Builder> responseFactory,
            IResponseTranslator<Message> bodyTranslator) {

        this.responseFactory = responseFactory;
        this.bodyTranslator = bodyTranslator;
    }

    public Message decodeLpm(ByteBuf bodyBuffer) {

        var builder = responseFactory.get();

        try {
            return GrpcUtils.decodeLpm(builder, bodyBuffer);
        }
        catch (InvalidProtocolBufferException e) {
            // Bad response from back end service
            throw new ETracInternal("Invalid gRPC response: Message could not be decoded", e);
        }

    }

    public ByteBuf translateResponse(Message msg) {

        return bodyTranslator.translate(msg);
    }


    public HttpResponseStatus translateGrpcErrorCode(Status.Code grpcStatusCode) {

        switch (grpcStatusCode) {

            case OK:
                return HttpResponseStatus.OK;

            case UNAUTHENTICATED:
                return HttpResponseStatus.UNAUTHORIZED;

            case PERMISSION_DENIED:
                return HttpResponseStatus.FORBIDDEN;

            case INVALID_ARGUMENT:
                return HttpResponseStatus.BAD_REQUEST;

            case NOT_FOUND:
                return HttpResponseStatus.NOT_FOUND;

            case ALREADY_EXISTS:
                return HttpResponseStatus.CONFLICT;

            case FAILED_PRECONDITION:
                return HttpResponseStatus.PRECONDITION_FAILED;

            case UNAVAILABLE:
                return HttpResponseStatus.SERVICE_UNAVAILABLE;

            default:
                // For unrecognised errors, send error code 500 with no message
                return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }
    }
}
