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

import io.grpc.Context;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;


/**
 * Provide a mechanism for passing request metadata into gRPC service calls.
 * <br/>
 *
 * In the default configuration, TRAC will set request metadata for each incoming request.
 * Service extensions can override or add to request metadata using custom interceptors.
 */
public class RequestMetadata {

    public static final String UNKNOWN_REQUEST_ID = "#unknown_request_id";
    public static final OffsetDateTime UNKNOWN_REQUEST_TIMESTAMP = Instant.EPOCH.atOffset(ZoneOffset.UTC);

    private static final RequestMetadata REQUEST_METADATA_NOT_SET = new RequestMetadata(UNKNOWN_REQUEST_ID, UNKNOWN_REQUEST_TIMESTAMP);
    private static final Context.Key<RequestMetadata> REQUEST_METADATA_KEY = Context.keyWithDefault("trac-request-metadata", REQUEST_METADATA_NOT_SET);

    public static Context set(Context context, RequestMetadata requestMetadata) {

        return context.withValue(REQUEST_METADATA_KEY, requestMetadata);
    }

    public static RequestMetadata get(Context context) {

        return REQUEST_METADATA_KEY.get(context);
    }

    private final String requestId;
    private final OffsetDateTime requestTimestamp;

    public RequestMetadata(@Nonnull String requestId, @Nonnull OffsetDateTime requestTimestamp) {
        this.requestId = requestId;
        this.requestTimestamp = requestTimestamp;
    }

    public String requestId() {
        return requestId;
    }

    public OffsetDateTime requestTimestamp() {
        return requestTimestamp;
    }
}
