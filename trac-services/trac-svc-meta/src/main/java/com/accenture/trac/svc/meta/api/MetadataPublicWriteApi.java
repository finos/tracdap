/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.meta.api;

import com.accenture.trac.common.api.meta.MetadataPublicWriteApiGrpc;
import com.accenture.trac.common.api.meta.MetadataWriteRequest;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.common.metadata.TagHeader;
import com.accenture.trac.common.util.ApiWrapper;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.accenture.trac.svc.meta.logic.MetadataConstants.PUBLIC_API;


public class MetadataPublicWriteApi extends MetadataPublicWriteApiGrpc.MetadataPublicWriteApiImplBase {

    // Only a limited set of object types can be created directly by clients
    // Everything else can only be created by the trusted API, i.e. by other TRAC platform components
    public static final List<ObjectType> PUBLIC_TYPES = Arrays.asList(
            ObjectType.FLOW,
            ObjectType.CUSTOM);

    private final ApiWrapper apiWrapper;
    private final MetadataWriteLogic writeLogic;

    public MetadataPublicWriteApi(MetadataWriteLogic writeLogic) {
        this.apiWrapper = new ApiWrapper(getClass(), ApiErrorMapping.ERROR_MAPPING);
        this.writeLogic = writeLogic;
    }

    @Override
    public void createObject(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();

            if (!PUBLIC_TYPES.contains(objectType)) {
                var message = String.format("Object type %s cannot be created via the TRAC public API", objectType);
                var status = Status.PERMISSION_DENIED.withDescription(message);
                return CompletableFuture.failedFuture(status.asRuntimeException());
            }

            return writeLogic.createObject(tenant, objectType,
                    request.getDefinition(),
                    request.getAttrMap(),
                    PUBLIC_API);
        });
    }

    @Override
    public void updateObject(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();

            if (!PUBLIC_TYPES.contains(objectType)) {
                var message = String.format("Object type %s cannot be created via the TRAC public API", objectType);
                var status = Status.PERMISSION_DENIED.withDescription(message);
                return CompletableFuture.failedFuture(status.asRuntimeException());
            }

            return writeLogic.updateObject(tenant, objectType,
                    request.getPriorVersion(),
                    request.getDefinition(),
                    request.getAttrMap(),
                    PUBLIC_API);
        });
    }

    @Override
    public void updateTag(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            return writeLogic.updateTag(
                    request.getTenant(),
                    request.getObjectType(),
                    request.getPriorVersion(),
                    request.getAttrMap(),
                    PUBLIC_API);
        });
    }
}
