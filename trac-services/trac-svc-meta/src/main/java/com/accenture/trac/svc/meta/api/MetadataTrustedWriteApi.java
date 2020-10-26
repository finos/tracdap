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

import com.accenture.trac.common.api.meta.MetadataTrustedWriteApiGrpc;
import com.accenture.trac.common.api.meta.MetadataWriteRequest;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.TagHeader;
import com.accenture.trac.common.util.ApiWrapper;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;
import io.grpc.stub.StreamObserver;

import static com.accenture.trac.svc.meta.logic.MetadataConstants.TAG_FIRST_VERSION;
import static com.accenture.trac.svc.meta.logic.MetadataConstants.TRUSTED_API;


public class MetadataTrustedWriteApi extends MetadataTrustedWriteApiGrpc.MetadataTrustedWriteApiImplBase {

    private final ApiWrapper apiWrapper;
    private final MetadataWriteLogic writeLogic;

    public MetadataTrustedWriteApi(MetadataWriteLogic writeLogic) {
        this.apiWrapper = new ApiWrapper(getClass(), ApiErrorMapping.ERROR_MAPPING);
        this.writeLogic = writeLogic;
    }

    @Override
    public void createObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        apiWrapper.unaryCall(response, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var tag = request.getTag();

            var saveResult = writeLogic.saveNewObject(tenant, objectType, tag, TRUSTED_API);

            var idResponse = saveResult
                    .thenApply(objectId -> IdResponse.newBuilder()
                    .setObjectId(MetadataCodec.encode(objectId))
                    .setObjectVersion(1)
                    .setTagVersion(1)
                    .build());

            return idResponse;
        });
    }

    @Override
    public void updateObject(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var tag = request.getTag();

            var saveResult = writeLogic.saveNewVersion(tenant, objectType, tag, TRUSTED_API);

            var idResponse = saveResult
                    .thenApply(objectVersion -> IdResponse.newBuilder()
                    .setObjectId(tag.getDefinition().getHeader().getObjectId())
                    .setObjectVersion(objectVersion)
                    .setTagVersion(1)
                    .build());

            return idResponse;
        });
    }

    @Override
    public void updateTag(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var tag = request.getTag();

            var saveResult = writeLogic.saveNewTag(tenant, objectType, tag, TRUSTED_API);

            var idResponse = saveResult
                    .thenApply(tagVersion -> IdResponse.newBuilder()
                    .setObjectId(tag.getDefinition().getHeader().getObjectId())
                    .setObjectVersion(tag.getDefinition().getHeader().getObjectVersion())
                    .setTagVersion(tagVersion)
                    .build());

            return idResponse;
        });
    }

    @Override
    public void preallocateId(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();

            var saveResult = writeLogic.preallocateId(tenant, objectType);

            var idResponse = saveResult
                    .thenApply(header -> IdResponse.newBuilder()
                    .setObjectId(header.getObjectId())
                    .setObjectVersion(header.getObjectVersion())
                    .setTagVersion(TAG_FIRST_VERSION)
                    .build());

            return idResponse;
        });
    }

    @Override
    public void createPreallocatedObject(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var tag = request.getTag();

            var saveResult = writeLogic.savePreallocatedObject(tenant, objectType, tag);

            var idResponse = saveResult
                    .thenApply(header -> IdResponse.newBuilder()
                    .setObjectId(header.getObjectId())
                    .setObjectVersion(header.getObjectVersion())
                    .setTagVersion(TAG_FIRST_VERSION)
                    .build());

            return idResponse;
        });
    }
}
