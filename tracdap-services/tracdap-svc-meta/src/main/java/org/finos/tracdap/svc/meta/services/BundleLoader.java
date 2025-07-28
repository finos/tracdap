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

package org.finos.tracdap.svc.meta.services;

import org.finos.tracdap.api.MetadataWriteBatchRequest;
import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.store.IMetadataStore;
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.metadata.TagSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class BundleLoader {

    private final IMetadataStore metadataStore;
    private final Logger log = LoggerFactory.getLogger(getClass());

    public BundleLoader(IMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    public MetadataBundle loadReferenceBundle(String tenant, MetadataWriteRequest request) {

        var references = new ArrayList<TagSelector>();
        findReferences(request, references);

        var mapping = new HashMap<String, TagHeader>();
        var objects = new HashMap<String, ObjectDefinition>();

        loadReferences(tenant, references, mapping, objects);

        return new MetadataBundle(mapping, objects, Map.of());
    }

    public MetadataBundle loadReferenceBundle(
            String tenant,
            MetadataWriteBatchRequest batchWriteRequest,
            RequestMetadata requestMetadata) {

        var references = new ArrayList<TagSelector>();
        findReferences(batchWriteRequest.getCreatePreallocatedObjectsList(), references);
        findReferences(batchWriteRequest.getCreateObjectsList(), references);
        findReferences(batchWriteRequest.getUpdateObjectsList(), references);

        var mapping = new HashMap<String, TagHeader>();
        var objects = new HashMap<String, ObjectDefinition>();

        resolveInternalBatchReferences(references, batchWriteRequest, requestMetadata, mapping, objects);
        loadReferences(tenant, references, mapping, objects);

        return new MetadataBundle(mapping, objects, Map.of());
    }

    private void findReferences(List<MetadataWriteRequest> requests, List<TagSelector> references) {

        requests.forEach(request -> findReferences(request, references));
    }

    private void findReferences(MetadataWriteRequest request, List<TagSelector> references) {

        // TODO: Record references explicitly on object definition to avoid logic per object type
        // In particular references should be held for JobDefinition, which vary by job type

        if (request.getObjectType() == ObjectType.DATA) {

            if (request.getDefinition().getData().hasSchemaId())
                references.add(request.getDefinition().getData().getSchemaId());

            references.add(request.getDefinition().getData().getStorageId());
        }

        else if (request.getObjectType() == ObjectType.FILE) {

            references.add(request.getDefinition().getFile().getStorageId());
        }
    }

    private void loadReferences(
            String tenant, List<TagSelector> references,
            Map<String, TagHeader> mapping,
            Map<String, ObjectDefinition> objects) {

        try {

            var tags = metadataStore.loadObjects(tenant, references);

            for (int i = 0; i < references.size(); i++) {

                var ref = references.get(i);
                var tag = tags.get(i);

                if (!ref.hasObjectVersion()) {
                    var refKey = MetadataUtil.objectKey(ref);
                    mapping.put(refKey, tag.getHeader());
                }

                var objectKey = MetadataUtil.objectKey(tag.getHeader());

                objects.put(objectKey, tag.getDefinition());
            }
        }
        catch (EMetadataNotFound error) {

            log.error("Inconsistent metadata: One or more object references are missing", error);
            throw error;
        }
    }

    private void resolveInternalBatchReferences(
            List<TagSelector> references,
            MetadataWriteBatchRequest batchWriteRequest,
            RequestMetadata requestMetadata,
            Map<String, TagHeader> mapping,
            Map<String, ObjectDefinition> objects) {

        // For write requests, some references can refer to objects updated as part of a batch
        // These need to be resolved from the batch, the older stored versions have been superseded

        // To be referenced in a batch, the ID must be known a-prior
        // Items in createObjects have no pre-assigned ID, only preallocated and updated objects can be used

        var referenceMapping = new HashMap<String, MetadataWriteRequest>();
        buildReferenceMapping(batchWriteRequest.getCreatePreallocatedObjectsList(), referenceMapping);
        buildReferenceMapping(batchWriteRequest.getUpdateObjectsList(), referenceMapping);

        var batchTimestamp = MetadataCodec.encodeDatetime(requestMetadata.requestTimestamp());
        var iter = references.iterator();

        while (iter.hasNext()) {

            var ref = iter.next();

            if (!referenceMapping.containsKey(ref.getObjectId()))
                continue;

            var request = referenceMapping.get(ref.getObjectId());

            if (ref.hasLatestObject() || ref.getObjectVersion() == request.getPriorVersion().getObjectVersion() + 1) {

                // To ensure the bundle is consistent, recreate the new ID that will be assigned on write
                var newId = TagHeader.newBuilder()
                        .setObjectType(request.getPriorVersion().getObjectType())
                        .setObjectId(request.getPriorVersion().getObjectId())
                        .setObjectVersion(request.getPriorVersion().getObjectVersion() + 1)
                        .setObjectTimestamp(batchTimestamp)
                        .setIsLatestObject(true)
                        .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                        .setTagTimestamp(batchTimestamp)
                        .setIsLatestTag(true)
                        .build();

                if (ref.hasLatestObject()) {
                    var refKey = MetadataUtil.objectKey(ref);
                    mapping.put(refKey, newId);
                }

                objects.put(MetadataUtil.objectKey(newId), request.getDefinition());
                iter.remove();
            }
        }
    }

    private void buildReferenceMapping(List<MetadataWriteRequest> requests, Map<String, MetadataWriteRequest> referenceMapping) {

        for (var request : requests) {
            if (request.hasPriorVersion()) {
                var objectId = request.getPriorVersion().getObjectId();
                referenceMapping.put(objectId, request);
            }
        }
    }
}
