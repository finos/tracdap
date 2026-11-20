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

import io.grpc.Context;
import org.finos.tracdap.api.MetadataWriteBatchRequest;
import org.finos.tracdap.api.MetadataWriteBatchResponse;
import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.UuidFactory;
import org.finos.tracdap.common.metadata.store.IMetadataStore;
import org.finos.tracdap.common.metadata.store.MetadataBatchUpdate;
import org.finos.tracdap.common.metadata.tag.ObjectUpdateLogic;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.common.service.IConfigReader;
import org.finos.tracdap.common.service.IMetadataWriteHook;
import org.finos.tracdap.common.service.ObjectWrite;
import org.finos.tracdap.common.service.TagWrite;
import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.exception.EMetadataBadUpdate;
import org.finos.tracdap.common.exception.EVersionValidation;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


public class MetadataWriteService {

    // A write to all versions rewrites the full tag of every version in one transaction
    static final int ALL_VERSIONS_LIMIT = 1000;

    private final Validator validator = new Validator();
    private final IMetadataStore metadataStore;
    private final PluginRegistry registry;
    private final IConfigReader configReader;
    private final BundleLoader bundleLoader;
    private final UuidFactory objectIdFactory;

    public MetadataWriteService(IMetadataStore metadataStore, PluginRegistry registry, IConfigReader configReader) {
        this.metadataStore = metadataStore;
        this.registry = registry;
        this.configReader = configReader;
        this.bundleLoader = new BundleLoader(metadataStore);
        this.objectIdFactory = new UuidFactory();
    }

    public TagHeader preallocateId(String tenant, MetadataWriteRequest request) {

        var newIds = processPreallocatedIds(List.of(request));

        metadataStore.savePreallocatedIds(tenant, newIds);

        return newIds.get(0);
    }

    public TagHeader createPreallocatedObject(String tenant, MetadataWriteRequest request) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());
        var references = bundleLoader.loadReferenceBundle(tenant, request);

        var preallocatedObjects = processPreallocatedObjects(tenant, List.of(request), false, references, requestMetadata, userMetadata);

        metadataStore.savePreallocatedObjects(tenant, preallocatedObjects);

        return preallocatedObjects.get(0).getHeader();
    }

    public TagHeader createObject(String tenant, MetadataWriteRequest request) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());
        var references = bundleLoader.loadReferenceBundle(tenant, request);

        var newObjects = processNewObjects(tenant, List.of(request), false, references, requestMetadata, userMetadata);

        metadataStore.saveNewObjects(tenant, newObjects);

        return newObjects.get(0).getHeader();
    }

    public TagHeader updateObject(String tenant, MetadataWriteRequest request) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());
        var references = bundleLoader.loadReferenceBundle(tenant, request);

        var newVersions = processNewVersions(tenant, List.of(request), false, references, requestMetadata, userMetadata);

        metadataStore.saveNewVersions(tenant, newVersions);

        return newVersions.get(0).getHeader();
    }

    public TagHeader updateTag(String tenant, MetadataWriteRequest request) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());

        if (request.getVersionScope() == VersionScope.VERSION_SCOPE_ALL_VERSIONS) {

            var allVersions = processAllVersions(tenant, request, requestMetadata, userMetadata);

            metadataStore.saveNewTags(tenant, allVersions);

            // Versions are saved in ascending order, the request names the latest version
            return allVersions.get(allVersions.size() - 1).getHeader();
        }

        var newTags = processNewTags(tenant, List.of(request), false, requestMetadata, userMetadata);

        metadataStore.saveNewTags(tenant, newTags);

        return newTags.get(0).getHeader();
    }

    public MetadataWriteBatchResponse writeBatch(MetadataWriteBatchRequest request) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());

        var tenant = request.getTenant();
        var references = bundleLoader.loadReferenceBundle(tenant, request, requestMetadata);

        var preallocatedIds = processPreallocatedIds(request.getPreallocateIdsList());
        var preallocatedObjects = processPreallocatedObjects(tenant, request.getCreatePreallocatedObjectsList(), true, references, requestMetadata, userMetadata);
        var newObjects = processNewObjects(tenant, request.getCreateObjectsList(), true, references, requestMetadata, userMetadata);
        var newVersions = processNewVersions(tenant, request.getUpdateObjectsList(), true, references, requestMetadata, userMetadata);
        var newTags = processNewTags(tenant, request.getUpdateTagsList(), true, requestMetadata, userMetadata);

        var batchUpdate = new MetadataBatchUpdate(
                preallocatedIds, preallocatedObjects,
                newObjects, newVersions, newTags);

        metadataStore.saveBatchUpdate(request.getTenant(), batchUpdate);

        var preallocatedObjectIds = preallocatedObjects.stream().map(Tag::getHeader).collect(Collectors.toList());
        var newObjectIds = newObjects.stream().map(Tag::getHeader).collect(Collectors.toList());
        var newVersionIds = newVersions.stream().map(Tag::getHeader).collect(Collectors.toList());
        var newTagIds = newTags.stream().map(Tag::getHeader).collect(Collectors.toList());

        return MetadataWriteBatchResponse.newBuilder()
                .addAllPreallocateIds(preallocatedIds)
                .addAllCreatePreallocatedObjects(preallocatedObjectIds)
                .addAllCreateObjects(newObjectIds)
                .addAllUpdateObjects(newVersionIds)
                .addAllUpdateTags(newTagIds)
                .build();
    }

    private List<TagHeader> processPreallocatedIds(List<MetadataWriteRequest> requests) {

        var preallocatedIds = new ArrayList<TagHeader>(requests.size());

        for (var request : requests) {

            var objectId = objectIdFactory.allocate();

            var preallocatedId = TagHeader.newBuilder()
                    .setObjectType(request.getObjectType())
                    .setObjectId(objectId.toString())
                    .build();

            preallocatedIds.add(preallocatedId);
        }

        return preallocatedIds;
    }

    private List<Tag> processPreallocatedObjects(
            String tenant, List<MetadataWriteRequest> requests, boolean inBatch, MetadataBundle references,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var preallocatedObjects = new ArrayList<Tag>(requests.size());
        var hook = registry.trySingleton(IMetadataWriteHook.class);

        for (var request : requests) {

            if (hook != null)
                hook.validateObject(objectWrite(
                        tenant, ObjectWrite.Operation.CREATE_PREALLOCATED, inBatch,
                        request, null, userMetadata));

            var objectId = UUID.fromString(request.getPriorVersion().getObjectId());

            var preallocatedObject = ObjectUpdateLogic.buildNewObject(
                    objectId, request.getDefinition(), request.getTagUpdatesList(),
                    references, requestMetadata, userMetadata);

            preallocatedObjects.add(preallocatedObject);
        }

        return preallocatedObjects;
    }

    private List<Tag> processNewObjects(
            String tenant, List<MetadataWriteRequest> requests, boolean inBatch, MetadataBundle references,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var newObjects = new ArrayList<Tag>(requests.size());
        var hook = registry.trySingleton(IMetadataWriteHook.class);

        for (var request : requests) {

            if (hook != null)
                hook.validateObject(objectWrite(
                        tenant, ObjectWrite.Operation.CREATE, inBatch,
                        request, null, userMetadata));

            var objectId = objectIdFactory.allocate();

            var newObject = ObjectUpdateLogic.buildNewObject(
                    objectId, request.getDefinition(), request.getTagUpdatesList(),
                    references, requestMetadata, userMetadata);

            newObjects.add(newObject);
        }

        return newObjects;
    }

    private List<Tag> processNewVersions(
            String tenant, List<MetadataWriteRequest> requests, boolean inBatch, MetadataBundle references,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        // Do not query the DAL if there are no requests
        if (requests.isEmpty())
            return List.of();

        var priorIds = requests.stream().map(MetadataWriteRequest::getPriorVersion).collect(Collectors.toList());
        var priorVersions = metadataStore.loadPriorObjects(tenant, priorIds);
        var latestPriorTags = loadLatestPriorTags(tenant, priorVersions);

        var newVersions = new ArrayList<Tag>(requests.size());
        var hook = registry.trySingleton(IMetadataWriteHook.class);

        for (var i = 0; i < requests.size(); i++) {

            var request = requests.get(i);
            var priorVersion = priorVersions.get(i);

            // TODO: Apply the version validator in bulk across a batch of updates
            // Will need an update in the validator, currently 50 object updates -> 50 separate validation passes

            if (!priorVersion.getHeader().getIsLatestObject()) {

                var err = String.format(
                        "Cannot create a new version from a superseded object: prior version [%d] is not the latest version of [%s]",
                        priorVersion.getHeader().getObjectVersion(),
                        priorVersion.getHeader().getObjectId());

                throw new EVersionValidation(err);
            }

            validator.validateVersion(
                    request.getDefinition(),
                    priorVersion.getDefinition());

            if (hook != null)
                hook.validateObject(objectWrite(
                        tenant, ObjectWrite.Operation.UPDATE, inBatch,
                        request, priorVersion.getDefinition(), userMetadata));

            var latestPriorTag = latestPriorTags.get(i);

            var newObject = ObjectUpdateLogic.buildNewVersion(
                    priorVersion, latestPriorTag, request.getDefinition(), request.getTagUpdatesList(),
                    references, requestMetadata, userMetadata);

            newVersions.add(newObject);
        }

        return newVersions;
    }

    // Controlled attributes for all versions come from the latest tag of the prior version
    // That tag is only loaded when the request names an older tag, the result holds null for every other request

    private Map<Integer, Tag> loadLatestPriorTags(String tenant, List<Tag> priorVersions) {

        var indexes = new ArrayList<Integer>();
        var selectors = new ArrayList<TagSelector>();

        for (var i = 0; i < priorVersions.size(); i++) {

            var priorHeader = priorVersions.get(i).getHeader();

            if (!priorHeader.getIsLatestTag()) {
                indexes.add(i);
                selectors.add(MetadataUtil.selectorFor(priorHeader, false, true));
            }
        }

        var latestPriorTags = new HashMap<Integer, Tag>();

        if (selectors.isEmpty())
            return latestPriorTags;

        var latestTags = metadataStore.loadPriorTags(tenant, selectors);

        for (var i = 0; i < indexes.size(); i++)
            latestPriorTags.put(indexes.get(i), latestTags.get(i));

        return latestPriorTags;
    }

    private ObjectWrite objectWrite(
            String tenant, ObjectWrite.Operation operation, boolean inBatch,
            MetadataWriteRequest request, ObjectDefinition priorDefinition,
            UserMetadata userMetadata) {

        return new ObjectWrite(
                tenant, request.getObjectType(), operation, inBatch,
                priorDefinition, request.getDefinition(), userMetadata);
    }

    private List<Tag> processNewTags(
            String tenant, List<MetadataWriteRequest> requests, boolean inBatch,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        // Do not query the DAL if there are no requests
        if (requests.isEmpty())
            return List.of();

        var priorIds = requests.stream().map(MetadataWriteRequest::getPriorVersion).collect(Collectors.toList());
        var priorTags = metadataStore.loadPriorTags(tenant, priorIds);

        var newTags = new ArrayList<Tag>(requests.size());
        var hook = registry.trySingleton(IMetadataWriteHook.class);

        for (var i = 0; i < requests.size(); i++) {

            var request = requests.get(i);
            var priorTag = priorTags.get(i);

            if (hook != null)
                hook.validateTag(tagWrite(tenant, request, priorTag, 0, inBatch, userMetadata));

            var newTag = request.getControl() == TagControl.CONTROLLED
                    ? ObjectUpdateLogic.buildNewControlledTag(
                            priorTag, request.getTagUpdates(0), request.getVersionScope(), request.getReason(),
                            /* requireValue = */ true, requestMetadata, userMetadata)
                    : ObjectUpdateLogic.buildNewTag(
                            priorTag, request.getTagUpdatesList(),
                            requestMetadata, userMetadata);

            newTags.add(newTag);
        }

        return newTags;
    }

    private List<Tag> processAllVersions(
            String tenant, MetadataWriteRequest request,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var priorTag = metadataStore.loadPriorTags(tenant, List.of(request.getPriorVersion())).get(0);
        var priorHeader = priorTag.getHeader();

        if (!priorHeader.getIsLatestObject()) {

            var err = String.format(
                    "Cannot write a tag to all versions from a superseded object: prior version [%d] is not the latest version of [%s]",
                    priorHeader.getObjectVersion(), priorHeader.getObjectId());

            throw new EVersionValidation(err);
        }

        var latestVersion = priorHeader.getObjectVersion();

        if (latestVersion > ALL_VERSIONS_LIMIT) {

            var err = String.format(
                    "A tag write to all versions is limited to objects with at most %d versions: [%s] has %d versions",
                    ALL_VERSIONS_LIMIT, priorHeader.getObjectId(), latestVersion);

            throw new EInputValidation(err);
        }

        // Older versions are loaded at their latest tag, every tag is saved as the next tag version of the one loaded,
        // so a tag written on any version in the meantime makes the save fail as stale
        // A new version saved in the meantime is not part of this write, it keeps the value it copied from its prior
        // version and the next write to all versions converges it
        var priorTags = new ArrayList<Tag>(latestVersion);

        if (latestVersion > MetadataConstants.OBJECT_FIRST_VERSION) {

            var olderVersions = new ArrayList<TagSelector>(latestVersion - 1);

            for (var version = MetadataConstants.OBJECT_FIRST_VERSION; version < latestVersion; version++) {
                olderVersions.add(TagSelector.newBuilder()
                        .setObjectType(priorHeader.getObjectType())
                        .setObjectId(priorHeader.getObjectId())
                        .setObjectVersion(version)
                        .setLatestTag(true)
                        .build());
            }

            priorTags.addAll(metadataStore.loadObjects(tenant, olderVersions));
        }

        priorTags.add(priorTag);

        var hook = registry.trySingleton(IMetadataWriteHook.class);

        if (hook != null)
            hook.validateTag(tagWrite(tenant, request, priorTag, latestVersion, false, userMetadata));

        var update = request.getTagUpdates(0);
        var storedName = ObjectUpdateLogic.controlledAttrName(update.getAttrName(), request.getVersionScope());

        // A clear is refused only when no version holds the value, the refusal names the latest version
        // Otherwise every version converges on the clear, versions without the value get the lineage
        var isClear = update.getOperation() == TagOperation.DELETE_ATTR;
        var nothingToClear = isClear && priorTags.stream().noneMatch(tag -> tag.containsAttrs(storedName));

        var newTags = new ArrayList<Tag>(priorTags.size());

        for (var tag : priorTags) {

            var requireValue = nothingToClear && tag == priorTag;

            try {

                var newTag = ObjectUpdateLogic.buildNewControlledTag(
                        tag, update, request.getVersionScope(), request.getReason(),
                        requireValue, requestMetadata, userMetadata);

                newTags.add(newTag);
            }
            catch (EMetadataBadUpdate e) {

                var err = String.format("%s, on object version [%d]", e.getMessage(), tag.getHeader().getObjectVersion());
                throw new EMetadataBadUpdate(err, e);
            }
        }

        return newTags;
    }

    private TagWrite tagWrite(
            String tenant, MetadataWriteRequest request, Tag priorTag,
            int latestObjectVersion, boolean inBatch, UserMetadata userMetadata) {

        return new TagWrite(
                tenant, request.getObjectType(), priorTag,
                request.getControl(), request.getVersionScope(), request.getTagUpdatesList(),
                latestObjectVersion, inBatch, userMetadata, configReader);
    }
}
