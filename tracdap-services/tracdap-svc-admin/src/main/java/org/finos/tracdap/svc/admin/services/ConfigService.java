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

package org.finos.tracdap.svc.admin.services;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.dal.IMetadataDal;
import org.finos.tracdap.common.metadata.dal.MetadataBatchUpdate;
import org.finos.tracdap.common.metadata.tag.ObjectUpdateLogic;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.*;

import io.grpc.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;


public class ConfigService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Validator validator;
    private final IMetadataDal dal;

    public ConfigService(IMetadataDal dal) {
        this.validator = new Validator();
        this.dal = dal;
    }

    public ConfigEntry createConfigObject(ConfigWriteRequest request) {

        var result = createConfigObjects(request.getTenant(), List.of(request));
        return result.get(0);
    }

    public List<ConfigEntry> createConfigObjects(String tenant, List<ConfigWriteRequest> requests) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());

        // Look for deleted entries, the API allows creating if an existing entry is deleted
        // All requests in a batch have the same config class (this is enforced in validation)
        var configClass = requests.get(0).getConfigClass();
        var deletedEntries = dal.listConfigEntries(tenant, configClass, /* includeDeleted = */ true);

        // Build new metadata objects
        var objects = newObjects(requests, requestMetadata, userMetadata);
        var entries = newEntries(requests, objects, deletedEntries, requestMetadata);

        // Save to the DAL in a single batch
        var batch = new MetadataBatchUpdate(null, null, objects, null, null, entries);
        dal.saveBatchUpdate(tenant, batch);

        return entries;
    }

    public ConfigEntry updateConfigObject(ConfigWriteRequest request) {

        var result = updateConfigObjects(request.getTenant(), List.of(request));
        return result.get(0);
    }

    public List<ConfigEntry> updateConfigObjects(String tenant, List<ConfigWriteRequest> requests) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());

        // A prior entry / object should exist for every update request
        // Do not allow prior entries that are deleted, those can be re-created using createConfigObject
        var priorKeys = requests.stream().map(ConfigWriteRequest::getPriorVersion).collect(Collectors.toList());
        var priorEntries = dal.loadConfigEntries(tenant, priorKeys, /* includeDeleted = */ false);
        var priorSelectors = priorEntries.stream().map(entry -> entry.getDetails().getObjectSelector()).collect(Collectors.toList());
        var priorObjects = dal.loadObjects(tenant, priorSelectors);

        // Version semantics must apply to object definitions
        validateDefinitionUpdates(requests, priorObjects);

        // Build new metadata objects
        var objects = updateObjects(requests, priorObjects, requestMetadata, userMetadata);
        var entries = updateEntries(requests, objects, requestMetadata);

        // Save to the DAL in a single batch
        var batch = new MetadataBatchUpdate(null, null, null, objects, null, entries);
        dal.saveBatchUpdate(tenant, batch);

        return entries;
    }

    public ConfigEntry deleteConfigObject(ConfigWriteRequest request) {

        var result = deleteConfigObjects(request.getTenant(), List.of(request));
        return result.get(0);
    }

    public List<ConfigEntry> deleteConfigObjects(String tenant, List<ConfigWriteRequest> requests) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());

        // A prior entry / object should exist for every update request
        // Do not allow prior entries that are already deleted
        var priorKeys = requests.stream().map(ConfigWriteRequest::getPriorVersion).collect(Collectors.toList());
        var priorEntries = dal.loadConfigEntries(tenant, priorKeys, /* includeDeleted = */ false);
        var priorSelectors = priorEntries.stream().map(entry -> entry.getDetails().getObjectSelector()).collect(Collectors.toList());

        // Build new metadata objects with the deleted flag set
        var tags = deleteObjects(priorSelectors);
        var entries = deleteEntries(requests, requestMetadata);

        // Save to the DAL in a single batch
        var batch = new MetadataBatchUpdate(null, null, null, null, tags, entries);
        dal.saveBatchUpdate(tenant, batch);

        return entries;
    }

    public Tag readConfigObject(ConfigReadRequest request) {

        var entry = dal.loadConfigEntry(request.getTenant(), request.getEntry(), /* includeDeleted = */ false);
        var selector = entry.getDetails().getObjectSelector();

        return dal.loadObject(request.getTenant(), selector);
    }

    public ConfigReadBatchResponse readConfigBatch(ConfigReadBatchRequest request) {

        var entries = dal.loadConfigEntries(request.getTenant(), request.getEntriesList(), /* includeDeleted = */ false);
        var selectors = entries.stream().map(entry -> entry.getDetails().getObjectSelector()).collect(Collectors.toList());
        var tags = dal.loadObjects(request.getTenant(), selectors);

        return ConfigReadBatchResponse.newBuilder()
                .addAllEntries(tags)
                .build();
    }

    public ConfigListResponse listConfigKeys(ConfigListRequest request) {

        var entries = dal.listConfigEntries(
                request.getTenant(),
                request.getConfigClass(),
                request.getShowDeleted());

        return ConfigListResponse.newBuilder()
                .addAllEntries(entries)
                .build();
    }

    private List<Tag> newObjects(
            List<ConfigWriteRequest> requests,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var configAttrs = List.<TagUpdate>of();  // Allow config attrs to propagate from previous version

        var newObjects = new ArrayList<Tag>(requests.size());

        for (var request : requests) {

            // TODO: Centralize allocation of object IDs
            var objectId = UUID.randomUUID();

            var newObject = ObjectUpdateLogic.buildNewObject(
                    objectId, request.getDefinition(), configAttrs,
                    requestMetadata, userMetadata);

            newObjects.add(newObject);
        }

        return newObjects;
    }

    private List<ConfigEntry> newEntries(
            List<ConfigWriteRequest> requests, List<Tag> objects,
            List<ConfigEntry> deletedEntries, RequestMetadata requestMetadata) {

        var timestamp = MetadataCodec.encodeDatetime(requestMetadata.requestTimestamp());
        var entries = new ArrayList<ConfigEntry>(requests.size());

        var deletedMap = deletedEntries.stream()
                .filter(ConfigEntry::getConfigDeleted)
                .collect(Collectors.toMap(ConfigEntry::getConfigKey, ce -> ce));

        for (int i = 0; i < requests.size(); i++) {

            var request = requests.get(i);
            var object = objects.get(i);
            var deletedEntry = deletedMap.get(request.getConfigKey());

            var entry = newEntry(request, object, deletedEntry, timestamp);

            entries.add(entry);
        }

        return entries;

    }

    private ConfigEntry newEntry(
            ConfigWriteRequest request, Tag configObject,
            ConfigEntry deletedEntry, DatetimeValue timestamp) {

        var details = buildConfigDetails(configObject);

        if (deletedEntry != null) {

            return ConfigEntry.newBuilder()
                    .setConfigClass(request.getConfigClass())
                    .setConfigKey(request.getConfigKey())
                    .setConfigVersion(deletedEntry.getConfigVersion() + 1)
                    .setConfigTimestamp(timestamp)
                    .setIsLatestConfig(true)
                    .setConfigDeleted(false)
                    .setDetails(details)
                    .build();
        }
        else {

            return ConfigEntry.newBuilder()
                    .setConfigClass(request.getConfigClass())
                    .setConfigKey(request.getConfigKey())
                    .setConfigVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                    .setConfigTimestamp(timestamp)
                    .setIsLatestConfig(true)
                    .setDetails(details)
                    .build();
        }
    }

    private List<Tag> updateObjects(
            List<ConfigWriteRequest> requests, List<Tag> priorVersions,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var configAttrs = List.<TagUpdate>of();  // Allow config attrs to propagate from previous version
        var objects = new ArrayList<Tag>(requests.size());

        for (var i = 0; i < requests.size(); i++) {

            var request = requests.get(i);
            var priorVersion = priorVersions.get(i);

            var object = ObjectUpdateLogic.buildNewVersion(
                    priorVersion, request.getDefinition(), configAttrs,
                    requestMetadata, userMetadata);

            objects.add(object);
        }

        return objects;
    }

    private List<ConfigEntry> updateEntries(
            List<ConfigWriteRequest> requests, List<Tag> objects,
            RequestMetadata requestMetadata) {

        var timestamp = MetadataCodec.encodeDatetime(requestMetadata.requestTimestamp());
        var entries = new ArrayList<ConfigEntry>(requests.size());

        for (int i = 0; i < requests.size(); i++) {

            var request = requests.get(i);
            var object = objects.get(i);

            var priorEntry = request.getPriorVersion();
            var entry = updateEntry(priorEntry, object, timestamp);

            entries.add(entry);
        }

        return entries;
    }

    private ConfigEntry updateEntry(ConfigEntry priorEntry, Tag configObject, DatetimeValue timestamp) {

        var details = buildConfigDetails(configObject);

        return priorEntry.toBuilder()
                .setConfigVersion(priorEntry.getConfigVersion() + 1)
                .setConfigTimestamp(timestamp)
                .setIsLatestConfig(true)
                .setDetails(details)
                .build();
    }

    private List<Tag> deleteObjects(List<TagSelector> priorVersions) {

        // TODO: Deleting objects is not supported yet
        // Config objects will be left as orphans in the database

        return List.of();
    }

    private List<ConfigEntry> deleteEntries(List<ConfigWriteRequest> requests, RequestMetadata requestMetadata) {

        var timestamp = MetadataCodec.encodeDatetime(requestMetadata.requestTimestamp());
        var entries = new ArrayList<ConfigEntry>(requests.size());

        for (var request : requests) {

            var priorEntry = request.getPriorVersion();
            var entry = deleteEntry(priorEntry, timestamp);

            entries.add(entry);
        }

        return entries;
    }

    private ConfigEntry deleteEntry(ConfigEntry priorEntry, DatetimeValue timestamp) {

        return priorEntry.toBuilder()
                .setConfigVersion(priorEntry.getConfigVersion() + 1)
                .setConfigTimestamp(timestamp)
                .setIsLatestConfig(true)
                .setConfigDeleted(true)
                .clearDetails()
                .build();
    }

    private ConfigDetails buildConfigDetails(Tag configObject) {

        var selector = MetadataUtil.selectorFor(configObject.getHeader());

        var builder =  ConfigDetails.newBuilder()
                .setObjectSelector(selector)
                .setObjectType(configObject.getDefinition().getObjectType());

        return builder.build();
    }

    private void validateDefinitionUpdates(List<ConfigWriteRequest> requests, List<Tag> priorVersions) {

        if (requests.size() != priorVersions.size())
            throw new EUnexpected();

        // TODO: Apply the version validator in bulk across a batch of updates

        for (int i = 0; i < requests.size(); i++) {

            var currentDefinition = requests.get(i).getDefinition();
            var priorDefinition = priorVersions.get(i).getDefinition();

            validator.validateVersion(currentDefinition, priorDefinition);
        }
    }
}
