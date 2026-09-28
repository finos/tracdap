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

import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.store.IMetadataStore;
import org.finos.tracdap.common.metadata.store.PlatformConfigRecord;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.common.service.ConfigWrite;
import org.finos.tracdap.common.service.IConfigReader;
import org.finos.tracdap.common.service.IConfigWriteHook;
import org.finos.tracdap.common.validation.ValidationConstants;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.metadata.PlatformConfigEntry;
import org.finos.tracdap.metadata.Value;

import io.grpc.Context;
import com.google.protobuf.InvalidProtocolBufferException;

import java.text.MessageFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class PlatformConfigService {

    private static final String PRIOR_ENTRY_SUPERSEDED = "Prior platform config entry [{0}] [{1}] has been superseded (expected version {2}, found version {3})";

    private final IMetadataStore metadataStore;
    private final PluginRegistry registry;
    private final IConfigReader configReader;
    private final ConfigKeyGenerator keyGenerator;
    private final Validator validator;

    public PlatformConfigService(IMetadataStore metadataStore, PluginRegistry registry, IConfigReader configReader) {
        this(metadataStore, registry, configReader, new ConfigKeyGenerator());
    }

    PlatformConfigService(
            IMetadataStore metadataStore, PluginRegistry registry,
            IConfigReader configReader, ConfigKeyGenerator keyGenerator) {

        this.metadataStore = metadataStore;
        this.registry = registry;
        this.configReader = configReader;
        this.keyGenerator = keyGenerator;
        this.validator = new Validator();
    }

    public PlatformConfigWriteResponse createPlatformConfigObject(PlatformConfigWriteRequest request) {

        var timestamp = RequestMetadata.get(Context.current()).requestTimestamp().toInstant();
        var user = UserMetadata.get(Context.current());

        var configClass = request.getConfigClass();
        var definition = request.getDefinition();

        var configKey = ValidationConstants.GENERATED_KEY_OBJECT_TYPES.contains(definition.getObjectType())
                ? keyGenerator.generateKey(key -> isKeyFree(configClass, key))
                : request.getConfigKey();

        var hook = registry.trySingleton(IConfigWriteHook.class);

        if (hook != null) {

            var write = ConfigWrite.forPlatform(
                    ConfigUpdateType.CREATE, configClass, configKey,
                    null, null, definition, user, configReader);

            definition = hook.transform(write);
            hook.validate(write, definition);
        }

        var entry = metadataStore.createPlatformConfigEntry(
                configClass, configKey, timestamp, user, definition.toByteArray());

        return PlatformConfigWriteResponse.newBuilder().setEntry(entry).build();
    }

    public PlatformConfigWriteResponse updatePlatformConfigObject(PlatformConfigWriteRequest request) {

        var timestamp = RequestMetadata.get(Context.current()).requestTimestamp().toInstant();
        var user = UserMetadata.get(Context.current());

        var priorEntry = request.getPriorEntry();
        var prior = loadPrior(priorEntry);
        var priorDefinition = parseDefinition(prior.value());
        var definition = request.getDefinition();

        var hook = registry.trySingleton(IConfigWriteHook.class);
        ConfigWrite write = null;

        if (hook != null) {

            write = ConfigWrite.forPlatform(
                    ConfigUpdateType.UPDATE, priorEntry.getConfigClass(), priorEntry.getConfigKey(),
                    prior.entry(), priorDefinition, definition, user, configReader);

            definition = hook.transform(write);
        }

        validator.validateVersion(definition, priorDefinition);

        if (hook != null)
            hook.validate(write, definition);

        var entry = metadataStore.updatePlatformConfigEntry(priorEntry, timestamp, user, definition.toByteArray());

        return PlatformConfigWriteResponse.newBuilder().setEntry(entry).build();
    }

    public PlatformConfigWriteResponse deletePlatformConfigObject(PlatformConfigWriteRequest request) {

        var timestamp = RequestMetadata.get(Context.current()).requestTimestamp().toInstant();
        var user = UserMetadata.get(Context.current());

        var priorEntry = request.getPriorEntry();
        var hook = registry.trySingleton(IConfigWriteHook.class);

        if (hook != null) {

            var prior = loadPrior(priorEntry);

            var write = ConfigWrite.forPlatform(
                    ConfigUpdateType.DELETE, priorEntry.getConfigClass(), priorEntry.getConfigKey(),
                    prior.entry(), parseDefinition(prior.value()), null, user, configReader);

            hook.validate(write, null);
        }

        var entry = metadataStore.deletePlatformConfigEntry(priorEntry, timestamp, user);

        return PlatformConfigWriteResponse.newBuilder().setEntry(entry).build();
    }

    public PlatformConfigReadResponse readPlatformConfigEntry(PlatformConfigReadRequest request) {

        var key = request.getEntry();

        var record = metadataStore.loadPlatformConfigEntry(
                key.getConfigClass(), key.getConfigKey(), /* includeDeleted = */ false);

        return buildReadResponse(record);
    }

    public PlatformConfigReadBatchResponse readPlatformConfigBatch(PlatformConfigReadBatchRequest request) {

        var records = metadataStore.loadPlatformConfigEntries(
                request.getEntriesList(), /* includeDeleted = */ false);

        var entries = records.stream()
                .map(PlatformConfigService::buildReadResponse)
                .collect(Collectors.toList());

        return PlatformConfigReadBatchResponse.newBuilder().addAllEntries(entries).build();
    }

    public PlatformConfigListResponse listPlatformConfigEntries(PlatformConfigListRequest request) {

        var entries = metadataStore.listPlatformConfigEntries(
                request.getConfigClass(), request.getIncludeDeleted());

        return PlatformConfigListResponse.newBuilder().addAllEntries(entries).build();
    }

    private PlatformConfigRecord loadPrior(PlatformConfigEntry priorEntry) {

        var prior = metadataStore.loadPlatformConfigEntry(
                priorEntry.getConfigClass(), priorEntry.getConfigKey(), /* includeDeleted = */ false);

        if (prior.entry().getConfigVersion() != priorEntry.getConfigVersion()) {

            var message = MessageFormat.format(
                    PRIOR_ENTRY_SUPERSEDED, priorEntry.getConfigClass(), priorEntry.getConfigKey(),
                    priorEntry.getConfigVersion(), prior.entry().getConfigVersion());

            throw new EMetadataDuplicate(message);
        }

        return prior;
    }

    private boolean isKeyFree(String configClass, String configKey) {

        try {
            metadataStore.loadPlatformConfigEntry(configClass, configKey, /* includeDeleted = */ true);
            return false;
        }
        catch (EMetadataNotFound e) {
            return true;
        }
    }

    static PlatformConfigReadResponse buildReadResponse(PlatformConfigRecord record) {

        return PlatformConfigReadResponse.newBuilder()
                .setEntry(record.entry())
                .setDefinition(parseDefinition(record.value()))
                .putAllAttrs(buildAttrs(record))
                .build();
    }

    private static Map<String, Value> buildAttrs(PlatformConfigRecord record) {

        var entry = record.entry();
        var attrs = new HashMap<String, Value>();

        attrs.put(MetadataConstants.TRAC_CONFIG_CLASS, MetadataCodec.encodeValue(entry.getConfigClass()));
        attrs.put(MetadataConstants.TRAC_CONFIG_KEY, MetadataCodec.encodeValue(entry.getConfigKey()));

        if (record.createUserId() != null) {
            var createTime = OffsetDateTime.ofInstant(record.createTime(), ZoneOffset.UTC);
            attrs.put(MetadataConstants.TRAC_CREATE_TIME, MetadataCodec.encodeValue(createTime));
            attrs.put(MetadataConstants.TRAC_CREATE_USER_ID, MetadataCodec.encodeValue(record.createUserId()));
            attrs.put(MetadataConstants.TRAC_CREATE_USER_NAME, MetadataCodec.encodeValue(record.createUserName()));
        }

        if (record.updateUserId() != null) {
            var updateTime = MetadataCodec.decodeDatetime(entry.getConfigTimestamp());
            attrs.put(MetadataConstants.TRAC_UPDATE_TIME, MetadataCodec.encodeValue(updateTime));
            attrs.put(MetadataConstants.TRAC_UPDATE_USER_ID, MetadataCodec.encodeValue(record.updateUserId()));
            attrs.put(MetadataConstants.TRAC_UPDATE_USER_NAME, MetadataCodec.encodeValue(record.updateUserName()));
        }

        return attrs;
    }

    private static ObjectDefinition parseDefinition(byte[] value) {

        try {
            return ObjectDefinition.parseFrom(value);
        }
        catch (InvalidProtocolBufferException e) {

            // Platform config values are always written by this service as serialized ObjectDefinition bytes
            // A parse failure here means the stored bytes have been corrupted, which is an internal error
            throw new EUnexpected();
        }
    }
}
