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

package org.finos.tracdap.common.metadata.dal.jdbc;

import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.db.JdbcDialect;
import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.metadata.dal.IMetadataDal;

import org.finos.tracdap.common.metadata.dal.MetadataBatchUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.UUID;
import java.util.stream.Collectors;


public class JdbcMetadataDal extends JdbcBaseDal implements IMetadataDal {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataSource dataSource;

    private final JdbcTenantImpl tenants;
    private final JdbcReadImpl readSingle;
    private final JdbcReadBatchImpl readBatch;
    private final JdbcWriteBatchImpl writeBatch;
    private final JdbcSearchImpl search;


    public JdbcMetadataDal(JdbcDialect dialect, DataSource dataSource) {

        super(dataSource, dialect);

        this.dataSource = dataSource;

        tenants = new JdbcTenantImpl();
        readSingle = new JdbcReadImpl();
        readBatch = new JdbcReadBatchImpl(this.dialect);
        writeBatch = new JdbcWriteBatchImpl(this.dialect, readBatch);
        search = new JdbcSearchImpl();
    }

    @Override
    public void start() {

        try {
            // Synchronous database call, avoid futures / callbacks during the startup sequence!
            executeDirect(tenants::loadTenantMap);
        }
        catch (SQLException e) {

            var message = "Error connecting to metadata database: " + e.getMessage();
            log.error(message, e);

            throw new EStartup(message, e);
        }
    }

    @Override
    public void stop() {

        JdbcSetup.destroyDatasource(dataSource);
    }

    @Override
    public List<TenantInfo> listTenants() {

        return wrapTransaction(tenants::listTenants);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // SAVE METHODS
    // -----------------------------------------------------------------------------------------------------------------

    // Assume that save methods are not highly sensitive to latency
    // So, use the same implementation for save one and save many
    // I.e. no special optimisation for saving a single item, even though this is the common case

    @Override
    public void saveBatchUpdate(String tenant, MetadataBatchUpdate batchUpdate) {

        wrapTransaction(conn -> {

            prepareMappingTable(conn);

            if (batchUpdate.getPreallocatedIds() != null && !batchUpdate.getPreallocatedIds().isEmpty())
                savePreallocatedIds(conn, tenant, batchUpdate.getPreallocatedIds());

            if (batchUpdate.getPreallocatedObjects() != null && !batchUpdate.getPreallocatedObjects().isEmpty())
                savePreallocatedObjects(conn, tenant, batchUpdate.getPreallocatedObjects());

            if (batchUpdate.getNewObjects() != null && !batchUpdate.getNewObjects().isEmpty())
                saveNewObjects(conn, tenant, batchUpdate.getNewObjects());

            if (batchUpdate.getNewVersions() != null && !batchUpdate.getNewVersions().isEmpty())
                saveNewVersions(conn, tenant, batchUpdate.getNewVersions());

            if (batchUpdate.getNewTags() != null && !batchUpdate.getNewTags().isEmpty())
                saveNewTags(conn, tenant, batchUpdate.getNewTags());

            if (batchUpdate.getConfigEntries() != null && !batchUpdate.getConfigEntries().isEmpty())
                saveConfigEntries(conn, tenant, batchUpdate.getConfigEntries());
        });
    }

    @Override
    public void savePreallocatedIds(String tenant, List<TagHeader> objectIds) {

        wrapTransaction(conn -> {
            prepareMappingTable(conn);
            savePreallocatedIds(conn, tenant, objectIds);
        });
    }

    @Override
    public void savePreallocatedObjects(String tenant, List<Tag> tags) {

        wrapTransaction(conn -> {
            prepareMappingTable(conn);
            savePreallocatedObjects(conn, tenant, tags);
        });
    }

    @Override
    public void saveNewObjects(String tenant, List<Tag> tags) {

        wrapTransaction(conn -> {
            prepareMappingTable(conn);
            saveNewObjects(conn, tenant, tags);
        });
    }

    @Override
    public void saveNewVersions(String tenant, List<Tag> tags) {

        wrapTransaction(conn -> {
            prepareMappingTable(conn);
            saveNewVersions(conn, tenant, tags);
        });
    }

    @Override
    public void saveNewTags(String tenant, List<Tag> tags) {

        wrapTransaction(conn -> {
            prepareMappingTable(conn);
            saveNewTags(conn, tenant, tags);
        });
    }

    private void savePreallocatedIds(Connection conn, String tenant, List<TagHeader> objectIds) {

        var parts = separateIdParts(objectIds);

        try {

            var tenantId = tenants.getTenantId(conn, tenant);

            writeBatch.writeObjectId(conn, tenantId, parts);
        }
        catch (SQLException error) {

            JdbcError.duplicateObjectId(error, dialect, parts);

            throw JdbcError.catchAll(error, dialect);
        }
    }

    private void savePreallocatedObjects(Connection conn, String tenant, List<Tag> tags) {

        var parts = separateParts(tags);

        try {

            var tenantId = tenants.getTenantId(conn, tenant);

            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
            checkObjectTypes(parts, objectType);

            long[] defPk = writeBatch.writeObjectDefinition(conn, tenantId, objectType.keys, parts);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);
        }
        catch (SQLException error) {

            JdbcError.idNotPreallocated(error, dialect, parts);
            JdbcError.idAlreadyInUse(error, dialect, parts);
            JdbcError.wrongObjectType(error, dialect, parts);

            throw JdbcError.catchAll(error, dialect);
        }
    }

    private void saveNewObjects(Connection conn, String tenant, List<Tag> tags) {

        var parts = separateParts(tags);

        try {

            var tenantId = tenants.getTenantId(conn, tenant);

            long[] objectPk = writeBatch.writeObjectId(conn, tenantId, parts);
            long[] defPk = writeBatch.writeObjectDefinition(conn, tenantId, objectPk, parts);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);
        }
        catch (SQLException error) {

            JdbcError.duplicateObjectId(error, dialect, parts);

            throw JdbcError.catchAll(error, dialect);
        }
    }

    private void saveNewVersions(Connection conn, String tenant, List<Tag> tags) {

        var parts = separateParts(tags);

        try {

            var tenantId = tenants.getTenantId(conn, tenant);

            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
            checkObjectTypes(parts, objectType);

            writeBatch.closeObjectDefinition(conn, tenantId, objectType.keys, parts);

            long[] defPk = writeBatch.writeObjectDefinition(conn, tenantId, objectType.keys, parts);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);
        }
        catch (SQLException error) {

            JdbcError.priorVersionMissing(error, dialect, parts);
            JdbcError.versionSuperseded(error, dialect, parts);
            JdbcError.wrongObjectType(error, dialect, parts);

            throw JdbcError.catchAll(error, dialect);
        }
    }

    private void saveNewTags(Connection conn, String tenant, List<Tag> tags) {

        var parts = separateParts(tags);

        try {

            var tenantId = tenants.getTenantId(conn, tenant);

            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
            checkObjectTypes(parts, objectType);

            long[] defPk = readBatch.lookupDefinitionPk(conn, tenantId, objectType.keys, parts.objectVersion);
            writeBatch.closeTagRecord(conn, tenantId, defPk, parts);

            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);
        }
        catch (SQLException error) {

            JdbcError.priorTagMissing(error, dialect, parts);
            JdbcError.tagSuperseded(error, dialect, parts);
            JdbcError.wrongObjectType(error, dialect, parts);

            throw JdbcError.catchAll(error, dialect);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // LOAD METHODS
    // -----------------------------------------------------------------------------------------------------------------

    // Loading a single object is highly sensitive to latency
    // This is because UI applications use metadata queries to decide what to display
    // These queries may also be recursive, i.e. query a job, then related data/models or dependent jobs
    // We want these multiple queries to complete in < 100 ms for a fluid user experience
    // So, optimising the common case of loading a single item makes sense

    @Override
    public Tag loadObject(String tenant, TagSelector selector) {

        // Single item reads don't use the mapping table, so prepareMappingTable() is not needed

        return wrapTransaction(conn -> {
            return loadObject(conn, tenant, selector);
        });
    }

    private Tag loadObject(Connection conn, String tenant, TagSelector selector) {

        var parts = selectorParts(selector);

        try {

            var tenantId = tenants.getTenantId(conn, tenant);

            var objectType = readSingle.readObjectTypeById(conn, tenantId, parts.objectId[0]);
            checkObjectType(parts, objectType);

            var definition = readSingle.readDefinition(conn, tenantId, objectType.key, parts.selector[0]);
            var tagRecord = readSingle.readTagRecord(conn, tenantId, definition.key, parts.selector[0]);
            var tagAttrs = readSingle.readTagAttrs(conn, tenantId, tagRecord.key);

            return buildTag(objectType.item, parts.objectId[0], definition, tagRecord, tagAttrs);
        }
        catch (SQLException error) {

            JdbcError.objectNotFound(error, dialect, parts, false, false);
            JdbcError.wrongObjectType(error, dialect, parts);

            throw JdbcError.catchAll(error, dialect);
        }
    }


    // Batch loading may be used e.g. to query all items related to a job in a single query
    // This can be used both by the platform (e.g. to set up a job) and applications / UI (e.g. to display a job)
    // Latency remains important, optimisations are in ReadBatchImpl

    @Override
    public List<Tag> loadObjects(String tenant, List<TagSelector> selectors) {

        return wrapTransaction(conn -> {
            prepareMappingTable(conn);
            return loadObjects(conn, tenant, selectors, false, false);
        });
    }

    @Override
    public List<Tag> loadPriorObjects(String tenant, List<TagSelector> selectors) {

        return wrapTransaction(conn -> {
            prepareMappingTable(conn);
            return loadObjects(conn, tenant, selectors, true, false);
        });
    }

    @Override
    public List<Tag> loadPriorTags(String tenant, List<TagSelector> selectors) {

        return wrapTransaction(conn -> {
            prepareMappingTable(conn);
            return loadObjects(conn, tenant, selectors, false, true);
        });
    }

    private List<Tag> loadObjects(
            Connection conn, String tenant, List<TagSelector> selectors,
            boolean priorVersions, boolean priorTags) {

        var parts = selectorParts(selectors);

        try {

            var tenantId = tenants.getTenantId(conn, tenant);

            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
            checkObjectTypes(parts, objectType);

            var definition = readBatch.readDefinition(conn, tenantId, objectType.keys, parts.selector);
            var tag = readBatch.readTag(conn, tenantId, definition.keys, parts.selector);

            return buildTags(objectType.items, parts.objectId, definition, tag);
        }
        catch (SQLException error) {

            JdbcError.objectNotFound(error, dialect, parts, priorVersions, priorTags);
            JdbcError.wrongObjectType(error, dialect, parts);

            throw JdbcError.catchAll(error, dialect);
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // SEARCH METHODS
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public List<Tag> search(String tenant, SearchParameters searchParameters) {

        return wrapTransaction(conn -> {
            prepareMappingTable(conn);
            return search(conn, tenant, searchParameters);
        });
    }

    private List<Tag> search(Connection conn, String tenant, SearchParameters searchParameters) {

        try {

            var tenantId = tenants.getTenantId(conn, tenant);

            long[] tagPk = search.search(conn, tenantId, searchParameters);
            var tag = readBatch.readTagWithHeader(conn, tenantId, tagPk);

            return Arrays.stream(tag.items)
                    .map(Tag.Builder::build)
                    .collect(Collectors.toList());
        }
        catch (SQLException error) {

            throw JdbcError.catchAll(error, dialect);
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // CONFIG ENTRIES
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public void saveConfigEntries(String tenant, List<ConfigEntry> configEntries) {

        wrapTransaction(conn -> {
            prepareMappingTable(conn);
            saveConfigEntries(conn, tenant, configEntries);
        });
    }

    @Override
    public ConfigEntry loadConfigEntry(String tenant, ConfigEntry configKey, boolean includeDeleted) {

        return wrapTransaction(conn -> {
            return loadConfigEntry(conn, tenant, configKey, includeDeleted);
        });
    }

    @Override
    public List<ConfigEntry> loadConfigEntries(String tenant, List<ConfigEntry> configKeys, boolean includeDeleted) {

        return wrapTransaction(conn -> {
            prepareMappingTable(conn);
            return loadConfigEntries(conn, tenant, configKeys, includeDeleted);
        });
    }

    @Override
    public List<ConfigEntry> listConfigEntries(String tenant, String configClass, boolean includeDeleted) {

        return wrapTransaction(conn -> {
            prepareMappingTable(conn);
            return listConfigEntries(conn, tenant, configClass, includeDeleted);
        });
    }

    private void saveConfigEntries(Connection conn, String tenant, List<ConfigEntry> configEntries) {

        var parts = configParts(configEntries);

        try {

            var tenantId = tenants.getTenantId(conn, tenant);

            // Find prior versions for entries with version > 1
            var priorVersions = configPriorVersions(parts.configEntry);

            // Close any prior version entries if they exist
            if (priorVersions.length > 0) {
                long[] priorPk = readBatch.lookupConfigPkByVersion(conn, tenantId, priorVersions, /* includeDeleted = */ true);
                writeBatch.closeConfigEntry(conn, tenantId, priorPk, parts);
            }

            writeBatch.writeConfigEntry(conn, tenantId, parts);
        }
        catch (SQLException error) {

            JdbcError.priorConfigMissing(error, dialect, parts);
            JdbcError.duplicateConfig(error, dialect, parts);

            throw JdbcError.catchAll(error, dialect);
        }
    }

    private List<ConfigEntry> loadConfigEntries(Connection conn, String tenant, List<ConfigEntry> configKeys, boolean includeDeleted) {

        var parts = configParts(configKeys);

        try {

            var tenantId = tenants.getTenantId(conn, tenant);
            var configEntries = readBatch.readConfigEntry(conn, tenantId, parts.configEntry, parts.configTimestamp, includeDeleted);

            return buildConfigEntry(configKeys, configEntries);
        }
        catch (SQLException error) {

            JdbcError.configNotFound(error, dialect, parts);
            throw JdbcError.catchAll(error, dialect);
        }
    }

    private ConfigEntry loadConfigEntry(Connection conn, String tenant, ConfigEntry configKey, boolean includeDeleted) {

        var parts = configParts(configKey);

        try {

            var tenantId = tenants.getTenantId(conn, tenant);
            var configEntry = readSingle.readConfigEntry(conn, tenantId, parts.configEntry[0], parts.configTimestamp[0], includeDeleted);

            return buildConfigEntry(configKey, configEntry);
        }
        catch (SQLException error) {

            JdbcError.configNotFound(error, dialect, parts);
            throw JdbcError.catchAll(error, dialect);
        }
    }

    private List<ConfigEntry> listConfigEntries(Connection conn, String tenant, String configClass, boolean includeDeleted) {

        try {

            var tenantId = tenants.getTenantId(conn, tenant);

            long[] configPk = search.searchConfigKeys(conn, tenantId, configClass, includeDeleted);

            if (configPk.length == 0)
                throw new JdbcException(JdbcErrorCode.NO_DATA);

            var configStub = readBatch.readConfigStub(conn, tenantId, configPk);

            return buildConfigStub(configClass, configStub);

        }
        catch (SQLException error) {

            JdbcError.configClassNotFound(error, dialect, configClass);
            throw JdbcError.catchAll(error, dialect);
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // OBJECT PARTS
    // -----------------------------------------------------------------------------------------------------------------

    // Break down requests / results into a standard set of parts

    static class ObjectParts {

        ObjectType[] objectType;
        UUID[] objectId;
        int[] objectVersion;
        int[] tagVersion;
        Instant[] objectTimestamp;
        Instant[] tagTimestamp;

        Tag[] tag;
        ObjectDefinition[] definition;

        TagSelector[] selector;

        ConfigEntry[] configEntry;
        Instant[] configTimestamp;
    }

    private ObjectParts separateParts(List<Tag> tags) {

        var headers = tags.stream().map(Tag::getHeader).toArray(TagHeader[]::new);

        var parts = new ObjectParts();
        parts.objectType = Arrays.stream(headers).map(TagHeader::getObjectType).toArray(ObjectType[]::new);
        parts.objectId = Arrays.stream(headers).map(TagHeader::getObjectId).map(UUID::fromString).toArray(UUID[]::new);
        parts.objectVersion = Arrays.stream(headers).mapToInt(TagHeader::getObjectVersion).toArray();
        parts.tagVersion = Arrays.stream(headers).mapToInt(TagHeader::getTagVersion).toArray();

        parts.objectTimestamp = Arrays.stream(headers)
                .map(TagHeader::getObjectTimestamp)
                .map(MetadataCodec::decodeDatetime)
                .map(OffsetDateTime::toInstant)
                .toArray(Instant[]::new);

        parts.tagTimestamp = Arrays.stream(headers)
                .map(TagHeader::getTagTimestamp)
                .map(MetadataCodec::decodeDatetime)
                .map(OffsetDateTime::toInstant)
                .toArray(Instant[]::new);

        parts.tag = tags.toArray(Tag[]::new);
        parts.definition = tags.stream().map(Tag::getDefinition).toArray(ObjectDefinition[]::new);

        return parts;
    }

    private ObjectParts separateIdParts(List<TagHeader> objectIds) {

        var parts = new ObjectParts();
        parts.objectType = objectIds.stream().map(TagHeader::getObjectType).toArray(ObjectType[]::new);
        parts.objectId = objectIds.stream().map(TagHeader::getObjectId).map(UUID::fromString).toArray(UUID[]::new);

        return parts;
    }

    private ObjectParts selectorParts(TagSelector selector) {

        var parts = new ObjectParts();
        parts.objectType = new ObjectType[] {selector.getObjectType()};
        parts.objectId = new UUID[] {UUID.fromString(selector.getObjectId())};
        parts.selector = new TagSelector[] {selector};

        return parts;
    }

    private ObjectParts selectorParts(List<TagSelector> selector) {

        var parts = new ObjectParts();

        parts.objectType = selector.stream()
                .map(TagSelector::getObjectType)
                .toArray(ObjectType[]::new);

        parts.objectId = selector.stream()
                .map(TagSelector::getObjectId)
                .map(UUID::fromString)
                .toArray(UUID[]::new);

        parts.selector = selector.toArray(TagSelector[]::new);

        return parts;
    }

    private ObjectParts configParts(ConfigEntry configEntry) {

        var parts = new ObjectParts();

        parts.configEntry = new ConfigEntry[] {configEntry};

        var isoDatetime = configEntry.getConfigTimestamp();

        if (isoDatetime.getIsoDatetime().isBlank())
            parts.configTimestamp = new Instant[] {null};
        else
            parts.configTimestamp = new Instant[] {MetadataCodec.decodeDatetime(isoDatetime).toInstant()};

        return parts;
    }

    private ObjectParts configParts(List<ConfigEntry> configEntry) {

        var parts = new ObjectParts();

        parts.configEntry = configEntry.toArray(ConfigEntry[]::new);

        parts.configTimestamp = configEntry.stream()
                .map(ConfigEntry::getConfigTimestamp)
                .map(dtv -> dtv.getIsoDatetime().isBlank() ? null : MetadataCodec.decodeDatetime(dtv).toInstant())
                .toArray(Instant[]::new);

        return parts;
    }

    private ConfigEntry[] configPriorVersions(ConfigEntry[] configEntries) {

        return Arrays.stream(configEntries)
                .filter(entry -> entry.getConfigVersion() > 1)
                .map(ConfigEntry::toBuilder)
                .map(entry -> entry.setConfigVersion(entry.getConfigVersion() - 1))
                .map(ConfigEntry.Builder::clearConfigTimestamp)
                .map(ConfigEntry.Builder::clearIsLatestConfig)
                .map(ConfigEntry.Builder::clearConfigDeleted)
                .map(ConfigEntry.Builder::clearDetails)
                .map(ConfigEntry.Builder::build)
                .toArray(ConfigEntry[]::new);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // CHECK OBJECT TYPES
    // -----------------------------------------------------------------------------------------------------------------

    private void checkObjectTypes(ObjectParts parts, KeyedItems<ObjectType> existingTypes) throws JdbcException {

        for (int i = 0; i < parts.objectType.length; i++)
            if (parts.objectType[i] != existingTypes.items[i])
                throw new JdbcException(JdbcErrorCode.WRONG_OBJECT_TYPE);
    }

    private void checkObjectType(ObjectParts parts, KeyedItem<ObjectType> existingType) throws JdbcException {

        if (parts.objectType[0] != existingType.item)
            throw new JdbcException(JdbcErrorCode.WRONG_OBJECT_TYPE);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // BUILD TAGS
    // -----------------------------------------------------------------------------------------------------------------

    private Tag buildTag(
            ObjectType objectType, UUID objectId,
            KeyedItem<ObjectDefinition> definition,
            KeyedItem<Void> tagRecord,
            Map<String, Value> tagAttrs) {

        var objectTimestamp = definition.timestamp.atOffset(ZoneOffset.UTC);
        var tagTimestamp = tagRecord.timestamp.atOffset(ZoneOffset.UTC);

        var header = TagHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(definition.version)
                .setTagVersion(tagRecord.version)
                .setIsLatestTag(tagRecord.isLatest)
                .setIsLatestObject(definition.isLatest)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(objectTimestamp))
                .setTagTimestamp(MetadataCodec.encodeDatetime(tagTimestamp));

        return Tag.newBuilder()
                .setHeader(header)
                .setDefinition(definition.item)
                .putAllAttrs(tagAttrs)
                .build();
    }

    private List<Tag> buildTags(
            ObjectType[] objectType, UUID[] objectId,
            KeyedItems<ObjectDefinition> definitions,
            KeyedItems<Tag.Builder> tags) {

        var result = new ArrayList<Tag>(objectId.length);

        for (int i = 0; i < objectId.length; i++) {

            var objectTimestamp = definitions.timestamps[i].atOffset(ZoneOffset.UTC);
            var tagTimestamp = tags.timestamps[i].atOffset(ZoneOffset.UTC);

            var header = TagHeader.newBuilder()
                    .setObjectType(objectType[i])
                    .setObjectId(objectId[i].toString())
                    .setObjectVersion(definitions.versions[i])
                    .setTagVersion(tags.versions[i])
                    .setIsLatestTag(tags.isLatest[i])
                    .setIsLatestObject(definitions.isLatest[i])
                    .setObjectTimestamp(MetadataCodec.encodeDatetime(objectTimestamp))
                    .setTagTimestamp(MetadataCodec.encodeDatetime(tagTimestamp));

            var tag = tags.items[i]
                    .setHeader(header)
                    .setDefinition(definitions.items[i])
                    .build();

            result.add(tag);
        }

        return result;
    }

    private ConfigEntry buildConfigEntry(ConfigEntry entry, KeyedItem<ConfigDetails> keyedDetails) {

        return ConfigEntry.newBuilder()
                .setConfigClass(entry.getConfigClass())
                .setConfigKey(entry.getConfigKey())
                .setConfigVersion(keyedDetails.version)
                .setConfigTimestamp(MetadataCodec.encodeDatetime(keyedDetails.timestamp))
                .setIsLatestConfig(keyedDetails.isLatest)
                .setConfigDeleted(keyedDetails.deleted)
                .setDetails(keyedDetails.item)
                .build();
    }

    private List<ConfigEntry> buildConfigEntry(List<ConfigEntry> entries, KeyedItems<ConfigDetails> keyedDetails) {

        var result = new ArrayList<ConfigEntry>(keyedDetails.keys.length);

        for (int i = 0; i < keyedDetails.keys.length; i++) {

            var version = keyedDetails.versions[i];
            var timestamp = keyedDetails.timestamps[i];
            var latest = keyedDetails.isLatest[i];
            var deleted = keyedDetails.deleted[i];
            var details = keyedDetails.items[i];

            var entry = ConfigEntry.newBuilder()
                    .setConfigClass(entries.get(i).getConfigClass())
                    .setConfigKey(entries.get(i).getConfigKey())
                    .setConfigVersion(version)
                    .setConfigTimestamp(MetadataCodec.encodeDatetime(timestamp))
                    .setIsLatestConfig(latest)
                    .setConfigDeleted(deleted)
                    .setDetails(details)
                    .build();

            result.add(entry);
        }

        return result;
    }

    private List<ConfigEntry> buildConfigStub(String configClass, KeyedItems<String> keyedStubs) {

        var result = new ArrayList<ConfigEntry>(keyedStubs.keys.length);

        for (int i = 0; i < keyedStubs.keys.length; i++) {

            var version = keyedStubs.versions[i];
            var timestamp = keyedStubs.timestamps[i];
            var latest = keyedStubs.isLatest[i];
            var deleted = keyedStubs.deleted[i];
            var configKey = keyedStubs.items[i];

            var entry = ConfigEntry.newBuilder()
                    .setConfigClass(configClass)
                    .setConfigKey(configKey)
                    .setConfigVersion(version)
                    .setConfigTimestamp(MetadataCodec.encodeDatetime(timestamp))
                    .setIsLatestConfig(latest)
                    .setConfigDeleted(deleted)
                    .build();

            result.add(entry);
        }

        return result;
    }

}
