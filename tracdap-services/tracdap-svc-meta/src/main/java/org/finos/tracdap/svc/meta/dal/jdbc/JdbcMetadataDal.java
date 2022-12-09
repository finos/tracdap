/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.meta.dal.jdbc;

import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.db.JdbcDialect;
import org.finos.tracdap.svc.meta.dal.IMetadataDal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.UUID;
import java.util.stream.Collectors;


public class JdbcMetadataDal extends JdbcBaseDal implements IMetadataDal {

    private final Logger log;

    private final JdbcTenantImpl tenants;
    private final JdbcReadImpl readSingle;
    private final JdbcReadBatchImpl readBatch;
    private final JdbcWriteBatchImpl writeBatch;
    private final JdbcSearchImpl search;


    public JdbcMetadataDal(JdbcDialect dialect, DataSource dataSource) {

        super(dialect, dataSource);

        log = LoggerFactory.getLogger(getClass());

        tenants = new JdbcTenantImpl();
        readSingle = new JdbcReadImpl();
        readBatch = new JdbcReadBatchImpl(this.dialect);
        writeBatch = new JdbcWriteBatchImpl(this.dialect, readBatch);
        search = new JdbcSearchImpl();
    }

    public void startup() {

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

    public void shutdown() {

        // Noop
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
    public void saveNewObjects(String tenant, List<Tag> tags) {

        var parts = separateParts(tags);
        saveNewObjects(tenant, parts);
    }

    private void saveNewObjects(String tenant, ObjectParts parts) {

        wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(conn, tenant);

            long[] objectPk = writeBatch.writeObjectId(conn, tenantId, parts);
            long[] defPk = writeBatch.writeObjectDefinition(conn, tenantId, objectPk, parts);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);
        },
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts));
    }

    @Override
    public void saveNewVersions(String tenant, List<Tag> tags) {

        var parts = separateParts(tags);
        saveNewVersions(tenant, parts);
    }

    private void saveNewVersions(String tenant, ObjectParts parts) {

        wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(conn, tenant);
            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
            checkObjectTypes(parts, objectType);

            writeBatch.closeObjectDefinition(conn, tenantId, objectType.keys, parts);
            long[] defPk = writeBatch.writeObjectDefinition(conn, tenantId, objectType.keys, parts);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);
        },
        (error, code) -> JdbcError.handleMissingItem(error, code, parts),
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts),
        (error, code) ->  JdbcError.newVersion_WrongType(error, code, parts));
    }

    @Override
    public void saveNewTags(String tenant, List<Tag> tags) {

        var parts = separateParts(tags);
        saveNewTags(tenant, parts);
    }

    private void saveNewTags(String tenant, ObjectParts parts) {

        wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(conn, tenant);
            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
            checkObjectTypes(parts, objectType);

            long[] defPk = readBatch.lookupDefinitionPk(conn, tenantId, objectType.keys, parts.objectVersion);
            writeBatch.closeTagRecord(conn, tenantId, defPk, parts);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);
        },
        (error, code) -> JdbcError.handleMissingItem(error, code, parts),
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts),
        (error, code) ->  JdbcError.newTag_WrongType(error, code, parts));
    }

    @Override
    public void preallocateObjectId(String tenant, ObjectType objectType, UUID objectId) {

        var parts = separateParts(objectType, objectId);
        preallocateObjectIds(tenant, parts);
    }

    @Override
    public void preallocateObjectIds(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds) {

        var parts = separateParts(objectTypes, objectIds);
        preallocateObjectIds(tenant, parts);
    }

    private void preallocateObjectIds(String tenant, ObjectParts parts) {

        wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(conn, tenant);

            writeBatch.writeObjectId(conn, tenantId, parts);
        },
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts));
    }

    @Override
    public void savePreallocatedObject(String tenant, Tag tag) {

        var parts = separateParts(tag);
        savePreallocatedObjects(tenant, parts);
    }

    @Override
    public void savePreallocatedObjects(String tenant, List<Tag> tags) {

        var parts = separateParts(tags);
        savePreallocatedObjects(tenant, parts);
    }

    private void savePreallocatedObjects(String tenant, ObjectParts parts) {

        wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(conn, tenant);
            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
            checkObjectTypes(parts, objectType);

            long[] defPk = writeBatch.writeObjectDefinition(conn, tenantId, objectType.keys, parts);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);
        },
        (error, code) -> JdbcError.handleMissingItem(error, code, parts),   // TODO: different errors
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts),  // TODO: different errors
        (error, code) ->  JdbcError.savePreallocated_WrongType(error, code, parts));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // LOAD METHODS
    // -----------------------------------------------------------------------------------------------------------------

    // Loading a single object is highly sensitive to latency
    // This is because UI applications use metadata queries to decide what to display
    // These queries may also be recursive, i.e. query a job, then related data/models or dependent jobs
    // We want these multiple queries to complete in < 100 ms for a fluid user experience
    // So, optimising the common case of loading a single item makes sense

    @Override public Tag
    loadObject(String tenant, TagSelector selector) {

        var parts = selectorParts(selector);

        return wrapTransaction(conn -> {

            var tenantId = tenants.getTenantId(conn, tenant);
            var objectType = readSingle.readObjectTypeById(conn, tenantId, parts.objectId[0]);

            checkObjectType(parts, objectType);

            var definition = readSingle.readDefinition(conn, tenantId, objectType.key, parts.selector[0]);
            var tagRecord = readSingle.readTagRecord(conn, tenantId, definition.key, parts.selector[0]);
            var tagAttrs = readSingle.readTagAttrs(conn, tenantId, tagRecord.key);

            return buildTag(objectType.item, parts.objectId[0], definition, tagRecord, tagAttrs);
        },
        (error, code) -> JdbcError.loadOne_missingItem(error, code, selector),
        (error, code) -> JdbcError.loadOne_WrongObjectType(error, code, selector));
    }


    // Batch loading may be used e.g. to query all items related to a job in a single query
    // This can be used both by the platform (e.g. to set up a job) and applications / UI (e.g. to display a job)
    // Latency remains important, optimisations are in ReadBatchImpl

    @Override public List<Tag>
    loadObjects(String tenant, List<TagSelector> selectors) {

        var parts = selectorParts(selectors);

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(conn, tenant);
            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);

            checkObjectTypes(parts, objectType);

            var definition = readBatch.readDefinition(conn, tenantId, objectType.keys, parts.selector);
            var tag = readBatch.readTag(conn, tenantId, definition.keys, parts.selector);

            return buildTags(objectType.items, parts.objectId, definition, tag);
        },
        (error, code) -> JdbcError.loadBatch_missingItem(error, code, selectors),
        (error, code) -> JdbcError.loadBatch_WrongObjectType(error, code, selectors));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // LOAD METHODS (LEGACY)
    // -----------------------------------------------------------------------------------------------------------------

    @Override public Tag
    loadTag(String tenant, ObjectType objectType, UUID objectId, int objectVersion, int tagVersion) {

        var selector = TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(objectVersion)
                .setTagVersion(tagVersion)
                .build();

        return loadObject(tenant, selector);
    }

    @Override public Tag
    loadLatestTag(String tenant, ObjectType objectType, UUID objectId, int objectVersion) {

        var selector = TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(objectVersion)
                .setLatestTag(true)
                .build();

        return loadObject(tenant, selector);
    }

    @Override public Tag
    loadLatestVersion(String tenant, ObjectType objectType, UUID objectId) {

        var selector = TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        return loadObject(tenant, selector);
    }

    @Override public List<Tag>
    loadTags(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds, List<Integer> objectVersions, List<Integer> tagVersions) {

        var selectors = new ArrayList<TagSelector>(objectIds.size());

        for (int i = 0; i < objectIds.size(); i++) {

            var selector = TagSelector.newBuilder()
                    .setObjectType(objectTypes.get(i))
                    .setObjectId(objectIds.get(i).toString())
                    .setObjectVersion(objectVersions.get(i))
                    .setTagVersion(tagVersions.get(i))
                    .build();

            selectors.add(selector);
        }

        return loadObjects(tenant, selectors);
    }

    @Override public List<Tag>
    loadLatestTags(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds, List<Integer> objectVersions) {

        var selectors = new ArrayList<TagSelector>(objectIds.size());

        for (int i = 0; i < objectIds.size(); i++) {

            var selector = TagSelector.newBuilder()
                    .setObjectType(objectTypes.get(i))
                    .setObjectId(objectIds.get(i).toString())
                    .setObjectVersion(objectVersions.get(i))
                    .setLatestTag(true)
                    .build();

            selectors.add(selector);
        }

        return loadObjects(tenant, selectors);
    }

    @Override public List<Tag>
    loadLatestVersions(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds) {

        var selectors = new ArrayList<TagSelector>(objectIds.size());

        for (int i = 0; i < objectIds.size(); i++) {

            var selector = TagSelector.newBuilder()
                    .setObjectType(objectTypes.get(i))
                    .setObjectId(objectIds.get(i).toString())
                    .setLatestObject(true)
                    .setLatestTag(true)
                    .build();

            selectors.add(selector);
        }

        return loadObjects(tenant, selectors);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // SEARCH METHODS
    // -----------------------------------------------------------------------------------------------------------------

    @Override public List<Tag>
    search(String tenant, SearchParameters searchParameters) {

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(conn, tenant);
            long[] tagPk = search.search(conn, tenantId, searchParameters);

            var tag = readBatch.readTagWithHeader(conn, tenantId, tagPk);

            return Arrays.stream(tag.items)
                    .map(Tag.Builder::build)
                    .collect(Collectors.toList());
        });
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
    }

    private ObjectParts separateParts(Tag tag) {

        var header = tag.getHeader();

        var parts = new ObjectParts();
        parts.objectType = new ObjectType[] {header.getObjectType()};
        parts.objectId = new UUID[] {UUID.fromString(header.getObjectId())};
        parts.objectVersion = new int[] {header.getObjectVersion()};
        parts.tagVersion = new int[] {header.getTagVersion()};

        var objectTimestamp = MetadataCodec.decodeDatetime(header.getObjectTimestamp()).toInstant();
        var tagTimestamp = MetadataCodec.decodeDatetime(header.getTagTimestamp()).toInstant();

        parts.objectTimestamp = new Instant[] {objectTimestamp};
        parts.tagTimestamp = new Instant[] {tagTimestamp};

        parts.tag = new Tag[] {tag};
        parts.definition = new ObjectDefinition[] {tag.getDefinition()};

        return parts;
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

    private ObjectParts separateParts(ObjectType objectType, UUID objectId) {

        var parts = new ObjectParts();
        parts.objectType = new ObjectType[] {objectType};
        parts.objectId = new UUID[] {objectId};

        return parts;
    }

    private ObjectParts separateParts(List<ObjectType> objectTypes, List<UUID> objectIds) {

        var parts = new ObjectParts();
        parts.objectType = objectTypes.toArray(ObjectType[]::new);
        parts.objectId = objectIds.toArray(UUID[]::new);

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

}
