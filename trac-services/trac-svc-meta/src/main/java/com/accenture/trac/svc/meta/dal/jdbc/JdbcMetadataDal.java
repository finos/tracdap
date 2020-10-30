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

package com.accenture.trac.svc.meta.dal.jdbc;

import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.metadata.search.SearchParameters;
import com.accenture.trac.common.metadata.*;
import com.accenture.trac.common.db.JdbcDialect;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;


public class JdbcMetadataDal extends JdbcBaseDal implements IMetadataDal {

    private static final int LATEST_TAG = -1;
    private static final int LATEST_VERSION = -1;

    private final Logger log;

    private final JdbcTenantImpl tenants;
    private final JdbcReadImpl readSingle;
    private final JdbcReadBatchImpl readBatch;
    private final JdbcWriteBatchImpl writeBatch;
    private final JdbcSearchImpl search;


    public JdbcMetadataDal(JdbcDialect dialect, DataSource dataSource, Executor executor) {

        super(dialect, dataSource, executor);

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


    // -----------------------------------------------------------------------------------------------------------------
    // SAVE METHODS
    // -----------------------------------------------------------------------------------------------------------------

    // Assume that save methods are not highly sensitive to latency
    // So, use the same implementation for save one and save many
    // I.e. no special optimisation for saving a single item, even though this is the common case


    @Override
    public CompletableFuture<Void> saveNewObject(String tenant, Tag tag) {

        var parts = separateParts(tag);
        return saveNewObjects(tenant, parts);
    }

    @Override
    public CompletableFuture<Void> saveNewObjects(String tenant, List<Tag> tags) {

        var parts = separateParts(tags);
        return saveNewObjects(tenant, parts);
    }

    private CompletableFuture<Void> saveNewObjects(String tenant, ObjectParts parts) {

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(tenant);

            long[] objectPk = writeBatch.writeObjectId(conn, tenantId, parts);
            long[] defPk = writeBatch.writeObjectDefinition(conn, tenantId, objectPk, parts);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);

            writeBatch.writeLatestVersion(conn, tenantId, objectPk, defPk);
            writeBatch.writeLatestTag(conn, tenantId, defPk, tagPk);
        },
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts));
    }

    @Override
    public CompletableFuture<Void> saveNewVersion(String tenant, Tag tag) {

        var parts = separateParts(tag);
        return saveNewVersions(tenant, parts);
    }

    @Override
    public CompletableFuture<Void> saveNewVersions(String tenant, List<Tag> tags) {

        var parts = separateParts(tags);
        return saveNewVersions(tenant, parts);
    }

    private CompletableFuture<Void> saveNewVersions(String tenant, ObjectParts parts) {

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(tenant);
            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);

            checkObjectTypes(parts, objectType);

            long[] defPk = writeBatch.writeObjectDefinition(conn, tenantId, objectType.keys, parts);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);

            writeBatch.updateLatestVersion(conn, tenantId, objectType.keys, defPk);
            writeBatch.writeLatestTag(conn, tenantId, defPk, tagPk);
        },
        (error, code) -> JdbcError.handleMissingItem(error, code, parts),
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts),
        (error, code) ->  JdbcError.newVersion_WrongType(error, code, parts));
    }

    @Override
    public CompletableFuture<Void> saveNewTag(String tenant, Tag tag) {

        var parts = separateParts(tag);
        return saveNewTags(tenant, parts);
    }

    @Override
    public CompletableFuture<Void> saveNewTags(String tenant, List<Tag> tags) {

        var parts = separateParts(tags);
        return saveNewTags(tenant, parts);
    }

    private CompletableFuture<Void> saveNewTags(String tenant, ObjectParts parts) {

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(tenant);
            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);

            checkObjectTypes(parts, objectType);

            long[] defPk = readBatch.lookupDefinitionPk(conn, tenantId, objectType.keys, parts.objectVersion);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);

            writeBatch.updateLatestTag(conn, tenantId, defPk, tagPk);
        },
        (error, code) -> JdbcError.handleMissingItem(error, code, parts),
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts),
        (error, code) ->  JdbcError.newTag_WrongType(error, code, parts));
    }

    @Override
    public CompletableFuture<Void> preallocateObjectId(String tenant, ObjectType objectType, UUID objectId) {

        var parts = separateParts(objectType, objectId);
        return preallocateObjectIds(tenant, parts);
    }

    @Override
    public CompletableFuture<Void> preallocateObjectIds(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds) {

        var parts = separateParts(objectTypes, objectIds);
        return preallocateObjectIds(tenant, parts);
    }

    private CompletableFuture<Void> preallocateObjectIds(String tenant, ObjectParts parts) {

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(tenant);

            writeBatch.writeObjectId(conn, tenantId, parts);
        },
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts));
    }

    @Override
    public CompletableFuture<Void> savePreallocatedObject(String tenant, Tag tag) {

        var parts = separateParts(tag);
        return savePreallocatedObjects(tenant, parts);
    }

    @Override
    public CompletableFuture<Void> savePreallocatedObjects(String tenant, List<Tag> tags) {

        var parts = separateParts(tags);
        return savePreallocatedObjects(tenant, parts);
    }

    private CompletableFuture<Void> savePreallocatedObjects(String tenant, ObjectParts parts) {

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(tenant);
            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);

            checkObjectTypes(parts, objectType);

            long[] defPk = writeBatch.writeObjectDefinition(conn, tenantId, objectType.keys, parts);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);

            writeBatch.writeLatestVersion(conn, tenantId, objectType.keys, defPk);
            writeBatch.writeLatestTag(conn, tenantId, defPk, tagPk);
        },
        (error, code) -> JdbcError.handleMissingItem(error, code, parts),   // TODO: different errors
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts),  // TODO: different errors
        (error, code) ->  JdbcError.savePreallocated_WrongType(error, code, parts));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // LOAD METHODS (SINGLE ITEM)
    // -----------------------------------------------------------------------------------------------------------------

    // Load methods *are* highly sensitive to latency
    // This is because UI applications use metadata queries to decide what to display
    // These queries may also be recursive, i.e. query a job, then related data/models or dependent jobs
    // We want these multiple queries to complete in < 100 ms for a fluid user experience
    // So, optimising the common case of querying a single item makes sense


    @Override public CompletableFuture<Tag>
    loadTag(String tenant, ObjectType objectType, UUID objectId, int objectVersion, int tagVersion) {

        var parts = assembleParts(objectType, objectId, objectVersion, tagVersion);

        return wrapTransaction(conn -> {

            var tenantId = tenants.getTenantId(tenant);
            var storedType = readSingle.readObjectTypeById(conn, tenantId, objectId);

            checkObjectType(parts, storedType);

            var definition = readSingle.readDefinitionByVersion(conn, tenantId, storedType.key, objectVersion);
            var tagRecord = readSingle.readTagRecordByVersion(conn, tenantId, definition.key, tagVersion);
            var tagAttrs = readSingle.readTagAttrs(conn, tenantId, tagRecord.key);

            return buildTag(objectType, objectId, definition, tagRecord, tagAttrs);
        },
        (error, code) -> JdbcError.loadOne_missingItem(error, code, parts),
        (error, code) -> JdbcError.loadOne_WrongObjectType(error, code, parts));
    }

    @Override public CompletableFuture<Tag>
    loadLatestTag(String tenant, ObjectType objectType, UUID objectId, int objectVersion) {

        var parts = assembleParts(objectType, objectId, objectVersion, LATEST_TAG);

        return wrapTransaction(conn -> {

            var tenantId = tenants.getTenantId(tenant);
            var storedType = readSingle.readObjectTypeById(conn, tenantId, objectId);

            checkObjectType(parts, storedType);

            var definition = readSingle.readDefinitionByVersion(conn, tenantId, storedType.key, objectVersion);
            var tagRecord = readSingle.readTagRecordByLatest(conn, tenantId, definition.key);
            var tagAttrs = readSingle.readTagAttrs(conn, tenantId, tagRecord.key);

            return buildTag(objectType, objectId, definition, tagRecord, tagAttrs);
        },
        (error, code) -> JdbcError.loadOne_missingItem(error, code, parts),
        (error, code) -> JdbcError.loadOne_WrongObjectType(error, code, parts));
    }

    @Override public CompletableFuture<Tag>
    loadLatestVersion(String tenant, ObjectType objectType, UUID objectId) {

        var parts = assembleParts(objectType, objectId, LATEST_VERSION, LATEST_TAG);

        return wrapTransaction(conn -> {

            var tenantId = tenants.getTenantId(tenant);
            var storedType = readSingle.readObjectTypeById(conn, tenantId, objectId);

            checkObjectType(parts, storedType);

            var definition = readSingle.readDefinitionByLatest(conn, tenantId, storedType.key);
            var tagRecord = readSingle.readTagRecordByLatest(conn, tenantId, definition.key);
            var tagAttrs = readSingle.readTagAttrs(conn, tenantId, tagRecord.key);

            return buildTag(objectType, objectId, definition, tagRecord, tagAttrs);
        },
        (error, code) -> JdbcError.loadOne_missingItem(error, code, parts),
        (error, code) -> JdbcError.loadOne_WrongObjectType(error, code, parts));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // LOAD METHODS (MULTIPLE ITEMS)
    // -----------------------------------------------------------------------------------------------------------------

    // Load batch methods may be used e.g. to query all items related to a job in a single query
    // This can be used both by the platform (e.g. to set up a job) and applications / UI (e.g. to display a job)
    // Latency remains important, optimisations are in ReadBatchImpl


    @Override public CompletableFuture<List<Tag>>
    loadTags(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds, List<Integer> objectVersions, List<Integer> tagVersions) {

        var parts = assembleParts(objectTypes, objectIds, objectVersions, tagVersions);

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(tenant);
            var objPk = lookupObjectPks(conn, tenantId, parts);
            var definition = readBatch.readDefinitionByVersion(conn, tenantId, parts.objectType, objPk, parts.objectVersion);
            var tag = readBatch.readTagByVersion(conn, tenantId, definition.keys, parts.tagVersion);

            return buildTags(objectTypes, objectIds, definition, tag);
        },
        (error, code) -> JdbcError.loadBatch_missingItem(error, code, parts),
        (error, code) -> JdbcError.loadBatch_WrongObjectType(error, code, parts));
    }

    @Override public CompletableFuture<List<Tag>>
    loadLatestTags(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds, List<Integer> objectVersions) {

        var parts = assembleParts(objectTypes, objectIds, objectVersions,
                Collections.nCopies(objectIds.size(), LATEST_TAG));

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(tenant);
            var objPk = lookupObjectPks(conn, tenantId, parts);
            var definition = readBatch.readDefinitionByVersion(conn, tenantId, parts.objectType, objPk, parts.objectVersion);
            var tag = readBatch.readTagByLatest(conn, tenantId, definition.keys);

            return buildTags(objectTypes, objectIds, definition, tag);
        },
        (error, code) -> JdbcError.loadBatch_missingItem(error, code, parts),
        (error, code) -> JdbcError.loadBatch_WrongObjectType(error, code, parts));
    }

    @Override public CompletableFuture<List<Tag>>
    loadLatestVersions(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds) {

        var parts = assembleParts(objectTypes, objectIds,
                Collections.nCopies(objectIds.size(), LATEST_VERSION),
                Collections.nCopies(objectIds.size(), LATEST_TAG));

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(tenant);
            var objPk = lookupObjectPks(conn, tenantId, parts);
            var definition = readBatch.readDefinitionByLatest(conn, tenantId, parts.objectType, objPk);
            var tag = readBatch.readTagByLatest(conn, tenantId, definition.keys);

            return buildTags(objectTypes, objectIds, definition, tag);
        },
        (error, code) -> JdbcError.loadBatch_missingItem(error, code, parts),
        (error, code) -> JdbcError.loadBatch_WrongObjectType(error, code, parts));
    }

    private long[]
    lookupObjectPks(Connection conn, short tenantId, ObjectParts parts) throws SQLException {

        var storedType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
        checkObjectTypes(parts, storedType);

        return storedType.keys;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // SEARCH METHODS
    // -----------------------------------------------------------------------------------------------------------------


    @Override public CompletableFuture<List<Tag>>
    search(String tenant, SearchParameters searchParameters) {

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(tenant);
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
    }


    private ObjectParts separateParts(Tag tag) {

        var header = tag.getHeader();

        var parts = new ObjectParts();
        parts.objectType = new ObjectType[] {header.getObjectType()};
        parts.objectId = new UUID[] {UUID.fromString(header.getObjectId())};
        parts.objectVersion = new int[] {header.getObjectVersion()};
        parts.tagVersion = new int[] {header.getTagVersion()};

        var objectTimestamp = MetadataCodec.parseDatetime(header.getObjectTimestamp()).toInstant();
        var tagTimestamp = MetadataCodec.parseDatetime(header.getTagTimestamp()).toInstant();

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
                .map(MetadataCodec::parseDatetime)
                .map(OffsetDateTime::toInstant)
                .toArray(Instant[]::new);

        parts.tagTimestamp = Arrays.stream(headers)
                .map(TagHeader::getTagTimestamp)
                .map(MetadataCodec::parseDatetime)
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

    private ObjectParts assembleParts(ObjectType type, UUID id, int version, int tagVersion) {

        var parts = new ObjectParts();
        parts.objectType = new ObjectType[] {type};
        parts.objectId = new UUID[] {id};
        parts.objectVersion = new int[] {version};
        parts.tagVersion = new int[] {tagVersion};

        return parts;
    }

    private ObjectParts assembleParts(List<ObjectType> types, List<UUID> ids, List<Integer> versions, List<Integer> tagVersions) {

        var parts = new ObjectParts();
        parts.objectType = types.toArray(ObjectType[]::new);
        parts.objectId = ids.toArray(UUID[]::new);
        parts.objectVersion = versions.stream().mapToInt(x -> x).toArray();
        parts.tagVersion = tagVersions.stream().mapToInt(x -> x).toArray();

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
                .setObjectTimestamp(MetadataCodec.quoteDatetime(objectTimestamp))
                .setTagTimestamp(MetadataCodec.quoteDatetime(tagTimestamp));

        return Tag.newBuilder()
                .setHeader(header)
                .setDefinition(definition.item)
                .putAllAttr(tagAttrs)
                .build();
    }

    private List<Tag> buildTags(
            List<ObjectType> objectType, List<UUID> objectId,
            KeyedItems<ObjectDefinition> definitions,
            KeyedItems<Tag.Builder> tags) {

        var result = new ArrayList<Tag>(objectId.size());

        for (int i = 0; i < objectId.size(); i++) {

            var objectTimestamp = definitions.timestamps[i].atOffset(ZoneOffset.UTC);
            var tagTimestamp = tags.timestamps[i].atOffset(ZoneOffset.UTC);

            var header = TagHeader.newBuilder()
                    .setObjectType(objectType.get(i))
                    .setObjectId(objectId.get(i).toString())
                    .setObjectVersion(definitions.versions[i])
                    .setTagVersion(tags.versions[i])
                    .setObjectTimestamp(MetadataCodec.quoteDatetime(objectTimestamp))
                    .setTagTimestamp(MetadataCodec.quoteDatetime(tagTimestamp));

            var tag = tags.items[i]
                    .setHeader(header)
                    .setDefinition(definitions.items[i])
                    .build();

            result.add(tag);
        }

        return result;
    }

}
