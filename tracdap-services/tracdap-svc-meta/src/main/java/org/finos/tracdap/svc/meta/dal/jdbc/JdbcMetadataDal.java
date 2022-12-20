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

import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.db.JdbcDialect;
import org.finos.tracdap.svc.meta.dal.*;

import org.finos.tracdap.svc.meta.dal.operations.*;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class JdbcMetadataDal extends JdbcBaseDal implements IMetadataDal {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataSource dataSource;

    private final JdbcTenantImpl tenants;
    private final JdbcReadImpl readSingle;
    private final JdbcReadBatchImpl readBatch;
    private final JdbcWriteBatchImpl writeBatch;
    private final JdbcSearchImpl search;


    public JdbcMetadataDal(JdbcDialect dialect, DataSource dataSource) {

        super(dialect, dataSource);

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
    public void runWriteOperations(String tenant, List<DalWriteOperation> operations) {
        if (operations.isEmpty()) {
            return;
        }

        ensureNoRepeatedOperations(operations);

        var parts = separateParts(operations.get(0));

        wrapTransaction(conn -> {
                prepareMappingTable(conn);
                var tenantId = tenants.getTenantId(conn, tenant);

                for (var operation : operations) {
                    handleOperation(conn, tenantId, operation);
                }
            },
            (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts),
            (error, code) -> JdbcError.handleMissingItem(error, code, parts),
            (error, code) ->  JdbcError.handleWrongObjectType(error, code, parts)
        );
    }

    private void ensureNoRepeatedOperations(List<DalWriteOperation> operations) {
        var counts = operations.stream()
                .map(DalWriteOperation::getClass)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        var operationRepeated = counts.values().stream().anyMatch(cnt -> cnt > 1);

        if (operationRepeated) {
            throw new ETracInternal("some DAL write operation was repeated");
        }
    }

    private void handleOperation(Connection conn, short tenantId, DalWriteOperation operation) throws SQLException {
        if (operation instanceof WriteOperationWithTag) {
            handleWriteOperationWithTag(conn, tenantId, (WriteOperationWithTag)operation);
        }
        else if (operation instanceof PreallocateObjectId) {
            handlePreallocateObjectId(conn, tenantId, (PreallocateObjectId)operation);
        }
        else {
            throw new ETracInternal("invalid DalWriteOperation");
        }
    }

    private void handlePreallocateObjectId(Connection conn, short tenantId, PreallocateObjectId operation) throws SQLException {
        writeBatch.writeObjectId(conn, tenantId, separateParts(operation));
    }

    private void handleWriteOperationWithTag(Connection conn, short tenantId, WriteOperationWithTag operation) throws SQLException {
        var parts = separateParts(operation);

        long[] defPk;

        if (operation instanceof SaveNewObject) {
            defPk = getDefPkForSaveNewObject(conn, tenantId, parts);
        }
        else if (operation instanceof SaveNewTag) {
            defPk = getDefPkForSaveNewTag(conn, tenantId, parts);
        }
        else if (operation instanceof SaveNewVersion) {
            defPk = getDefPkForSaveNewVersion(conn, tenantId, parts);
        }
        else if (operation instanceof SavePreallocatedObject) {
            defPk = getDefPkForSavePreallocatedObject(conn, tenantId, parts);
        }
        else {
            throw new ETracInternal("invalid DalWriteOperation");
        }

        long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts);
        writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts);
    }

    private long[] getDefPkForSavePreallocatedObject(Connection conn, short tenantId, ObjectParts parts) throws SQLException {
        var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
        checkObjectTypes(parts, objectType);

        return writeBatch.writeObjectDefinition(conn, tenantId, objectType.keys, parts);
    }

    private long[] getDefPkForSaveNewTag(Connection conn, short tenantId, ObjectParts parts) throws SQLException {
        var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
        checkObjectTypes(parts, objectType);

        var defPk = readBatch.lookupDefinitionPk(conn, tenantId, objectType.keys, parts.objectVersion);
        writeBatch.closeTagRecord(conn, tenantId, defPk, parts);

        return defPk;
    }

    private long[] getDefPkForSaveNewVersion(Connection conn, short tenantId, ObjectParts parts) throws SQLException {
        var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
        checkObjectTypes(parts, objectType);

        writeBatch.closeObjectDefinition(conn, tenantId, objectType.keys, parts);

        return writeBatch.writeObjectDefinition(conn, tenantId, objectType.keys, parts);
    }

    private long[] getDefPkForSaveNewObject(Connection conn, short tenantId, ObjectParts parts) throws SQLException {
        var objectPk = writeBatch.writeObjectId(conn, tenantId, parts);
        return writeBatch.writeObjectDefinition(conn, tenantId, objectPk, parts);
    }

    @Override
    public void saveNewObjects(String tenant, List<Tag> tags) {
        runWriteOperations(tenant, Collections.singletonList(new SaveNewObject(tags)));
    }

    @Override
    public void saveNewVersions(String tenant, List<Tag> tags) {
        runWriteOperations(tenant, Collections.singletonList(new SaveNewVersion(tags)));
    }

    @Override
    public void saveNewTags(String tenant, List<Tag> tags) {
        runWriteOperations(tenant, Collections.singletonList(new SaveNewTag(tags)));
    }

    @Override
    public void preallocateObjectIds(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds) {
        runWriteOperations(tenant, Collections.singletonList(new PreallocateObjectId(objectTypes, objectIds)));
    }

    @Override
    public void savePreallocatedObjects(String tenant, List<Tag> tags) {
        runWriteOperations(tenant, Collections.singletonList(new SavePreallocatedObject(tags)));
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

    private ObjectParts separateParts(DalWriteOperation operation) {
        if (operation instanceof WriteOperationWithTag) {
            var op = (WriteOperationWithTag) operation;

            return separateParts(op.getTags());
        } else if (operation instanceof PreallocateObjectId) {
            var op = (PreallocateObjectId) operation;

            return separateParts(op.getObjectTypes(), op.getObjectIds());
        } else {
            throw new ETracInternal("invalid DalWriteOperation");
        }
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
