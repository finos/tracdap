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

package org.finos.tracdap.common.metadata.store.jdbc;

import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.db.JdbcErrorCode;
import org.finos.tracdap.common.db.JdbcException;
import org.finos.tracdap.common.db.dialects.IDialect;
import org.finos.tracdap.common.exception.EValidationGap;
import org.finos.tracdap.common.metadata.MetadataCodec;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;


class JdbcReadBatchImpl {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IDialect dialect;
    private final AtomicInteger mappingStage;

    JdbcReadBatchImpl(IDialect dialect) {
        this.dialect = dialect;
        this.mappingStage = new AtomicInteger();
    }

    KeyedItems<ObjectType>
    readObjectTypeById(Connection conn, short tenantId, UUID[] objectId) throws SQLException {

        var mappingStage = insertIdForMapping(conn, objectId);
        mapObjectById(conn, tenantId, mappingStage);

        var query =
                "select object_pk, object_type\n" +
                "from object_id oid\n" +
                "join key_mapping km\n" +
                "  on oid.object_pk = km.pk\n" +
                "where oid.tenant_id = ?\n" +
                "  and km.mapping_stage = ?\n" +
                "order by km.ordering";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY readObjectTypeById: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            try (var rs = stmt.executeQuery()) {

                var keys = new long[objectId.length];
                var types = new ObjectType[objectId.length];

                for (int i = 0; i < objectId.length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA);

                    var pk = rs.getLong(1);
                    var objectTypeCode = rs.getString(2);
                    var objectType = ObjectType.valueOf(objectTypeCode);

                    keys[i] = pk;
                    types[i] = objectType;
                }

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                return new KeyedItems<>(keys, types);
            }
        }
    }

    KeyedItems<ObjectDefinition>
    readDefinition(Connection conn, short tenantId, long[] objectFk, TagSelector[] selector) throws SQLException {

        var mappingStage = insertObjectSelectors(conn, objectFk, selector);
        mapObjectSelectors(conn, tenantId, mappingStage);

        return fetchDefinition(conn, tenantId, objectFk.length, mappingStage);
    }

    private KeyedItems<ObjectDefinition>
    fetchDefinition(
            Connection conn, short tenantId,
            int length, int mappingStage)
            throws SQLException {

        var query =
                "select definition_pk, object_version, object_timestamp, definition, object_is_latest\n" +
                "from object_definition def\n" +
                "join key_mapping km\n" +
                "  on def.definition_pk = km.pk\n" +
                "where def.tenant_id = ?\n" +
                "  and km.mapping_stage = ?\n" +
                "order by km.ordering";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY fetchDefinition: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            try (var rs = stmt.executeQuery()) {

                long[] pks = new long[length];
                int[] versions = new int[length];
                Instant[] timestamps = new Instant[length];
                ObjectDefinition[] defs = new ObjectDefinition[length];
                boolean[] objectsIsLatest = new boolean[length];

                for (var i = 0; i < length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA);

                    var defPk = rs.getLong(1);
                    var defVersion = rs.getInt(2);
                    var sqlTimestamp = rs.getTimestamp(3);
                    var defTimestamp = sqlTimestamp.toInstant();
                    var defEncoded = rs.getBytes(4);
                    var defDecoded = ObjectDefinition.parseFrom(defEncoded);
                    var objectIsLatest = rs.getBoolean(5);

                    // TODO: Encode / decode helper, type = protobuf | json ?

                    pks[i] = defPk;
                    versions[i] = defVersion;
                    timestamps[i] = defTimestamp;
                    defs[i] = defDecoded;
                    objectsIsLatest[i] = objectIsLatest;
                }

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                return new KeyedItems<>(pks, versions, timestamps, defs, objectsIsLatest);
            }
            catch (InvalidProtocolBufferException e) {
                throw new MetadataException(MetadataErrorCode.INVALID_OBJECT_DEFINITION);
            }

        }
    }

    KeyedItems<Tag.Builder>
    readTag(Connection conn, short tenantId, long[] definitionFk, TagSelector[] selector) throws SQLException {

        var mappingStage = insertTagSelectors(conn, definitionFk, selector);
        mapTagSelectors(conn, tenantId, mappingStage);

        var tagRecords = fetchTagRecord(conn, tenantId, definitionFk.length, mappingStage);
        var attrs = fetchTagAttrs(conn, tenantId, definitionFk.length, mappingStage);

        return applyTagAttrs(tagRecords, attrs);
    }

    KeyedItems<Tag.Builder>
    readTagWithHeader(Connection conn, short tenantId, long[] tagPk) throws SQLException {

        var mappingStage = insertPk(conn, tagPk);
        mapDefinitionByTagPk(conn, tenantId, mappingStage);

        var tagRecords = fetchTagRecord(conn, tenantId, tagPk.length, mappingStage);
        var partialHeaders = fetchTagHeader(conn, tenantId, tagPk.length, mappingStage);
        var attrs = fetchTagAttrs(conn, tenantId, tagPk.length, mappingStage);

        return applyTagAttrs(tagRecords, partialHeaders, attrs);
    }

    private KeyedItems<Void>
    fetchTagRecord(Connection conn, short tenantId, int length, int mappingStage) throws SQLException {

        // Tag records contain no attributes, we only need pks and versions
        // Note: Common attributes may be added to the tag table as search optimisations, but do not need to be read

        var query =
                "select tag.tag_pk, tag.tag_version, tag.tag_timestamp, tag.tag_is_latest\n" +
                "from tag\n" +
                "join key_mapping km\n" +
                "  on tag.tag_pk = km.pk\n" +
                "where tag.tenant_id = ?\n" +
                "  and km.mapping_stage = ?\n" +
                "order by km.ordering";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY fetchTagRecord: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            try (var rs = stmt.executeQuery()) {

                long[] pks = new long[length];
                int[] versions = new int[length];
                Instant[] timestamps = new Instant[length];
                boolean[] tagsIsLatest = new boolean[length];

                for (var i = 0; i < length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA);

                    var tagPk = rs.getLong(1);
                    var tagVersion = rs.getInt(2);
                    var sqlTimestamp = rs.getTimestamp(3);
                    var tagTimestamp = sqlTimestamp.toInstant();
                    var tagIsLatest = rs.getBoolean(4);

                    pks[i] = tagPk;
                    versions[i] = tagVersion;
                    timestamps[i] = tagTimestamp;
                    tagsIsLatest[i] = tagIsLatest;
                }

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                // Tag record requires only PK and version info
                return new KeyedItems<>(pks, versions, timestamps, null, tagsIsLatest);
            }
        }
    }

    private KeyedItems<TagHeader>
    fetchTagHeader(Connection conn, short tenantId, int length, int mappingStage) throws SQLException {

        // Data to build the full header is in the object and definition tables
        // Assume key_mapping has been populated with keys from the tag table:
        //  - key_mapping.pk = tag_pk
        //  - key_mapping.ver = tag_version
        //  - key_mapping.fk = definition_fk
        // Now we only need to join object_definition and object_id
        // To set up this mapping, put tag PKs into key_mapping and call mapDefinitionByTagPk()

        var query = "select \n" +
                "  km.fk as def_pk\n," +
                "  obj.object_type\n," +
                "  obj.object_id_hi,\n" +
                "  obj.object_id_lo,\n" +
                "  def.object_version,\n" +
                "  def.object_timestamp,\n" +
                "  def.object_is_latest\n"+
                "from key_mapping km\n" +
                "join object_definition def\n" +
                "  on def.definition_pk = km.fk\n" +
                "join object_id obj\n" +
                "  on obj.tenant_id = def.tenant_id\n" +
                "  and obj.object_pk = def.object_fk\n" +
                "where def.tenant_id = ?\n" +
                "  and km.mapping_stage = ?\n" +
                "order by km.ordering";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY fetchTagHeader: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            try (var rs = stmt.executeQuery()) {

                var pks = new long[length];
                var headers = new TagHeader[length];

                for (int i = 0; i < length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA);

                    var definitionPk = rs.getLong(1);
                    var objectTypeCode = rs.getString(2);
                    var objectIdHi = rs.getLong(3);
                    var objectIdLo = rs.getLong(4);
                    var objectVersion = rs.getInt(5);
                    var sqlTimestamp = rs.getTimestamp(6);
                    var objectTimestamp = sqlTimestamp.toInstant();
                    var isLatestObject = rs.getBoolean(7);

                    var objectId = new UUID(objectIdHi, objectIdLo);
                    var objectType = ObjectType.valueOf(objectTypeCode);

                    var header = TagHeader.newBuilder()
                            .setObjectType(objectType)
                            .setObjectId(objectId.toString())
                            .setObjectVersion(objectVersion)
                            .setIsLatestObject(isLatestObject)
                            .setObjectTimestamp(MetadataCodec.encodeDatetime(objectTimestamp.atOffset(ZoneOffset.UTC)))
                            .build();

                    pks[i] = definitionPk;
                    headers[i] = header;
                }

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                return new KeyedItems<>(pks, headers);
            }
        }
    }

    private Map<String, Value>[]
    fetchTagAttrs(Connection conn, short tenantId, int nTags, int mappingStage) throws SQLException {

        // PKs are inserted into the key mapping table in the order of tag PK
        // We read back attribute records according to those PKs
        // There will be multiple entries per tagPk, i.e. [0, n)
        // The order of attributes within each tag is not known

        var query =
                "select ta.*, km.ordering as tag_index\n" +
                "from key_mapping km\n" +
                "left join tag_attr ta\n" +
                "  on ta.tag_fk = km.pk\n" +
                "where ta.tenant_id = ?\n" +
                "  and km.mapping_stage = ?\n" +
                "order by km.ordering, ta.attr_name, ta.attr_index";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY fetchTagAttrs: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setInt(1, tenantId);
            stmt.setInt(2, mappingStage);

            try (var rs = stmt.executeQuery()) {

                @SuppressWarnings("unchecked")
                var result = (Map<String, Value>[]) new HashMap[nTags];

                // Start by storing attrs for tag index = 0
                var currentTagAttrs = new HashMap<String, Value>();
                var currentTagIndex = 0;

                var currentAttrArray = new ArrayList<Value>();
                var currentAttrName = "";

                while (rs.next()) {

                    var tagIndex = rs.getInt("tag_index");
                    var attrName = rs.getString("attr_name");
                    var attrIndex = rs.getInt("attr_index");
                    var attrValue = JdbcAttrHelpers.readAttrValue(rs);

                    // Check to see if we have finished processing a multi-valued attr
                    // If so, record it against the last tag and attr name before moving on
                    if (!currentAttrArray.isEmpty()) {
                        if (tagIndex != currentTagIndex || !attrName.equals(currentAttrName)) {

                            var arrayValue = JdbcAttrHelpers.assembleArrayValue(currentAttrArray);
                            currentTagAttrs.put(currentAttrName, arrayValue);

                            currentAttrArray = new ArrayList<>();
                        }
                    }

                    // Check if the current tag index has moved on
                    // If so store accumulated attrs for the previous index
                    while (currentTagIndex != tagIndex) {

                        result[currentTagIndex] = currentTagAttrs;

                        currentTagAttrs = new HashMap<>();
                        currentTagIndex++;
                    }

                    // Sanity check - should never happen
                    if (currentTagIndex >= nTags)
                        throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                    // Update current attr name
                    currentAttrName = attrName;

                    // Accumulate attr against the current tag index
                    if (attrIndex < 0)
                        currentTagAttrs.put(attrName, attrValue);
                    else
                        currentAttrArray.add(attrValue);
                }

                // Check in case the last attr record was part of a multi-valued attr
                if (!currentAttrArray.isEmpty()) {
                    var arrayValue = JdbcAttrHelpers.assembleArrayValue(currentAttrArray);
                    currentTagAttrs.put(currentAttrName, arrayValue);
                }

                // Store accumulated attrs for the final tag index
                if (nTags > 0) {
                    result[currentTagIndex] = currentTagAttrs;
                    currentTagIndex++;
                }

                // In the case where some tags have no attrs
                // Ensure an empty map is created for those tags
                while (currentTagIndex < nTags) {

                    result[currentTagIndex] = new HashMap<>();
                    currentTagIndex++;
                }

                return result;
            }
        }
    }

    private KeyedItems<Tag.Builder>
    applyTagAttrs(KeyedItems<Void> tagRecords, Map<String, Value>[] attrs) {

        var tags = new Tag.Builder[tagRecords.keys.length];

        for (var i = 0; i < tagRecords.keys.length; i++) {

            tags[i] = Tag.newBuilder()
                    .putAllAttrs(attrs[i]);
        }

        return new KeyedItems<>(tagRecords.keys, tagRecords.versions, tagRecords.timestamps, tags, tagRecords.isLatest);
    }

    private KeyedItems<Tag.Builder>
    applyTagAttrs(KeyedItems<Void> tagRecords, KeyedItems<TagHeader> headers, Map<String, Value>[] attrs) {

        var tags = new Tag.Builder[headers.keys.length];

        for (var i = 0; i < headers.keys.length; i++) {

            var tagTimestamp = MetadataCodec
                    .encodeDatetime(tagRecords.timestamps[i]
                    .atOffset(ZoneOffset.UTC));

            var header = headers.items[i].toBuilder()
                    .setTagVersion(tagRecords.versions[i])
                    .setTagTimestamp(tagTimestamp)
                    .setIsLatestTag(tagRecords.isLatest[i])
                    .build();

            tags[i] = Tag.newBuilder()
                    .setHeader(header)
                    .putAllAttrs(attrs[i]);
        }

        return new KeyedItems<>(headers.keys, tagRecords.versions, tagRecords.timestamps, tags, tagRecords.isLatest);
    }

    public KeyedItems<ConfigDetails>
    readConfigEntry(Connection conn, short tenantId, ConfigEntry[] configEntry, Instant[] configTimestamp, boolean includeDeleted) throws SQLException {

        var mappingStage = insertConfigEntries(conn, configEntry, configTimestamp);
        mapConfigEntries(conn, tenantId, mappingStage, includeDeleted);

        return fetchConfigEntry(conn, tenantId, configEntry.length, mappingStage);
    }

    public KeyedItems<ConfigDetails>
    readConfigEntry(Connection conn, short tenantId, long[] configPk) throws SQLException {

        var mappingStage = insertPk(conn, configPk);

        return fetchConfigEntry(conn, tenantId, configPk.length, mappingStage);
    }

    private KeyedItems<ConfigDetails>
    fetchConfigEntry(Connection conn, short tenantId, int length, int mappingStage)
    throws SQLException {

        var query =
                "select cfg.config_pk, cfg.config_key, cfg.config_version, cfg.config_timestamp, cfg.config_is_latest, cfg.config_deleted, cfg.details\n" +
                "from config_entry cfg\n" +
                "join key_mapping km\n" +
                "  on cfg.config_pk = km.pk\n" +
                "where cfg.tenant_id = ?\n" +
                "  and km.mapping_stage = ?\n" +
                "order by km.ordering";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY fetchConfigEntry: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            try (var rs = stmt.executeQuery()) {

                long[] pks = new long[length];
                String[] keys =  new String[length];
                int[] versions = new int[length];
                Instant[] timestamps = new Instant[length];
                boolean[] latest = new boolean[length];
                boolean[] deleted = new boolean[length];
                ConfigDetails[] details = new ConfigDetails[length];

                for (var i = 0; i < length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA);

                    var cfgPk = rs.getLong(1);
                    var cfgKey = rs.getString(2);
                    var cfgVersion = rs.getInt(3);
                    var sqlTimestamp = rs.getTimestamp(4);
                    var cfgTimestamp = sqlTimestamp.toInstant();
                    var cfgLatest = rs.getBoolean(5);
                    var cfgDeleted = rs.getBoolean(6);

                    // TODO: Encode / decode helper, type = protobuf | json ?
                    var detailsEncoded = rs.getBytes(7);
                    var detailsDecoded = cfgDeleted
                            ? ConfigDetails.getDefaultInstance()
                            : ConfigDetails.parseFrom(detailsEncoded);

                    pks[i] = cfgPk;
                    keys[i] = cfgKey;
                    versions[i] = cfgVersion;
                    timestamps[i] = cfgTimestamp;
                    latest[i] = cfgLatest;
                    deleted[i] = cfgDeleted;
                    details[i] = detailsDecoded;
                }

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                return new KeyedItems<>(pks, versions, timestamps, details, latest, deleted, keys);
            }
            catch (InvalidProtocolBufferException e) {
                throw new MetadataException(MetadataErrorCode.INVALID_CONFIG_ENTRY);
            }
        }
    }

    private KeyedItems<String>
    fetchConfigStub(Connection conn, short tenantId, int length, int mappingStage) throws SQLException {

        var query =
                "select config_pk, config_version, config_timestamp, config_is_latest, config_deleted, cfg.config_key\n" +
                "from config_entry cfg\n" +
                "join key_mapping km\n" +
                "  on cfg.config_pk = km.pk\n" +
                "where cfg.tenant_id = ?\n" +
                "  and km.mapping_stage = ?\n" +
                "order by km.ordering";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY fetchConfigStub: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            try (var rs = stmt.executeQuery()) {

                long[] pks = new long[length];
                int[] versions = new int[length];
                Instant[] timestamps = new Instant[length];
                boolean[] latest = new boolean[length];
                boolean[] deleted = new boolean[length];
                String[] keys =  new String[length];

                for (var i = 0; i < length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA);

                    var cfgPk = rs.getLong(1);
                    var cfgVersion = rs.getInt(2);
                    var sqlTimestamp = rs.getTimestamp(3);
                    var cfgTimestamp = sqlTimestamp.toInstant();
                    var cfgLatest = rs.getBoolean(4);
                    var cfgDeleted = rs.getBoolean(5);
                    var cfgKey = rs.getString(6);

                    pks[i] = cfgPk;
                    versions[i] = cfgVersion;
                    timestamps[i] = cfgTimestamp;
                    latest[i] = cfgLatest;
                    deleted[i] = cfgDeleted;
                    keys[i] = cfgKey;
                }

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                return new KeyedItems<>(pks, versions, timestamps, keys, latest, deleted);
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // KEY LOOKUP FUNCTIONS
    // -----------------------------------------------------------------------------------------------------------------

    long[] lookupObjectPks(Connection conn, short tenantId, UUID[] objectIds) throws SQLException {

        var mappingStage = insertIdForMapping(conn, objectIds);
        mapObjectById(conn, tenantId, mappingStage);

        return fetchMappedPk(conn, mappingStage, objectIds.length);
    }

    long[] lookupDefinitionPk(Connection conn, short tenantId, long[] objectPk, int[] version) throws SQLException {

        var mappingStage = insertFkAndVersionForMapping(conn, objectPk, version);
        mapDefinitionByVersion(conn, tenantId, mappingStage);

        return fetchMappedPk(conn, mappingStage, objectPk.length);
    }

    long[] lookupDefinitionPk(Connection conn, short tenantId, long[] objectPk, TagSelector[] objectSelectors) throws SQLException {

        var mappingStage = insertObjectSelectors(conn, objectPk, objectSelectors);
        mapObjectSelectors(conn, tenantId, mappingStage);

        return fetchMappedPk(conn, mappingStage, objectPk.length);
    }

    long[] lookupTagPk(Connection conn, short tenantId, long[] definitionPk, int[] tagVersion) throws SQLException {

        var mappingStage = insertFkAndVersionForMapping(conn, definitionPk, tagVersion);
        mapTagByVersion(conn, tenantId, mappingStage);

        return fetchMappedPk(conn, mappingStage, definitionPk.length);
    }

    long[] lookupConfigPkByVersion(Connection conn, short tenantId, ConfigEntry[] configEntry, boolean includeDeleted) throws SQLException {

        var mappingStage = insertConfigEntries(conn, configEntry, null);
        mapConfigEntriesByVersion(conn, tenantId, mappingStage, includeDeleted);

        return fetchMappedPk(conn, mappingStage, configEntry.length);
    }

    private long[] fetchMappedPk(Connection conn, int mappingStage, int length) throws SQLException {

        var query =
                "select pk from key_mapping\n" +
                "where mapping_stage = ?\n" +
                "order by ordering";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY fetchMappedPk: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setInt(1, mappingStage);

            try (var rs = stmt.executeQuery()) {

                long[] keys = new long[length];

                for (int i = 0; i < length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA);

                    keys[i] = rs.getLong(1);

                    if (rs.wasNull())
                        throw new JdbcException(JdbcErrorCode.NO_DATA);
                }

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                return keys;
            }
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // KEY MAPPING FUNCTIONS
    // -----------------------------------------------------------------------------------------------------------------

    // Directly insert the PKs that will be read

    private int insertPk(Connection conn, long[] pks) throws SQLException {

        var query =
                "insert into key_mapping (pk, mapping_stage, ordering)\n" +
                "values (?, ?, ?)";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY insertPk: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            var mappingStage = nextMappingStage();

            for (var i = 0; i < pks.length; i++) {

                stmt.clearParameters();

                stmt.setLong(1, pks[i]);
                stmt.setInt(2, mappingStage);
                stmt.setInt(3, i);

                stmt.addBatch();
            }

            stmt.executeBatch();

            return mappingStage;
        }
    }


    // Insert object ID, map to object PK

    private int insertIdForMapping(Connection conn, UUID[] ids) throws SQLException {

        var query =
                "insert into key_mapping (id_hi, id_lo, mapping_stage, ordering)\n" +
                "values (?, ?, ?, ?)";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY insertIdForMapping: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            var mappingStage = nextMappingStage();

            for (var i = 0; i < ids.length; i++) {

                stmt.clearParameters();

                stmt.setLong(1, ids[i].getMostSignificantBits());
                stmt.setLong(2, ids[i].getLeastSignificantBits());
                stmt.setInt(3, mappingStage);
                stmt.setInt(4, i);

                stmt.addBatch();
            }

            stmt.executeBatch();

            return mappingStage;
        }
    }

    private void mapObjectById(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var query = "update key_mapping\n" +
                "set pk = (" +
                "  select object_pk from object_id oid\n" +
                "  where oid.tenant_id = ?\n" +
                "  and oid.object_id_hi = key_mapping.id_hi\n" +
                "  and oid.object_id_lo = key_mapping.id_lo)\n" +
                "where mapping_stage = ?";

        query = query.replaceAll("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY mapObjectById: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query))  {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.execute();
        }
    }


    // Insert selectors for objects, map to definition pk

    private int insertObjectSelectors(Connection conn, long[] objectFk, TagSelector[] selector) throws SQLException {

        var mappingStage = nextMappingStage();

        var query = "insert into key_mapping (\n" +
                "  fk, ver, as_of, is_latest,\n" +
                "  mapping_stage, ordering)\n" +
                "values (?, ?, ?, ?, ?, ?)";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY insertObjectSelectors: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            for (var i = 0; i < objectFk.length; i++) {

                var criteria = selector[i].getObjectCriteriaCase();

                stmt.clearParameters();

                stmt.setLong(1, objectFk[i]);
                stmt.setNull(2, Types.INTEGER);
                stmt.setNull(3, Types.TIMESTAMP);
                stmt.setNull(4, Types.BOOLEAN);

                switch (criteria) {

                    case OBJECTVERSION:
                        stmt.setInt(2, selector[i].getObjectVersion());
                        break;

                    case OBJECTASOF:
                        var objectAsOf = MetadataCodec.decodeDatetime(selector[i].getObjectAsOf()).toInstant();
                        var sqlAsOf = java.sql.Timestamp.from(objectAsOf);
                        stmt.setTimestamp(3, sqlAsOf);
                        break;

                    case LATESTOBJECT:
                        stmt.setBoolean(4, true);
                        break;

                    default:
                        throw new EValidationGap("Object criteria not set in selector");
                }

                stmt.setInt(5, mappingStage);
                stmt.setInt(6, i);

                stmt.addBatch();
            }

            stmt.executeBatch();
        }

        return mappingStage;
    }

    private void mapObjectSelectors(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var query = "update key_mapping\n" +
                "set pk = (" +
                "  select definition_pk from object_definition def\n" +
                "  where def.tenant_id = ?\n" +
                "  and def.object_fk = key_mapping.fk\n" +
                "  and (" +
                "    (key_mapping.ver is not null and def.object_version = key_mapping.ver) or\n" +
                "    (key_mapping.as_of is not null and def.object_timestamp <= key_mapping.as_of and\n" +
                "    (def.object_superseded is null or def.object_superseded > key_mapping.as_of)) or\n" +
                "    (key_mapping.is_latest is not null and def.object_is_latest = key_mapping.is_latest)))\n" +
                "where mapping_stage = ?";

        query = query.replaceAll("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY mapObjectSelectors: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.executeUpdate();
        }
    }


    // Insert selectors for tags, map to tag pk

    private int insertTagSelectors(Connection conn, long[] definitionFk, TagSelector[] selector) throws SQLException {

        var mappingStage = nextMappingStage();

        var query = "insert into key_mapping (\n" +
                "  fk, ver, as_of, is_latest,\n" +
                "  mapping_stage, ordering)\n" +
                "values (?, ?, ?, ?, ?, ?)";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY insertTagSelectors: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            for (var i = 0; i < definitionFk.length; i++) {

                var criteria = selector[i].getTagCriteriaCase();

                stmt.clearParameters();

                stmt.setLong(1, definitionFk[i]);
                stmt.setNull(2, Types.INTEGER);
                stmt.setNull(3, Types.TIMESTAMP);
                stmt.setNull(4, Types.BOOLEAN);

                switch (criteria) {

                    case TAGVERSION:
                        stmt.setInt(2, selector[i].getTagVersion());
                        break;

                    case TAGASOF:
                        var tagAsOf = MetadataCodec.decodeDatetime(selector[i].getTagAsOf()).toInstant();
                        var sqlAsOf = java.sql.Timestamp.from(tagAsOf);
                        stmt.setTimestamp(3, sqlAsOf);
                        break;

                    case LATESTTAG:
                        stmt.setBoolean(4, true);
                        break;

                    default:
                        throw new EValidationGap("Tag criteria not set in selector");
                }

                stmt.setInt(5, mappingStage);
                stmt.setInt(6, i);

                stmt.addBatch();
            }

            stmt.executeBatch();
        }

        return mappingStage;
    }

    private void mapTagSelectors(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var query = "update key_mapping\n" +
                "set pk = (" +
                "  select tag_pk from tag\n" +
                "  where tag.tenant_id = ?\n" +
                "  and tag.definition_fk = key_mapping.fk\n" +
                "  and (" +
                "    (key_mapping.ver is not null and tag.tag_version = key_mapping.ver) or\n" +
                "    (key_mapping.as_of is not null and tag.tag_timestamp <= key_mapping.as_of and\n" +
                "    (tag.tag_superseded is null or tag.tag_superseded > key_mapping.as_of)) or\n" +
                "    (key_mapping.is_latest is not null and tag.tag_is_latest = key_mapping.is_latest)))\n" +
                "where mapping_stage = ?";

        query = query.replaceAll("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY mapTagSelectors: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.executeUpdate();
        }
    }


    // Insert FK + version, can map to definition or tag PK more directly than using selectors

    private int insertFkAndVersionForMapping(Connection conn, long[] fk, int[] version) throws SQLException {

        var query = "insert into key_mapping (fk, ver, mapping_stage, ordering)\n" +
                "values (?, ?, ?, ?)";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY insertFkAndVersionForMapping: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            var mappingStage = nextMappingStage();

            for (var i = 0; i < fk.length; i++) {

                stmt.clearParameters();

                stmt.setLong(1, fk[i]);
                stmt.setInt(2, version[i]);
                stmt.setInt(3, mappingStage);
                stmt.setInt(4, i);

                stmt.addBatch();
            }

            stmt.executeBatch();

            return mappingStage;
        }
    }

    private void mapDefinitionByVersion(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var query =
                "update key_mapping\n" +
                "set pk = (" +
                "  select definition_pk from object_definition def\n" +
                "  where def.tenant_id = ?\n" +
                "  and def.object_fk = key_mapping.fk\n" +
                "  and def.object_version = key_mapping.ver)\n" +
                "where mapping_stage = ?";

        query = query.replaceAll("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY mapDefinitionByVersion: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query))  {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.execute();
        }
    }

    private void mapTagByVersion(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var query = "update key_mapping\n" +
                "set pk = (\n" +
                "  select tag_pk from tag\n" +
                "  where tag.tenant_id = ?\n" +
                "  and tag.definition_fk = key_mapping.fk\n" +
                "  and tag.tag_version = key_mapping.ver)\n" +
                "where mapping_stage = ?";

        query = query.replaceAll("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY mapTagByVersion: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query))  {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.execute();
        }
    }


    // Map definition keys from tag PKs, used during a search operation

    private void mapDefinitionByTagPk(Connection conn, short tenantId, int mappingStage) throws SQLException {

        // Use this mapping to prepare for fetchTagHeaders()

        // Tag key is already in key_mapping.pk
        // Put definition key into key_mapping.fk
        // Also put tag version into key_mapping.ver

        var query = "update key_mapping\n" +
                "set fk = (\n" +
                "  select definition_fk from tag t1\n" +
                "  where t1.tenant_id = ?\n" +
                "  and t1.tag_pk = key_mapping.pk),\n" +
                "ver = (\n" +
                "  select tag_version from tag t2\n" +
                "  where t2.tenant_id = ?\n" +
                "  and t2.tag_pk = key_mapping.pk)\n" +
                "where mapping_stage = ?";

        query = query.replaceAll("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY mapDefinitionByTagPk: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setShort(2, tenantId);
            stmt.setInt(3, mappingStage);

            stmt.execute();
        }
    }

    private int insertConfigEntries(Connection conn, ConfigEntry[] configEntry, Instant[] configTimestamps) throws SQLException {

        // Method does not insert config as_of, which is not currently used

        var query =
                "insert into key_mapping (\n" +
                "  config_class, config_key,\n" +
                "  ver, as_of, is_latest,\n" +
                "  mapping_stage, ordering)\n" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        query = query.replaceFirst("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY insertConfigEntries: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query)) {

            var mappingStage = nextMappingStage();

            for (var i = 0; i < configEntry.length; i++) {

                var configTimestamp = configTimestamps != null ? configTimestamps[i] : null;
                var sqlTimestamp = configTimestamp != null ? java.sql.Timestamp.from(configTimestamp) : null;

                stmt.clearParameters();

                stmt.setString(1, configEntry[i].getConfigClass());
                stmt.setString(2 ,configEntry[i].getConfigKey());

                if (configEntry[i].getConfigVersion() > 0)
                    stmt.setInt(3, configEntry[i].getConfigVersion());
                else
                    stmt.setNull(3, Types.INTEGER);

                if (sqlTimestamp != null)
                    stmt.setTimestamp(4, sqlTimestamp);
                else
                    stmt.setNull(4, Types.TIMESTAMP);

                if (configEntry[i].getIsLatestConfig())
                    stmt.setBoolean(5, configEntry[i].getIsLatestConfig());
                else
                    stmt.setNull(5, Types.BOOLEAN);

                stmt.setInt(6, mappingStage);
                stmt.setInt(7, i);

                stmt.addBatch();
            }

            stmt.executeBatch();

            return mappingStage;
        }
    }

    private void mapConfigEntries(Connection conn, short tenantId, int mappingStage, boolean includeDeleted) throws SQLException {

        // Match all criteria specified in the config entry (not known ahead of time)

        var query = "update key_mapping\n" +
                "set pk = (\n" +
                "  select config_pk from config_entry\n" +
                "  where config_entry.tenant_id = ?\n" +
                "  and config_entry.config_class = key_mapping.config_class\n" +
                "  and config_entry.config_key = key_mapping.config_key\n" +
                "  and (\n" +

                // Do not return anything if no criteria are specified
                "    (key_mapping.ver is not null or key_mapping.as_of is not null or key_mapping.is_latest is not null) and\n" +

                // Match on version
                "    (key_mapping.ver is null or config_entry.config_version = key_mapping.ver) and\n" +

                // Match on as-of
                "    (key_mapping.as_of is null or (config_entry.config_timestamp <= key_mapping.as_of and\n" +
                "    (config_entry.config_superseded is null or config_entry.config_superseded > key_mapping.as_of))) and\n" +

                // Match on latest
                "    (key_mapping.is_latest is null or config_entry.config_is_latest = key_mapping.is_latest))\n" +

                // Whether to filter deleted items
                (includeDeleted ? ")\n" : "and config_entry.config_deleted = ?)\n") +

                "where mapping_stage = ?";

        query = query.replaceAll("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY mapConfigEntries: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query))  {

            int nextParam = 1;
            stmt.setShort(nextParam++, tenantId);

            if (!includeDeleted)
                stmt.setBoolean(nextParam++, false);

            stmt.setInt(nextParam, mappingStage);
            stmt.execute();
        }
    }

    private void mapConfigEntriesByVersion(Connection conn, short tenantId, int mappingStage, boolean includeDeleted) throws SQLException {

        // Specialization for mapping by version (used in updates)

        var query = "update key_mapping\n" +
                "set pk = (\n" +
                "  select config_pk from config_entry\n" +
                "  where config_entry.tenant_id = ?\n" +
                "  and config_entry.config_class = key_mapping.config_class\n" +
                "  and config_entry.config_key = key_mapping.config_key\n" +
                "  and config_entry.config_version = key_mapping.ver\n" +

                // Whether to filter deleted items
                (includeDeleted ? ")\n" : "and config_entry.config_deleted = ?)\n") +

                "where mapping_stage = ?";

        query = query.replaceAll("key_mapping", JdbcDialects.mappingTableName(dialect));

        if (log.isDebugEnabled()) {
            log.debug("QUERY mapConfigEntriesByVersion: \n{}", query);
        }

        try (var stmt = conn.prepareStatement(query))  {

            int nextParam = 1;
            stmt.setShort(nextParam++, tenantId);

            if (!includeDeleted)
                stmt.setBoolean(nextParam++, false);

            stmt.setInt(nextParam, mappingStage);
            stmt.execute();
        }
    }

    private int nextMappingStage() {

        return mappingStage.incrementAndGet();
    }
}
