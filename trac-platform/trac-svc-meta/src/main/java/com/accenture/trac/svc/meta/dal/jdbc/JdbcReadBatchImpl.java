package com.accenture.trac.svc.meta.dal.jdbc;

import com.accenture.trac.common.metadata.*;
import com.google.protobuf.InvalidProtocolBufferException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;


class JdbcReadBatchImpl {

    private final AtomicInteger mappingStage;

    JdbcReadBatchImpl() {
        mappingStage = new AtomicInteger();
    }

    JdbcBaseDal.KeyedItems<ObjectType>
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

                return new JdbcBaseDal.KeyedItems<>(keys, types);
            }
        }
    }

    JdbcBaseDal.KeyedItems<ObjectDefinition>
    readDefinitionByVersion(
            Connection conn, short tenantId,
            ObjectType[] objectType, long[] objectFk, int[] objectVersion)
            throws SQLException {

        var mappingStage = insertFkAndVersionForMapping(conn, objectFk, objectVersion);
        mapDefinitionByVersion(conn, tenantId, mappingStage);

        return fetchDefinition(conn, tenantId, objectType, mappingStage);
    }

    JdbcBaseDal.KeyedItems<ObjectDefinition>
    readDefinitionByLatest(
            Connection conn, short tenantId,
            ObjectType[] objectType, long[] objectFk)
            throws SQLException {

        var mappingStage = insertFkForMapping(conn, objectFk);
        mapDefinitionByLatest(conn, tenantId, mappingStage);

        return fetchDefinition(conn, tenantId, objectType, mappingStage);
    }

    private JdbcBaseDal.KeyedItems<ObjectDefinition>
    fetchDefinition(
            Connection conn, short tenantId,
            ObjectType[] objectType, int mappingStage)
            throws SQLException {

        var query =
                "select definition_pk, object_version, definition\n" +
                "from object_definition def\n" +
                "join key_mapping km\n" +
                "  on def.definition_pk = km.pk\n" +
                "where def.tenant_id = ?\n" +
                "  and km.mapping_stage = ?\n" +
                "order by km.ordering";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            try (var rs = stmt.executeQuery()) {

                long[] pks = new long[objectType.length];
                int[] versions = new int[objectType.length];
                ObjectDefinition[] defs = new ObjectDefinition[objectType.length];

                for (var i = 0; i < objectType.length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA);

                    var defPk = rs.getLong(1);
                    var defVersion = rs.getInt(2);
                    var defEncoded = rs.getBytes(3);
                    var defDecoded = ObjectDefinition.parseFrom(defEncoded);

                    // TODO: Encode / decode helper, type = protobuf | json ?

                    pks[i] = defPk;
                    versions[i] = defVersion;
                    defs[i] = defDecoded;
                }

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                return new JdbcBaseDal.KeyedItems<>(pks, versions, defs);
            }
            catch (InvalidProtocolBufferException e) {
                throw new JdbcException(JdbcErrorCode.INVALID_OBJECT_DEFINITION);
            }

        }
    }

    JdbcBaseDal.KeyedItems<Tag.Builder>
    readTagByVersion(Connection conn, short tenantId, long[] definitionFk, int[] tagVersion) throws SQLException {

        var mappingStage = insertFkAndVersionForMapping(conn, definitionFk, tagVersion);
        mapTagByVersion(conn, tenantId, mappingStage);

        var tags = fetchTagRecord(conn, tenantId, definitionFk.length, mappingStage);
        var attrs = fetchTagAttrs(conn, tenantId, definitionFk.length, mappingStage);

        return applyTagAttrs(tags, attrs);
    }

    JdbcBaseDal.KeyedItems<Tag.Builder>
    readTagByLatest(Connection conn, short tenantId, long[] definitionFk) throws SQLException {

        var mappingStage = insertFkForMapping(conn, definitionFk);
        mapTagByLatest(conn, tenantId, mappingStage);

        var tags = fetchTagRecord(conn, tenantId, definitionFk.length, mappingStage);
        var attrs = fetchTagAttrs(conn, tenantId, definitionFk.length, mappingStage);

        return applyTagAttrs(tags, attrs);
    }

    JdbcBaseDal.KeyedItems<Tag.Builder>
    readTagByPk(Connection conn, short tenantId, long[] tagPk) throws SQLException {

        var mappingStage = insertPk(conn, tagPk);

        var tags = fetchTagRecord(conn, tenantId, tagPk.length, mappingStage);
        var attrs = fetchTagAttrs(conn, tenantId, tagPk.length, mappingStage);

        return applyTagAttrs(tags, attrs);
    }

    private JdbcBaseDal.KeyedItems<Tag.Builder>
    fetchTagRecord(Connection conn, short tenantId, int length, int mappingStage) throws SQLException {

        // Tag records contain no attributes, we only need pks and versions
        // Note: Common attributes may be added to the tag table as search optimisations, but do not need to be read

        var query =
                "select tag.tag_pk, tag.tag_version\n" +
                "from tag\n" +
                "join key_mapping km\n" +
                "  on tag.tag_pk = km.pk\n" +
                "where tag.tenant_id = ?\n" +
                "  and km.mapping_stage = ?\n" +
                "order by km.ordering";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            try (var rs = stmt.executeQuery()) {

                long[] pks = new long[length];
                int[] versions = new int[length];
                Tag.Builder[] tags = new Tag.Builder[length];

                for (var i = 0; i < length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA);

                    var tagPk = rs.getLong(1);
                    var tagVersion = rs.getInt(2);

                    pks[i] = tagPk;
                    versions[i] = tagVersion;
                    tags[i] = Tag.newBuilder().setTagVersion(tagVersion);
                }

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                return new JdbcBaseDal.KeyedItems<>(pks, versions, tags);
            }
        }
    }

    Map<String, Value>[]
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
                if (nTags > 0)
                    result[currentTagIndex] = currentTagAttrs;

                return result;
            }
        }
    }

    private JdbcBaseDal.KeyedItems<Tag.Builder>
    applyTagAttrs(JdbcBaseDal.KeyedItems<Tag.Builder> tags, Map<String, Value>[] attrs) {

        for (var i = 0; i < tags.items.length; i++)
            tags.items[i].putAllAttr(attrs[i]);

        return tags;
    }

    JdbcBaseDal.KeyedItems<ObjectHeader>
    readHeaderByTagPk(Connection conn, short tenantId, long[] tagPk) throws SQLException {

        var query = "select \n" +
                "  def.definition_pk,\n" +
                "  def.object_version,\n" +
                "  obj.object_id_hi,\n" +
                "  obj.object_id_lo,\n" +
                "  obj.object_type\n" +
                "from key_mapping km\n" +
                "join object_definition def\n" +
                "  on def.definition_pk = km.pk\n" +
                "join object_id obj\n" +
                "  on obj.tenant_id = def.tenant_id\n" +
                "  and obj.object_pk = def.object_fk\n" +
                "where def.tenant_id = ?\n" +
                "  and km.mapping_stage = ?\n" +
                "order by km.ordering";

        var mappingStage = insertFkForMapping(conn, tagPk);
        mapDefinitionByTagPk(conn, tenantId, mappingStage);

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            try (var rs = stmt.executeQuery()) {

                var pks = new long[tagPk.length];
                var headers = new ObjectHeader[tagPk.length];

                for (int i = 0; i < tagPk.length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA);

                    var defPk = rs.getLong(1);
                    var objectVersion = rs.getInt(2);
                    var objectIdHi = rs.getLong(3);
                    var objectIdLo = rs.getLong(4);
                    var objectTypeCode = rs.getString(5);

                    var objectId = new UUID(objectIdHi, objectIdLo);
                    var objectType = ObjectType.valueOf(objectTypeCode);

                    var header = ObjectHeader.newBuilder()
                            .setObjectId(MetadataCodec.encode(objectId))
                            .setObjectVersion(objectVersion)
                            .setObjectType(objectType)
                            .build();

                    pks[i] = defPk;
                    headers[i] = header;
                }

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                return new JdbcBaseDal.KeyedItems<>(pks, headers);
            }
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // KEY LOOKUP FUNCTIONS
    // -----------------------------------------------------------------------------------------------------------------

    long[] lookupDefinitionPk(Connection conn, short tenantId, long[] objectPk, int[] version) throws SQLException {

        var mappingStage = insertFkAndVersionForMapping(conn, objectPk, version);
        mapDefinitionByVersion(conn, tenantId, mappingStage);

        return fetchMappedPk(conn, mappingStage, objectPk.length);
    }

    private long[] fetchMappedPk(Connection conn, int mappingStage, int length) throws SQLException {

        var query =
                "select pk from key_mapping\n" +
                "where mapping_stage = ?\n" +
                "order by ordering";

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

    private int insertIdForMapping(Connection conn, UUID[] ids) throws SQLException {

        var insertQuery =
                "insert into key_mapping (id_hi, id_lo, mapping_stage, ordering)\n" +
                "values (?, ?, ?, ?)";

        try (var stmt = conn.prepareStatement(insertQuery)) {

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

    private int insertPk(Connection conn, long[] pks) throws SQLException {

        var query =
                "insert into key_mapping (pk, mapping_stage, ordering)\n" +
                "values (?, ?, ?)";

        try (var stmt = conn.prepareStatement(query)) {
            return insertKeysForMapping(stmt, pks);
        }
    }

    private int insertFkForMapping(Connection conn, long[] fks) throws SQLException {

        var query =
                "insert into key_mapping (fk, mapping_stage, ordering)\n" +
                "values (?, ?, ?)";

        try (var stmt = conn.prepareStatement(query)) {
            return insertKeysForMapping(stmt, fks);
        }
    }

    private int insertKeysForMapping(PreparedStatement stmt, long[] keys) throws SQLException {

        var mappingStage = nextMappingStage();

        for (var i = 0; i < keys.length; i++) {

            stmt.clearParameters();

            stmt.setLong(1, keys[i]);
            stmt.setInt(2, mappingStage);
            stmt.setInt(3, i);

            stmt.addBatch();
        }

        stmt.executeBatch();

        return mappingStage;
    }

    private int insertFkAndVersionForMapping(Connection conn, long[] fk, int[] version) throws SQLException {

        var insertQuery =
                "insert into key_mapping (fk, ver, mapping_stage, ordering)\n" +
                "values (?, ?, ?, ?)";

        try (var stmt = conn.prepareStatement(insertQuery)) {

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

    private void mapObjectById(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var mapQuery =
                "update key_mapping\n" +
                "set key_mapping.pk = (" +
                "  select object_pk from object_id oid\n" +
                "  where oid.tenant_id = ?\n" +
                "  and oid.object_id_hi = key_mapping.id_hi\n" +
                "  and oid.object_id_lo = key_mapping.id_lo)\n" +
                "where mapping_stage = ?";

        try (var stmt = conn.prepareStatement(mapQuery))  {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.execute();
        }
    }

    private void mapDefinitionByVersion(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var mapQuery =
                "update key_mapping\n" +
                "set key_mapping.pk = (" +
                "  select definition_pk from object_definition def\n" +
                "  where def.tenant_id = ?\n" +
                "  and def.object_fk = key_mapping.fk\n" +
                "  and def.object_version = key_mapping.ver)\n" +
                "where mapping_stage = ?";

        try (var stmt = conn.prepareStatement(mapQuery))  {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.execute();
        }
    }

    private void mapDefinitionByLatest(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var mappingQuery =
                "update key_mapping\n" +
                "set key_mapping.pk = (\n" +
                "  select lv.latest_definition_pk\n" +
                "  from latest_version lv\n" +
                "  where lv.tenant_id = ?\n" +
                "  and lv.object_fk = key_mapping.fk)\n" +
                "where mapping_stage = ?";

        try (var stmt = conn.prepareStatement(mappingQuery))  {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.execute();
        }
    }

    private void mapDefinitionByTagPk(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var mappingQuery =
                "update key_mapping\n" +
                "set key_mapping.pk = (\n" +
                "  select definition_fk from tag t\n" +
                "  where t.tenant_id = ?\n" +
                "  and t.tag_pk = key_mapping.fk)\n" +
                "where mapping_stage = ?";

        try (var stmt = conn.prepareStatement(mappingQuery)) {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.execute();
        }
    }

    private void mapTagByVersion(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var mappingQuery =
                "update key_mapping\n" +
                "set key_mapping.pk = (\n" +
                "  select tag_pk from tag\n" +
                "  where tag.tenant_id = ?\n" +
                "  and tag.definition_fk = key_mapping.fk\n" +
                "  and tag.tag_version = key_mapping.ver)\n" +
                "where mapping_stage = ?";

        try (var stmt = conn.prepareStatement(mappingQuery))  {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.execute();
        }
    }

    private void mapTagByLatest(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var mappingQuery =
                "update key_mapping\n" +
                "set key_mapping.pk = (\n" +
                "  select lt.latest_tag_pk\n" +
                "  from latest_tag lt\n" +
                "  where lt.tenant_id = ?\n" +
                "  and lt.definition_fk = key_mapping.fk)\n" +
                "where mapping_stage = ?";

        try (var stmt = conn.prepareStatement(mappingQuery))  {

            stmt.setShort(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.execute();
        }
    }

    private int nextMappingStage() {

        return mappingStage.incrementAndGet();
    }
}
