package trac.svc.meta.dal.jdbc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import trac.common.metadata.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;


class JdbcReadBatchImpl {

    private AtomicInteger mappingStage;

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
                        throw new JdbcException(JdbcErrorCode.NO_DATA.name(), JdbcErrorCode.NO_DATA);

                    var pk = rs.getLong(1);
                    var objectTypeCode = rs.getString(2);
                    var objectType = ObjectType.valueOf(objectTypeCode);

                    keys[i] = pk;
                    types[i] = objectType;
                }

                if (!rs.last())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS.name(), JdbcErrorCode.TOO_MANY_ROWS);

                return new JdbcBaseDal.KeyedItems<>(keys, types);
            }
        }
    }

    JdbcBaseDal.KeyedItems<MessageLite>
    readDefinitionByVersion(
            Connection conn, short tenantId,
            ObjectType[] objectType, long[] objectFk, int[] objectVersion)
            throws SQLException {

        var mappingStage = insertFkAndVersionForMapping(conn, objectFk, objectVersion);
        mapDefinitionByVersion(conn, tenantId, mappingStage);

        var query =
                "select definition_pk, definition\n" +
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

                long[] pks = new long[objectFk.length];
                MessageLite[] defs = new MessageLite[objectFk.length];

                for (var i = 0; i < objectFk.length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA.name(), JdbcErrorCode.NO_DATA);

                    var defPk = rs.getLong(1);
                    var defEncoded = rs.getBytes(2);
                    var defDecoded = MetadataCodec.decode(objectType[i], defEncoded);

                    // TODO: Encode / decode helper, type = protobuf | json ?

                    pks[i] = defPk;
                    defs[i] = defDecoded;
                }

                if (!rs.last())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS.name(), JdbcErrorCode.TOO_MANY_ROWS);

                return new JdbcBaseDal.KeyedItems<>(pks, defs);
            }
            catch (InvalidProtocolBufferException e) {
                throw new JdbcException(JdbcErrorCode.INVALID_OBJECT_DEFINITION.name(), JdbcErrorCode.INVALID_OBJECT_DEFINITION);
            }

        }
    }

    JdbcBaseDal.KeyedItems<Tag.Builder>
    readTagRecordByVersion(Connection conn, short tenantId, long[] definitionFk, int[] tagVersion) throws SQLException {

        // Tag records contain no attributes, we only need pks and versions
        // Note: Common attributes may be added to the tag table as search optimisations, but do not need to be read

        var pks = lookupTagPk(conn, tenantId, definitionFk, tagVersion);
        var stubs = Arrays.stream(tagVersion).mapToObj(v -> Tag.newBuilder().setTagVersion(v)).toArray(Tag.Builder[]::new);

        return new JdbcBaseDal.KeyedItems<>(pks, stubs);
    }

    Map<String, PrimitiveValue>[]
    readTagAttrs(Connection conn, short tenantId, long[] tagPk) throws SQLException {

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
                "order by km.ordering";

        var mappingStage = insertPk(conn, tagPk);

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setInt(1, tenantId);
            stmt.setInt(2, mappingStage);

            try (var rs = stmt.executeQuery()) {

                @SuppressWarnings("unchecked")
                var result = (Map<String, PrimitiveValue>[]) new HashMap[tagPk.length];

                // Start by storing attrs for tag index = 0
                var currentAttrs = new HashMap<String, PrimitiveValue>();
                var currentIndex = 0;

                while (rs.next()) {

                    var tagIndex = rs.getInt("tag_index");
                    var attrName = rs.getString("attr_name");
                    var attrValue = JdbcReadHelpers.readAttrValue(rs);

                    // Check if the current tag index has moved on
                    // If so store accumulated attrs for the previous index
                    while (currentIndex != tagIndex && currentIndex < tagPk.length) {
                        result[currentIndex] = currentAttrs;
                        currentAttrs = new HashMap<>();
                        currentIndex++;
                    }

                    if (currentIndex == tagPk.length)
                        throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS.name(), JdbcErrorCode.TOO_MANY_ROWS);

                    // Accumulate attr against the current tag index
                    currentAttrs.put(attrName, attrValue);
                }

                // Store accumulated attrs for the final tag index
                result[currentIndex] = currentAttrs;

                return result;
            }
        }
    }



    // -----------------------------------------------------------------------------------------------------------------
    // KEY LOOKUP FUNCTIONS
    // -----------------------------------------------------------------------------------------------------------------

    long[] lookupObjectPk(Connection conn, short tenantId, UUID[] objectId) throws SQLException {

        int mappingStage = insertIdForMapping(conn, objectId);
        mapObjectById(conn, tenantId, mappingStage);

        return fetchMappedPk(conn, mappingStage, objectId.length);
    }

    long[] lookupDefinitionPk(Connection conn, short tenantId, long[] objectPk, int[] version) throws SQLException {

        var mappingStage = insertFkAndVersionForMapping(conn, objectPk, version);
        mapDefinitionByVersion(conn, tenantId, mappingStage);

        return fetchMappedPk(conn, mappingStage, objectPk.length);
    }

    long[] lookupTagPk(Connection conn, short tenantId, long[] definitionPk, int[] tagVersion) throws SQLException {

        var mappingStage = insertFkAndVersionForMapping(conn, definitionPk, tagVersion);
        mapTagByVersion(conn, tenantId, mappingStage);

        return fetchMappedPk(conn, mappingStage, definitionPk.length);
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
                        throw new JdbcException(JdbcErrorCode.NO_DATA.name(), JdbcErrorCode.NO_DATA);

                    keys[i] = rs.getLong(1);

                    if (rs.wasNull())
                        throw new JdbcException(JdbcErrorCode.NO_DATA.name(), JdbcErrorCode.NO_DATA);
                }

                if (!rs.last())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS.name(), JdbcErrorCode.TOO_MANY_ROWS);

                return keys;
            }
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // KEY MAPPING FUNCTIONS
    // -----------------------------------------------------------------------------------------------------------------

    private int insertPk(Connection conn, long[] pks) throws SQLException {

        var query =
                "insert into key_mapping (pk, mapping_stage, ordering)\n" +
                "values (?, ?, ?)";

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
                "update key_mapping set\n" +
                "key_mapping.pk = (select object_pk from object_id oid\n" +
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
                "update key_mapping set\n" +
                "key_mapping.pk = (select definition_pk from object_definition def\n" +
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

    private void mapTagByVersion(Connection conn, short tenantId, int mappingStage) throws SQLException {

        var mappingQuery =
                "update key_mapping set\n" +
                "key_mapping.pk = (select tag_pk from tag\n" +
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

    private int nextMappingStage() {

        return mappingStage.incrementAndGet();
    }
}
