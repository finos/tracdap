package trac.svc.meta.dal.jdbc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import trac.common.metadata.*;

import trac.svc.meta.dal.jdbc.JdbcBaseDal.KeyedItem;
import trac.svc.meta.exception.CorruptItemError;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


class JdbcReadImpl {

    KeyedItem<ObjectType>
    readObjectTypeById(Connection conn, short tenantId, UUID objectId) throws SQLException {

        var query =
                "select object_pk, object_type\n" +
                "from object_id\n" +
                "where tenant_id = ?\n" +
                "and object_id_hi = ?\n" +
                "and object_id_lo = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, objectId.getMostSignificantBits());
            stmt.setLong(3, objectId.getLeastSignificantBits());

            try (var rs = stmt.executeQuery()) {

                if (!rs.next())
                    throw new JdbcException(JdbcErrorCode.NO_DATA.name(), JdbcErrorCode.NO_DATA);

                var objectPk = rs.getLong(1);
                var objectTypeCode = rs.getString(2);
                var objectType = ObjectType.valueOf(objectTypeCode);

                if (!rs.last())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS.name(), JdbcErrorCode.TOO_MANY_ROWS);

                return new KeyedItem<>(objectPk, objectType);
            }
        }
    }

    KeyedItem<MessageLite>
    readDefinitionByVersion(
            Connection conn, short tenantId,
            ObjectType objectType, long objectPk, int objectVersion)
            throws SQLException {

        var query =
                "select definition_pk, definition\n" +
                "from object_definition\n" +
                "where tenant_id = ?\n" +
                "and object_fk = ?\n" +
                "and object_version = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, objectPk);
            stmt.setInt(3, objectVersion);

            try (var rs = stmt.executeQuery()) {

                if (!rs.next())
                    throw new JdbcException(JdbcErrorCode.NO_DATA.name(), JdbcErrorCode.NO_DATA);

                var defPk = rs.getLong(1);
                var defEncoded = rs.getBytes(2);
                var defDecoded = MetadataCodec.decode(objectType, defEncoded);

                if (!rs.last())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS.name(), JdbcErrorCode.TOO_MANY_ROWS);

                return new KeyedItem<>(defPk, defDecoded);
            }
            catch (InvalidProtocolBufferException e) {
                throw new CorruptItemError("Metadata decode failed", e);   // TODO: Error message + log
            }
        }
    }

    KeyedItem<JdbcBaseDal.VersionedItem<MessageLite>>
    readDefinitionByLatest(
            Connection conn, short tenantId,
            ObjectType objectType, long objectPk)
            throws SQLException {

        var query =
                "select definition_pk, object_version, definition\n" +
                "from object_definition\n" +
                "where tenant_id = ?\n" +
                "and object_fk = ?\n" +
                "and object_version = (\n" +
                "  select latest_version\n" +
                "  from latest_version\n" +
                "  where latest_version.tenant_id = object_definition.tenant_id\n" +
                "  and latest_version.object_fk = object_definition.object_fk\n" +
                ")";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, objectPk);

            try (var rs = stmt.executeQuery()) {

                if (!rs.next())
                    throw new JdbcException(JdbcErrorCode.NO_DATA.name(), JdbcErrorCode.NO_DATA);

                var defPk = rs.getLong(1);
                var version = rs.getInt(2);
                var defEncoded = rs.getBytes(3);
                var defDecoded = MetadataCodec.decode(objectType, defEncoded);
                var defVersioned = new JdbcMetadataDal.VersionedItem<>(defDecoded, version);

                // TODO: Encode / decode helper, type = protobuf | json ?

                if (!rs.last())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS.name(), JdbcErrorCode.TOO_MANY_ROWS);

                return new KeyedItem<>(defPk, defVersioned);
            }
            catch (InvalidProtocolBufferException e) {
                throw new CorruptItemError("Metadata decode failed", e);   // TODO: Error message + log
            }
        }
    }

    KeyedItem<Tag.Builder>
    readTagRecordByVersion(Connection conn, short tenantId, long definitionPk, int tagVersion) throws SQLException {

        var query =
                "select tag_pk from tag\n" +
                "where tenant_id = ?\n" +
                "and definition_fk = ?\n" +
                "and tag_version = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, definitionPk);
            stmt.setInt(3, tagVersion);

            try (var rs = stmt.executeQuery()) {

                if (!rs.next())
                    throw new JdbcException(JdbcErrorCode.NO_DATA.name(), JdbcErrorCode.NO_DATA);

                var tagPk = rs.getLong(1);
                var tagStub = Tag.newBuilder().setTagVersion(tagVersion);

                if (!rs.last())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS.name(), JdbcErrorCode.TOO_MANY_ROWS);

                return new KeyedItem<>(tagPk, tagStub);
            }
        }
    }

    KeyedItem<Tag.Builder>
    readTagRecordByLatest(Connection conn, short tenantId, long definitionPk) throws SQLException {

        var query =
                "select tag_pk, tag_version from tag\n" +
                "where tenant_id = ?\n" +
                "and definition_fk = ?\n" +
                "and tag_version = (\n" +
                "  select latest_tag\n" +
                "  from latest_tag\n" +
                "  where latest_tag.tenant_id = tag.tenant_id\n" +
                "  and latest_tag.definition_fk = tag.definition_fk\n" +
                ")";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, definitionPk);

            try (var rs = stmt.executeQuery()) {

                if (!rs.next())
                    throw new JdbcException(JdbcErrorCode.NO_DATA.name(), JdbcErrorCode.NO_DATA);

                var tagPk = rs.getLong(1);
                var tagVersion = rs.getInt(2);
                var tagStub = Tag.newBuilder().setTagVersion(tagVersion);

                if (!rs.last())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS.name(), JdbcErrorCode.TOO_MANY_ROWS);

                return new KeyedItem<>(tagPk, tagStub);
            }
        }
    }

    Map<String, PrimitiveValue>
    readTagAttrs(Connection conn, short tenantId, long tagPk) throws SQLException {

        var query =
                "select * from tag_attr\n" +
                "where tenant_id = ?\n" +
                "and tag_fk = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, tagPk);

            try (var rs = stmt.executeQuery()) {

                var attrs = new HashMap<String, PrimitiveValue>();

                while (rs.next()) {

                    var attrName = rs.getString("attr_name");
                    var attrValue = JdbcReadHelpers.readAttrValue(rs);

                    attrs.put(attrName, attrValue);
                }

                return attrs;
            }
        }
    }
}
