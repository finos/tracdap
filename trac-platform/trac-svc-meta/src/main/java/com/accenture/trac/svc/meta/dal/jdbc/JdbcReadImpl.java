package com.accenture.trac.svc.meta.dal.jdbc;

import com.accenture.trac.svc.meta.dal.jdbc.JdbcBaseDal.KeyedItem;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.common.metadata.PrimitiveValue;
import com.accenture.trac.common.metadata.Tag;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

import java.sql.Connection;
import java.sql.PreparedStatement;
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
                    throw new JdbcException(JdbcErrorCode.NO_DATA);

                var objectPk = rs.getLong(1);
                var objectTypeCode = rs.getString(2);
                var objectType = ObjectType.valueOf(objectTypeCode);

                if (!rs.last())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

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
                "select definition_pk, object_version, definition\n" +
                "from object_definition\n" +
                "where tenant_id = ?\n" +
                "and object_fk = ?\n" +
                "and object_version = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, objectPk);
            stmt.setInt(3, objectVersion);

            return readDefinition(stmt, objectType);
        }
    }

    KeyedItem<MessageLite>
    readDefinitionByLatest(
            Connection conn, short tenantId,
            ObjectType objectType, long objectPk)
            throws SQLException {

        var query =
                "select definition_pk, object_version, definition\n" +
                "from object_definition\n" +
                "where tenant_id = ?\n" +
                "and definition_pk = (\n" +
                "  select lv.latest_definition_pk\n" +
                "  from latest_version lv\n" +
                "  where lv.tenant_id = ?\n" +
                "  and lv.object_fk = ?\n" +
                ")";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setShort(2, tenantId);
            stmt.setLong(3, objectPk);

            return readDefinition(stmt, objectType);
        }
    }

    private KeyedItem<MessageLite>
    readDefinition(PreparedStatement stmt, ObjectType objectType) throws SQLException {

        try (var rs = stmt.executeQuery()) {

            if (!rs.next())
                throw new JdbcException(JdbcErrorCode.NO_DATA);

            var defPk = rs.getLong(1);
            var version = rs.getInt(2);
            var defEncoded = rs.getBytes(3);
            var defDecoded = MetadataCodec.decode(objectType, defEncoded);

            // TODO: Encode / decode helper, type = protobuf | json ?

            if (!rs.last())
                throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

            return new KeyedItem<>(defPk, version, defDecoded);
        }
        catch (InvalidProtocolBufferException e) {
            throw new JdbcException(JdbcErrorCode.INVALID_OBJECT_DEFINITION);
        }
    }

    KeyedItem<Tag.Builder>
    readTagRecordByVersion(Connection conn, short tenantId, long definitionPk, int tagVersion) throws SQLException {

        var query =
                "select tag_pk, tag_version from tag\n" +
                "where tenant_id = ?\n" +
                "and definition_fk = ?\n" +
                "and tag_version = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, definitionPk);
            stmt.setInt(3, tagVersion);

            return readTagRecord(stmt);
        }
    }

    KeyedItem<Tag.Builder>
    readTagRecordByLatest(Connection conn, short tenantId, long definitionPk) throws SQLException {

        var query =
                "select tag_pk, tag_version from tag\n" +
                "where tenant_id = ?\n" +
                "and tag_pk = (\n" +
                "  select lt.latest_tag_pk\n" +
                "  from latest_tag lt\n" +
                "  where lt.tenant_id = ?\n" +
                "  and lt.definition_fk = ?)";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setShort(2, tenantId);
            stmt.setLong(3, definitionPk);

            return readTagRecord(stmt);
        }
    }

    private KeyedItem<Tag.Builder>
    readTagRecord(PreparedStatement stmt) throws SQLException {

        try (var rs = stmt.executeQuery()) {

            if (!rs.next())
                throw new JdbcException(JdbcErrorCode.NO_DATA);

            var tagPk = rs.getLong(1);
            var tagVersion = rs.getInt(2);
            var tagStub = Tag.newBuilder().setTagVersion(tagVersion);

            if (!rs.last())
                throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

            return new KeyedItem<>(tagPk, tagVersion, tagStub);
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
