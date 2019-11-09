package trac.svc.meta.dal.jdbc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import trac.common.metadata.*;

import trac.svc.meta.dal.jdbc.JdbcBaseDal.KeyedItem;
import trac.svc.meta.exception.CorruptItemError;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;


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

                rs.next();

                var objectPk = rs.getLong(1);
                var objectTypeCode = rs.getString(2);
                var objectType = ObjectType.valueOf(objectTypeCode);

                return new KeyedItem<>(objectPk, objectType);
            }
        }
    }

    KeyedItem<MessageLite>
    readDefinitionByVersion(Connection conn, short tenantId, long objectPk, int objectVersion) throws SQLException {

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

                rs.next();

                var defPk = rs.getLong(1);
                var defEncoded = rs.getBytes(2);
                var defDecoded = DataDefinition.parseFrom(defEncoded);

                return new KeyedItem<>(defPk, defDecoded);
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

                rs.next();

                var tagPk = rs.getLong(1);
                var tagStub = Tag.newBuilder().setTagVersion(tagVersion);

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

                    fetchAttrValue(attrs, attrName, ResultSet::getString, rs, "attr_value_string", PrimitiveType.STRING, Function.identity());

                }

                return attrs;
            }
        }
    }

    private <TSqlValue, TValue>
    boolean fetchAttrValue(
            Map<String, PrimitiveValue> attrs, String attrName,
            AttrGetter<TSqlValue> getter, ResultSet rs, String fieldName,
            PrimitiveType primitiveType,
            Function<TSqlValue, TValue> mapper)
            throws SQLException {

        var sqlValue = getter.get(rs, fieldName);

        if (!rs.wasNull()) {
            var attrValue = mapper.apply(sqlValue);
            var primitiveValue = PrimitiveValue.newBuilder()
                    .setType(primitiveType)
                    .setStringValue(attrValue.toString())
                    .build();
            attrs.put(attrName, primitiveValue);

            return true;
        }

        return false;
    }

    @FunctionalInterface
    private interface AttrGetter<TSqlValue> {

        TSqlValue get(ResultSet rs, String fieldName) throws SQLException;
    }
}
