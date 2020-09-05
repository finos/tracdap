package com.accenture.trac.svc.meta.dal.jdbc;

import com.accenture.trac.common.metadata.*;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;


class JdbcWriteBatchImpl {

    long[] writeObjectId(Connection conn, short tenantId, ObjectType[] objectType, UUID[] objectId) throws SQLException {

        var query =
                "insert into object_id (\n" +
                "  tenant_id,\n" +
                "  object_type,\n" +
                "  object_id_hi,\n" +
                "  object_id_lo\n" +
                ")\n" +
                "values (?, ?, ?, ?)";

        try (var stmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            for (var i = 0; i < objectId.length; i++) {

                stmt.setShort(1, tenantId);
                stmt.setString(2, objectType[i].name());
                stmt.setLong(3, objectId[i].getMostSignificantBits());
                stmt.setLong(4, objectId[i].getLeastSignificantBits());

                stmt.addBatch();
            }

            stmt.executeBatch();

            return generatedKeys(stmt, objectId.length);
        }
    }

    long[] writeObjectDefinition(
            Connection conn, short tenantId,
            long[] objectPk, int[] objectVersion, ObjectDefinition[] definition)
            throws SQLException {

        var query =
                "insert into object_definition (\n" +
                "  tenant_id,\n" +
                "  object_fk,\n" +
                "  object_version,\n" +
                "  definition" +
                ")\n" +
                "values (?, ?, ?, ?)";

        try (var stmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            for (var i = 0; i < objectPk.length; i++) {

                stmt.setShort(1, tenantId);
                stmt.setLong(2, objectPk[i]);
                stmt.setInt(3, objectVersion[i]);
                stmt.setBytes(4, definition[i].toByteArray());

                stmt.addBatch();
            }

            stmt.executeBatch();

            return generatedKeys(stmt, objectPk.length);
        }
    }

    long[] writeTagRecord(
            Connection conn, short tenantId,
            long[] definitionPk, int[] tagVersion, ObjectType[] objectTypes)
            throws SQLException {

        var query =
                "insert into tag (\n" +
                "  tenant_id,\n" +
                "  definition_fk,\n" +
                "  tag_version,\n" +
                "  object_type" +
                ")\n" +
                "values (?, ?, ?, ?)";

        try (var stmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            for (var i = 0; i < definitionPk.length; i++) {

                stmt.setShort(1, tenantId);
                stmt.setLong(2, definitionPk[i]);
                stmt.setInt(3, tagVersion[i]);
                stmt.setString(4, objectTypes[i].name());

                stmt.addBatch();
            }

            stmt.executeBatch();

            return generatedKeys(stmt, definitionPk.length);
        }
    }

    void writeTagAttrs(
            Connection conn, short tenantId,
            long[] tagPk, Tag[] tag)
            throws SQLException {

        var query =
                "insert into tag_attr (\n" +
                "  tenant_id,\n" +
                "  tag_fk,\n" +
                "  attr_name,\n" +
                "  attr_type,\n" +
                "  attr_index,\n" +
                "  attr_value_boolean,\n" +
                "  attr_value_integer,\n" +
                "  attr_value_float,\n" +
                "  attr_value_string,\n" +
                "  attr_value_decimal,\n" +
                "  attr_value_date,\n" +
                "  attr_value_datetime\n" +
                ")\n" +
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (var stmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            for (var i = 0; i < tagPk.length; i++) {
                for (var attr : tag[i].getAttrMap().entrySet()) {

                    var attrRootValue = attr.getValue();
                    var attrType = attrBasicType(attrRootValue.getType());

                    // TODO: Constants for single / multi valued base index
                    var attrIndex = TypeSystem.isPrimitive(attrRootValue.getType()) ? -1 : 0;

                    for (var attrValue : attrValues(attrRootValue)) {

                        stmt.setShort(1, tenantId);
                        stmt.setLong(2, tagPk[i]);
                        stmt.setString(3, attr.getKey());
                        stmt.setString(4, attrType.name());
                        stmt.setInt(5, attrIndex);

                        stmt.setNull(6, Types.BOOLEAN);
                        stmt.setNull(7, Types.BIGINT);
                        stmt.setNull(8, Types.DOUBLE);
                        stmt.setNull(9, Types.VARCHAR);
                        stmt.setNull(10, Types.DECIMAL);
                        stmt.setNull(11, Types.DATE);
                        stmt.setNull(12, Types.TIMESTAMP);

                        var attrTypeToIndex = Map.ofEntries(
                                Map.entry(BasicType.BOOLEAN, 6),
                                Map.entry(BasicType.INTEGER, 7),
                                Map.entry(BasicType.FLOAT, 8),
                                Map.entry(BasicType.STRING, 9),
                                Map.entry(BasicType.DECIMAL, 10),
                                Map.entry(BasicType.DATE, 11),
                                Map.entry(BasicType.DATETIME, 12)
                        );

                        var paramIndex = attrTypeToIndex.get(attrType);
                        JdbcAttrHelpers.setAttrValue(stmt, paramIndex, attrType, attrValue);

                        stmt.addBatch();

                        attrIndex++;
                    }
                }
            }

            stmt.executeBatch();
        }
    }

    private BasicType attrBasicType(TypeDescriptor typeDescriptor) {

        if (TypeSystem.isPrimitive(typeDescriptor))
            return typeDescriptor.getBasicType();

        if (typeDescriptor.getBasicType() == BasicType.ARRAY && TypeSystem.isPrimitive(typeDescriptor.getArrayType()))
            return typeDescriptor.getArrayType().getBasicType();

        throw new RuntimeException("");  // TODO: Error type and message
    }

    private Iterable<Value> attrValues(Value rootValue) {

        if (TypeSystem.isPrimitive(rootValue.getType()))
            return Collections.singletonList(rootValue);

        return rootValue.getArrayValue().getItemList();
    }

    void writeLatestVersion(
            Connection conn, short tenantId,
            long[] objectFk, long[] definitionPk)
            throws SQLException {

        var query =
                "insert into latest_version (\n" +
                "  tenant_id,\n" +
                "  object_fk,\n" +
                "  latest_definition_pk\n" +
                ")\n" +
                "values (?, ?, ?)";

        try (var stmt = conn.prepareStatement(query)) {
            writeLatest(stmt, tenantId, objectFk, definitionPk);
        }
    }

    void writeLatestTag(
            Connection conn, short tenantId,
            long[] definitionFk, long[] tagPk)
            throws SQLException {

        var query =
                "insert into latest_tag (\n" +
                "  tenant_id,\n" +
                "  definition_fk,\n" +
                "  latest_tag_pk\n" +
                ")\n" +
                "values (?, ?, ?)";

        try (var stmt = conn.prepareStatement(query)) {
            writeLatest(stmt, tenantId, definitionFk, tagPk);
        }
    }

    private void writeLatest(
            PreparedStatement stmt, short tenantId,
            long[] fk, long[] pk)
            throws SQLException {

        for (int i = 0; i < pk.length; i++) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, fk[i]);
            stmt.setLong(3, pk[i]);

            stmt.addBatch();
        }

        stmt.executeBatch();
    }

    void updateLatestVersion(
            Connection conn, short tenantId,
            long[] objectFk, long[] definitionPk)
            throws SQLException {

        var query =
                "update latest_version set \n" +
                "  latest_definition_pk = ?\n" +
                "where tenant_id = ?\n" +
                "  and object_fk = ?";

        try (var stmt = conn.prepareStatement(query)) {
            updateLatest(stmt, tenantId, objectFk, definitionPk);
        }
    }

    void updateLatestTag(
            Connection conn, short tenantId,
            long[] definitionFk, long[] tagPk)
            throws SQLException {

        var query =
                "update latest_tag set \n" +
                "  latest_tag_pk = ?\n" +
                "where tenant_id = ?\n" +
                "  and definition_fk = ?";

        try (var stmt = conn.prepareStatement(query)) {
            updateLatest(stmt, tenantId, definitionFk, tagPk);
        }
    }

    private void updateLatest(
            PreparedStatement stmt, short tenantId,
            long[] fk, long[] pk)
            throws SQLException {

        for (int i = 0; i < fk.length; i++) {

            stmt.setLong(1, pk[i]);
            stmt.setShort(2, tenantId);
            stmt.setLong(3, fk[i]);

            stmt.addBatch();
        }

        int[] updates = stmt.executeBatch();

        // Updates fail silent if no records are matched, so make an explicit check
        if (Arrays.stream(updates).anyMatch(count -> count != 1))
            throw new JdbcException(JdbcErrorCode.INSERT_MISSING_FK);
    }

    private long[] generatedKeys(Statement stmt, int rowCount) throws SQLException {

        try (ResultSet rs = stmt.getGeneratedKeys()) {

            long[] keys = new long[rowCount];

            for (int i = 0; i < rowCount; i++) {
                rs.next();
                keys[i] = rs.getLong(1);
            }

            if (rs.next())
                throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

            return keys;
        }
    }
}
