package trac.svc.meta.dal.jdbc;

import com.google.protobuf.MessageLite;
import trac.common.metadata.ObjectType;
import trac.common.metadata.PrimitiveType;
import trac.common.metadata.Tag;

import java.sql.*;
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
            long[] objectPk, int[] objectVersion, MessageLite[] definition)
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
            long[] definitionPk, int[] tagVersion, Tag[] tag)
            throws SQLException {

        var query =
                "insert into tag (\n" +
                "  tenant_id,\n" +
                "  definition_fk,\n" +
                "  tag_version" +
                ")\n" +
                "values (?, ?, ?)";

        try (var stmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            for (var i = 0; i < definitionPk.length; i++) {

                stmt.setShort(1, tenantId);
                stmt.setLong(2, definitionPk[i]);
                stmt.setInt(3, tagVersion[i]);

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
                "  attr_value_bool,\n" +
                "  attr_value_integer,\n" +
                "  attr_value_float,\n" +
                "  attr_value_decimal,\n" +
                "  attr_value_string,\n" +
                "  attr_value_date,\n" +
                "  attr_value_datetime,\n" +
                "  attr_value_datetime_zone\n" +
                ")\n" +
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (var stmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            for (var i = 0; i < tagPk.length; i++) {
                for (var attr : tag[i].getAttrMap().entrySet()) {


                    stmt.setShort(1, tenantId);
                    stmt.setLong(2, tagPk[i]);
                    stmt.setString(3, attr.getKey());

                    stmt.setNull(4, Types.BOOLEAN);
                    stmt.setNull(5, Types.BIGINT);
                    stmt.setNull(6, Types.DOUBLE);
                    stmt.setNull(7, Types.DECIMAL);
                    stmt.setNull(8, Types.VARCHAR);
                    stmt.setNull(9, Types.DATE);
                    stmt.setNull(10, Types.TIMESTAMP);
                    stmt.setNull(11, Types.VARCHAR);

                    var attrValue = attr.getValue();
                    var attrType = attrValue.getType();

                    if (attrType == PrimitiveType.BOOLEAN)
                        stmt.setBoolean(4, attrValue.getBooleanValue());
                    if (attrType == PrimitiveType.INTEGER)
                        stmt.setLong(5, attrValue.getIntegerValue());
                    if (attrType == PrimitiveType.FLOAT)
                        stmt.setDouble(6, attrValue.getFloatValue());
                    if (attrType == PrimitiveType.STRING)
                        stmt.setString(8, attrValue.getStringValue());

                    /*
                    if (attrType == PrimitiveType.DECIMAL)
                        ;  // TODO stmt.setBigDecimal(7, tracDecimal(attrValue.getDecimalValue()));
                    if (attrType == PrimitiveType.DATE)
                        ;  // TODO stmt.setDate(7, java.sql.Date.valueOf(tracDate(attrValue.getDateValue())));
                    if (attrType == PrimitiveType.DATETIME)
                        ;  // TODO stmt.setDate(7, java.sql.Timestamp.valueOf(tracDatetime(attrValue.getDateValue())));
                     */

                    stmt.addBatch();
                }
            }

            stmt.executeBatch();
        }
    }

    private long[] generatedKeys(Statement stmt, int rowCount) throws SQLException {

        try (ResultSet rs = stmt.getGeneratedKeys()) {

            long[] keys = new long[rowCount];

            for (int i = 0; i < rowCount; i++) {
                rs.next();
                keys[i] = rs.getLong(1);
            }

            if (!rs.last())
                // TODO: Real exception type
                throw new RuntimeException();

            return keys;
        }
    }
}
