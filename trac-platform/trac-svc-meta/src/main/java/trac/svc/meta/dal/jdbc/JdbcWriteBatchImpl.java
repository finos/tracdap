package trac.svc.meta.dal.jdbc;

import com.google.protobuf.MessageLite;
import trac.common.metadata.ObjectType;
import trac.common.metadata.PrimitiveType;
import trac.common.metadata.Tag;

import java.sql.*;
import java.util.Arrays;
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
            long[] definitionPk, int[] tagVersion)
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
                "  attr_type,\n" +
                "  attr_value_boolean,\n" +
                "  attr_value_integer,\n" +
                "  attr_value_float,\n" +
                "  attr_value_decimal,\n" +
                "  attr_value_string,\n" +
                "  attr_value_date,\n" +
                "  attr_value_datetime,\n" +
                "  attr_value_datetime_zone\n" +
                ")\n" +
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (var stmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            for (var i = 0; i < tagPk.length; i++) {
                for (var attr : tag[i].getAttrMap().entrySet()) {


                    stmt.setShort(1, tenantId);
                    stmt.setLong(2, tagPk[i]);
                    stmt.setString(3, attr.getKey());
                    stmt.setString(4, attr.getValue().getType().name());

                    stmt.setNull(5, Types.BOOLEAN);
                    stmt.setNull(6, Types.BIGINT);
                    stmt.setNull(7, Types.DOUBLE);
                    stmt.setNull(8, Types.DECIMAL);
                    stmt.setNull(9, Types.VARCHAR);
                    stmt.setNull(10, Types.DATE);
                    stmt.setNull(11, Types.TIMESTAMP);
                    stmt.setNull(12, Types.VARCHAR);

                    var attrValue = attr.getValue();
                    var attrType = attrValue.getType();

                    if (attrType == PrimitiveType.BOOLEAN)
                        stmt.setBoolean(5, attrValue.getBooleanValue());
                    if (attrType == PrimitiveType.INTEGER)
                        stmt.setLong(6, attrValue.getIntegerValue());
                    if (attrType == PrimitiveType.FLOAT)
                        stmt.setDouble(7, attrValue.getFloatValue());
                    if (attrType == PrimitiveType.STRING)
                        stmt.setString(9, attrValue.getStringValue());

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
            throw new JdbcException(JdbcErrorCode.INSERT_MISSING_FK.name(), JdbcErrorCode.INSERT_MISSING_FK);
    }

    private long[] generatedKeys(Statement stmt, int rowCount) throws SQLException {

        try (ResultSet rs = stmt.getGeneratedKeys()) {

            long[] keys = new long[rowCount];

            for (int i = 0; i < rowCount; i++) {
                rs.next();
                keys[i] = rs.getLong(1);
            }

            if (!rs.last())
                throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS.name(), JdbcErrorCode.TOO_MANY_ROWS);

            return keys;
        }
    }
}
