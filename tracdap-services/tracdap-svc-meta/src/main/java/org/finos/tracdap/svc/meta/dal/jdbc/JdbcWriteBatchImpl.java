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

import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.svc.meta.dal.jdbc.dialects.IDialect;

import java.sql.*;
import java.time.Instant;
import java.util.*;


class JdbcWriteBatchImpl {

    private final IDialect dialect;
    private final JdbcReadBatchImpl readBatch;

    JdbcWriteBatchImpl(IDialect dialect, JdbcReadBatchImpl readBatch) {
        this.dialect = dialect;
        this.readBatch = readBatch;
    }

    long[] writeObjectId(Connection conn, short tenantId, JdbcMetadataDal.ObjectParts parts) throws SQLException {

        var query =
                "insert into object_id (\n" +
                "  tenant_id,\n" +
                "  object_type,\n" +
                "  object_id_hi,\n" +
                "  object_id_lo\n" +
                ")\n" +
                "values (?, ?, ?, ?)";

        // Only request generated key columns if the driver supports it
        var keySupport = dialect.supportsGeneratedKeys();
        var keyColumns = new String[] { "object_pk" };

        try (var stmt = keySupport ? conn.prepareStatement(query, keyColumns) : conn.prepareStatement(query)) {

            for (var i = 0; i < parts.objectId.length; i++) {

                stmt.setShort(1, tenantId);
                stmt.setString(2, parts.objectType[i].name());
                stmt.setLong(3, parts.objectId[i].getMostSignificantBits());
                stmt.setLong(4, parts.objectId[i].getLeastSignificantBits());

                stmt.addBatch();
            }

            stmt.executeBatch();

            if (keySupport)
                return generatedKeys(stmt, parts.objectId.length);
            else
                return readBatch.lookupObjectPks(conn, tenantId, parts.objectId);
        }
    }

    long[] writeObjectDefinition(Connection conn, short tenantId, long[] objectPk, JdbcMetadataDal.ObjectParts parts) throws SQLException {

        var query =
                "insert into object_definition (\n" +
                "  tenant_id,\n" +
                "  object_fk,\n" +
                "  object_version,\n" +
                "  object_timestamp,\n" +
                "  object_is_latest,\n" +
                "  meta_format,\n" +
                "  meta_version,\n" +
                "  definition" +
                ")\n" +
                "values (?, ?, ?, ?, ?, ?, ?, ?)";

        // Only request generated key columns if the driver supports it
        var keySupport = dialect.supportsGeneratedKeys();
        var keyColumns = new String[] { "definition_pk" };

        try (var stmt = keySupport ? conn.prepareStatement(query, keyColumns) : conn.prepareStatement(query)) {

            for (var i = 0; i < objectPk.length; i++) {

                var sqlTimestamp = java.sql.Timestamp.from(parts.objectTimestamp[i]);

                // Metadata format can be used to support alternate encoding, e.g. JSON
                // Metadata version tracks breaking changes to the metadata model
                // Currently there is no support for reading back other formats or old versions

                stmt.setShort(1, tenantId);
                stmt.setLong(2, objectPk[i]);
                stmt.setInt(3, parts.objectVersion[i]);
                stmt.setTimestamp(4, sqlTimestamp);
                stmt.setBoolean(5, true);
                stmt.setInt(6, MetadataFormat.PROTO.getNumber());
                stmt.setInt(7, MetadataVersion.CURRENT.getNumber());
                stmt.setBytes(8, parts.definition[i].toByteArray());

                stmt.addBatch();
            }

            stmt.executeBatch();

            if (keySupport)
                return generatedKeys(stmt, objectPk.length);
            else
                return readBatch.lookupDefinitionPk(conn, tenantId, objectPk, parts.objectVersion);
        }
    }

    long[] writeTagRecord(Connection conn, short tenantId, long[] definitionPk, JdbcMetadataDal.ObjectParts parts) throws SQLException {

        var query =
                "insert into tag (\n" +
                "  tenant_id,\n" +
                "  definition_fk,\n" +
                "  tag_version,\n" +
                "  tag_timestamp,\n" +
                "  tag_is_latest,\n" +
                "  object_type" +
                ")\n" +
                "values (?, ?, ?, ?, ?, ?)";

        // Only request generated key columns if the driver supports it
        var keySupport = dialect.supportsGeneratedKeys();
        var keyColumns = new String[] { "tag_pk" };

        try (var stmt = keySupport ? conn.prepareStatement(query, keyColumns) : conn.prepareStatement(query)) {

            for (var i = 0; i < definitionPk.length; i++) {

                var sqlTimestamp = java.sql.Timestamp.from(parts.tagTimestamp[i]);

                stmt.setShort(1, tenantId);
                stmt.setLong(2, definitionPk[i]);
                stmt.setInt(3, parts.tagVersion[i]);
                stmt.setTimestamp(4, sqlTimestamp);
                stmt.setBoolean(5, true);
                stmt.setString(6, parts.objectType[i].name());

                stmt.addBatch();
            }

            stmt.executeBatch();

            if (keySupport)
                return generatedKeys(stmt, definitionPk.length);
            else
                return readBatch.lookupTagPk(conn, tenantId, definitionPk, parts.tagVersion);
        }
    }

    void writeTagAttrs(Connection conn, short tenantId, long[] tagPk, JdbcMetadataDal.ObjectParts parts) throws SQLException {

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
                for (var attr : parts.tag[i].getAttrsMap().entrySet()) {

                    var attrRootValue = attr.getValue();
                    var attrType = attrBasicType(attrRootValue);

                    // TODO: Constants for single / multi valued base index
                    var attrIndex = TypeSystem.isPrimitive(attrRootValue) ? -1 : 0;

                    for (var attrValue : attrValues(attrRootValue)) {

                        stmt.setShort(1, tenantId);
                        stmt.setLong(2, tagPk[i]);
                        stmt.setString(3, attr.getKey());
                        stmt.setString(4, attrType.name());
                        stmt.setInt(5, attrIndex);

                        stmt.setNull(6, dialect.booleanType());
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

    private BasicType attrBasicType(Value attrValue) {

        var basicType = TypeSystem.basicType(attrValue);

        if (TypeSystem.isPrimitive(basicType))
            return basicType;

        if (basicType == BasicType.ARRAY) {

            var descriptor = TypeSystem.descriptor(attrValue);
            var arrayType = descriptor.getArrayType();

            if (TypeSystem.isPrimitive(arrayType))
                return arrayType.getBasicType();

            // Tag attributes should be validated higher up the stack
            // If an attribute gets through that is not a supported type, that is an internal error

            throw new ETracInternal("Tag attributes of type [ARRAY] must contain primitive types");
        }

        var message = "Tag attributes of type [%s] are not supported";
        throw new ETracInternal(String.format(message, basicType.name()));
    }

    private Iterable<Value> attrValues(Value rootValue) {

        if (TypeSystem.isPrimitive(rootValue))
            return List.of(rootValue);

        return rootValue.getArrayValue().getItemsList();
    }

    void closeObjectDefinition(Connection conn, short tenantId, long[] objectPk, JdbcMetadataDal.ObjectParts parts) throws SQLException {

        var query =
                "update object_definition \n" +
                "set\n" +
                "  object_superseded = ?,\n" +
                "  object_is_latest = ?\n" +
                "where tenant_id = ?\n" +
                "  and object_fk = ?\n" +
                "  and object_is_latest = ?";

        try (var stmt = conn.prepareStatement(query)) {
            closeRecord(stmt, tenantId, objectPk, parts.objectTimestamp);
        }
    }

    void closeTagRecord(Connection conn, short tenantId, long[] definitionPk, JdbcMetadataDal.ObjectParts parts) throws SQLException {

        var query =
                "update tag \n" +
                "set\n" +
                "  tag_superseded = ?,\n" +
                "  tag_is_latest = ?\n" +
                "where tenant_id = ?\n" +
                "  and definition_fk = ?\n" +
                "  and tag_is_latest = ?";

        try (var stmt = conn.prepareStatement(query)) {
            closeRecord(stmt, tenantId, definitionPk, parts.tagTimestamp);
        }
    }

    private void closeRecord(PreparedStatement stmt, short tenantId, long[] keys, Instant[] timestamps) throws SQLException {


        for (var i = 0; i < keys.length; i++) {

            var sqlTimestamp = java.sql.Timestamp.from(timestamps[i]);

            stmt.setTimestamp(1, sqlTimestamp);
            stmt.setBoolean(2, false);
            stmt.setShort(3, tenantId);
            stmt.setLong(4, keys[i]);
            stmt.setBoolean(5, true);

            stmt.addBatch();
        }

        stmt.executeBatch();
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
