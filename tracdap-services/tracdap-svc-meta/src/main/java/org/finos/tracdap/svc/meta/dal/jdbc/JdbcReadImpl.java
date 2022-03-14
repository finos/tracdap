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

import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.exception.EValidationGap;
import org.finos.tracdap.svc.meta.dal.jdbc.JdbcBaseDal.KeyedItem;
import com.google.protobuf.InvalidProtocolBufferException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
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

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                return new KeyedItem<>(objectPk, objectType);
            }
        }
    }

    KeyedItem<ObjectDefinition>
    readDefinition(
            Connection conn, short tenantId,
            long objectPk, TagSelector selector)
            throws SQLException {

        if (selector.getObjectCriteriaCase() == TagSelector.ObjectCriteriaCase.OBJECTVERSION)
            return readDefinitionByVersion(conn, tenantId, objectPk, selector.getObjectVersion());

        if (selector.getObjectCriteriaCase() == TagSelector.ObjectCriteriaCase.OBJECTASOF) {
            var objectAsOf = MetadataCodec.decodeDatetime(selector.getObjectAsOf()).toInstant();
            return readDefinitionByAsOf(conn, tenantId, objectPk, objectAsOf);
        }

        if (selector.getObjectCriteriaCase() == TagSelector.ObjectCriteriaCase.LATESTOBJECT)
            return readDefinitionByLatest(conn, tenantId, objectPk);

        throw new EValidationGap("Object version criteria not set in selector");
    }

    KeyedItem<ObjectDefinition>
    readDefinitionByVersion(
            Connection conn, short tenantId,
            long objectPk, int objectVersion)
            throws SQLException {

        var query =
                "select definition_pk, object_version, object_timestamp, definition\n" +
                "from object_definition\n" +
                "where tenant_id = ?\n" +
                "and object_fk = ?\n" +
                "and object_version = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, objectPk);
            stmt.setInt(3, objectVersion);

            return fetchDefinition(stmt);
        }
    }

    KeyedItem<ObjectDefinition>
    readDefinitionByAsOf(
            Connection conn, short tenantId,
            long objectPk, Instant objectAsOf)
            throws SQLException {

        var query =
                "select definition_pk, object_version, object_timestamp, definition\n" +
                "from object_definition\n" +
                "where tenant_id = ?\n" +
                "and object_fk = ?\n" +
                "and object_timestamp <= ?\n" +
                "and (object_superseded is null or object_superseded > ?)\n";

        try (var stmt = conn.prepareStatement(query)) {

            var sqlAsOf = java.sql.Timestamp.from(objectAsOf);

            stmt.setShort(1, tenantId);
            stmt.setLong(2, objectPk);
            stmt.setTimestamp(3, sqlAsOf);
            stmt.setTimestamp(4, sqlAsOf);

            return fetchDefinition(stmt);
        }
    }

    KeyedItem<ObjectDefinition>
    readDefinitionByLatest(
            Connection conn, short tenantId, long objectPk)
            throws SQLException {

        var query =
                "select definition_pk, object_version, object_timestamp, definition\n" +
                "from object_definition\n" +
                "where tenant_id = ?\n" +
                "  and object_fk = ?\n" +
                "  and object_is_latest = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, objectPk);
            stmt.setBoolean(3, true);

            return fetchDefinition(stmt);
        }
    }

    private KeyedItem<ObjectDefinition>
    fetchDefinition(PreparedStatement stmt) throws SQLException {

        try (var rs = stmt.executeQuery()) {

            if (!rs.next())
                throw new JdbcException(JdbcErrorCode.NO_DATA);

            var defPk = rs.getLong(1);
            var objectVersion = rs.getInt(2);
            var sqlTimestamp = rs.getTimestamp(3);
            var objectTimestamp = sqlTimestamp.toInstant();
            var defEncoded = rs.getBytes(4);
            var defDecoded = ObjectDefinition.parseFrom(defEncoded);

            // TODO: Encode / decode helper, type = protobuf | json ?

            if (rs.next())
                throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

            return new KeyedItem<>(defPk, objectVersion, objectTimestamp, defDecoded);
        }
        catch (InvalidProtocolBufferException e) {
            throw new JdbcException(JdbcErrorCode.INVALID_OBJECT_DEFINITION);
        }
    }

    KeyedItem<Void>
    readTagRecord(
            Connection conn, short tenantId,
            long definitionPk, TagSelector selector)
            throws SQLException {

        if (selector.getTagCriteriaCase() == TagSelector.TagCriteriaCase.TAGVERSION)
            return readTagRecordByVersion(conn, tenantId, definitionPk, selector.getTagVersion());

        if (selector.getTagCriteriaCase() == TagSelector.TagCriteriaCase.TAGASOF) {
            var tagAsOf = MetadataCodec.decodeDatetime(selector.getTagAsOf()).toInstant();
            return readTagRecordByAsOf(conn, tenantId, definitionPk, tagAsOf);
        }

        if (selector.getTagCriteriaCase() == TagSelector.TagCriteriaCase.LATESTTAG)
            return readTagRecordByLatest(conn, tenantId, definitionPk);

        throw new EValidationGap("Tag version criteria not set in selector");
    }

    KeyedItem<Void>
    readTagRecordByVersion(Connection conn, short tenantId, long definitionPk, int tagVersion) throws SQLException {

        var query =
                "select tag_pk, tag_version, tag_timestamp\n" +
                "from tag\n" +
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

    KeyedItem<Void>
    readTagRecordByAsOf(
            Connection conn, short tenantId,
            long definitionPk, Instant tagAsOf)
            throws SQLException {

        var query =
                "select tag_pk, tag_version, tag_timestamp\n" +
                "from tag\n" +
                "where tenant_id = ?\n" +
                "and definition_fk = ?\n" +
                "and tag_timestamp <= ?\n" +
                "and (tag_superseded is null or tag_superseded > ?)\n";

        try (var stmt = conn.prepareStatement(query)) {

            var sqlAsOf = java.sql.Timestamp.from(tagAsOf);

            stmt.setShort(1, tenantId);
            stmt.setLong(2, definitionPk);
            stmt.setTimestamp(3, sqlAsOf);
            stmt.setTimestamp(4, sqlAsOf);

            return readTagRecord(stmt);
        }
    }

    KeyedItem<Void>
    readTagRecordByLatest(Connection conn, short tenantId, long definitionPk) throws SQLException {

        var query =
                "select tag_pk, tag_version, tag_timestamp\n" +
                "from tag\n" +
                "where tenant_id = ?\n" +
                "and definition_fk = ?\n" +
                "and tag_is_latest = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, definitionPk);
            stmt.setBoolean(3, true);

            return readTagRecord(stmt);
        }
    }

    private KeyedItem<Void>
    readTagRecord(PreparedStatement stmt) throws SQLException {

        try (var rs = stmt.executeQuery()) {

            if (!rs.next())
                throw new JdbcException(JdbcErrorCode.NO_DATA);

            var tagPk = rs.getLong(1);
            var tagVersion = rs.getInt(2);
            var sqlTimestamp = rs.getTimestamp(3);
            var tagTimestamp = sqlTimestamp.toInstant();

            if (rs.next())
                throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

            // Tag record requires only PK and version info
            return new KeyedItem<>(tagPk, tagVersion, tagTimestamp, null);
        }
    }

    Map<String, Value>
    readTagAttrs(Connection conn, short tenantId, long tagPk) throws SQLException {

        var query =
                "select * from tag_attr\n" +
                "where tenant_id = ?\n" +
                "and tag_fk = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, tenantId);
            stmt.setLong(2, tagPk);

            try (var rs = stmt.executeQuery()) {

                var attrs = new HashMap<String, Value>();

                var currentAttrArray = new ArrayList<Value>();
                var currentAttrName = "";

                while (rs.next()) {

                    var attrName = rs.getString("attr_name");
                    var attrIndex = rs.getInt("attr_index");
                    var attrValue = JdbcAttrHelpers.readAttrValue(rs);

                    // Check to see if we have finished processing a multi-valued attr
                    // If so, record it against the last attr name before moving on
                    if (!currentAttrArray.isEmpty() && !attrName.equals(currentAttrName)) {

                        var arrayValue = JdbcAttrHelpers.assembleArrayValue(currentAttrArray);
                        attrs.put(currentAttrName, arrayValue);

                        currentAttrArray = new ArrayList<>();
                    }

                    // Update current attr name
                    currentAttrName = attrName;

                    // Accumulate the current attr record
                    if (attrIndex < 0)
                        attrs.put(attrName, attrValue);
                    else
                        currentAttrArray.add(attrValue);
                }

                // Check in case the last attr record was part of a multi-valued attr
                if (!currentAttrArray.isEmpty()) {
                    var arrayValue = JdbcAttrHelpers.assembleArrayValue(currentAttrArray);
                    attrs.put(currentAttrName, arrayValue);
                }

                return attrs;
            }
        }
    }
}
