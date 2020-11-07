/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.meta.dal.jdbc;

import com.accenture.trac.metadata.*;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.exception.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.Function;


public class JdbcAttrHelpers {

    private static final Logger log = LoggerFactory.getLogger(JdbcAttrHelpers.class);

    static void setAttrValue(PreparedStatement stmt, int pIndex, BasicType attrType, Value value) throws SQLException {

        switch (attrType) {

            // Basic types - these should be uncontroversial!

            case BOOLEAN:
                stmt.setBoolean(pIndex, MetadataCodec.decodeBooleanValue(value));
                break;

            case INTEGER:
                stmt.setLong(pIndex, MetadataCodec.decodeIntegerValue(value));
                break;

            case FLOAT:
                stmt.setDouble(pIndex, MetadataCodec.decodeFloatValue(value));
                break;

            case STRING:
                stmt.setString(pIndex, MetadataCodec.decodeStringValue(value));
                break;

            // Object types - make sure these map to the correct SQL types

            case DECIMAL:
                var decimal = MetadataCodec.decodeDecimalValue(value);
                stmt.setBigDecimal(pIndex, decimal);
                break;

            case DATE:
                var localDate = MetadataCodec.decodeDateValue(value);
                var sqlDate = java.sql.Date.valueOf(localDate);
                stmt.setDate(pIndex, sqlDate);
                break;

            case DATETIME:
                var offsetDateTime = MetadataCodec.decodeDateTimeValue(value);
                var sqlTimestamp = java.sql.Timestamp.from(offsetDateTime.toInstant());
                stmt.setTimestamp(pIndex, sqlTimestamp);
                break;

            default:

                // Internal error - no code should attempt to set a non-primitive value as a query parameter

                var message = String.format(
                        "Failed to set attr value in query parameter" +
                        " (attr type %s is not recognised as a primitive type)",
                        attrType.name());

                log.error(message);

                throw new ETracInternal(message);
        }
    }

    static Value readAttrValue(ResultSet rs) throws SQLException {

        BasicType primitiveType = fetchAttrType(rs);

        switch (primitiveType) {

            case BOOLEAN:
                return fetchAttrValue(rs, "attr_value_boolean", ResultSet::getBoolean, BasicType.BOOLEAN);

            case INTEGER:
                return fetchAttrValue(rs, "attr_value_integer", ResultSet::getLong, BasicType.INTEGER);

            case FLOAT:
                return fetchAttrValue(rs, "attr_value_float", ResultSet::getDouble, BasicType.FLOAT);

            case STRING:
                return fetchAttrValue(rs, "attr_value_string", ResultSet::getString, BasicType.STRING);

            case DECIMAL:
                return fetchAttrValue(rs, "attr_value_decimal", ResultSet::getBigDecimal, BasicType.DECIMAL,
                        BigDecimal::stripTrailingZeros);

            case DATE:
                return fetchAttrValue(rs, "attr_value_date", ResultSet::getDate, BasicType.DATE,
                        Date::toLocalDate);

            case DATETIME:
                return fetchAttrValue(rs, "attr_value_datetime", ResultSet::getTimestamp, BasicType.DATETIME,
                        timestamp -> timestamp.toInstant().atOffset(ZoneOffset.UTC));

            default:

                // Internal error - the metadata service should make sure tag attrs are always stored correctly

                var message = String.format(
                        "Failed to read attr value" +
                        " (attr type %s is not recognised as a primitive type)",
                        primitiveType.name());

                log.error(message);

                throw new ETracInternal(message);
        }
    }

    static BasicType fetchAttrType(ResultSet rs) throws SQLException {

        try {
            var primitiveTypeCode = rs.getString("attr_type");
            return BasicType.valueOf(primitiveTypeCode);
        }
        catch (IllegalArgumentException e) {

            // Internal error - the metadata service should make sure tag attrs are always stored correctly
            var message = "Failed to read attr value (attr type was not stored or could not be decoded)";
            log.error(message);

            throw new ETracInternal(message, e);
        }
    }

    static private <TValue>
    Value fetchAttrValue(
            ResultSet rs, String fieldName,
            AttrGetter<TValue> getter,
            BasicType primitiveType)
            throws SQLException {

        return fetchAttrValue(rs, fieldName, getter, primitiveType, Function.identity());
    }

    static private <TValue, TMappedValue>
    Value fetchAttrValue(
            ResultSet rs, String fieldName,
            AttrGetter<TValue> getter,
            BasicType primitiveType,
            Function<TValue, TMappedValue> mapping)
            throws SQLException {

        var sqlValue = getter.get(rs, fieldName);

        if (rs.wasNull())
            return null;

        var mappedValue = mapping.apply(sqlValue);

        return MetadataCodec.encodeValue(mappedValue, primitiveType);
    }

    @FunctionalInterface
    private interface AttrGetter<TSqlValue> {

        TSqlValue get(ResultSet rs, String fieldName) throws SQLException;
    }

    static Value assembleArrayValue(List<Value> items) {

        var arrayBasicType = items.get(0).getType().getBasicType();

        // Sanity check to make sure all the items in a multi-valued attr are of the same type
        var arrayTypeCheck = items.stream().allMatch(item -> item.getType().getBasicType() == arrayBasicType);

        if (!arrayTypeCheck) {

            // Internal error - the metadata service should never store multi-valued attrs with mismatched types
            var message = "Failed to assemble multi-valued attr (some items do not match the specified array type)";
            log.error(message);

            throw new ETracInternal(message);
        }

        var typeDescriptor = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                        .setBasicType(arrayBasicType));

        var arrayValue = ArrayValue.newBuilder()
                .addAllItem(items);

        return Value.newBuilder()
                .setType(typeDescriptor)
                .setArrayValue(arrayValue)
                .build();
    }
}
