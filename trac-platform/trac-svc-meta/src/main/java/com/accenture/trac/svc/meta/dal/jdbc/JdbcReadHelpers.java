package com.accenture.trac.svc.meta.dal.jdbc;

import com.accenture.trac.svc.meta.exception.TracInternalError;
import trac.common.metadata.PrimitiveType;
import trac.common.metadata.PrimitiveValue;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;


public class JdbcReadHelpers {

    static PrimitiveValue readAttrValue(ResultSet rs) throws SQLException {

        PrimitiveType primitiveType;

        try {
            var primitiveTypeCode = rs.getString("attr_type");
            primitiveType = PrimitiveType.valueOf(primitiveTypeCode);
        }
        catch (IllegalArgumentException e) {
            throw new TracInternalError("", e);  // TODO: Error message
        }

        switch (primitiveType) {

            case BOOLEAN:
                return fetchAttrValue(rs, "attr_value_boolean", ResultSet::getBoolean, PrimitiveType.BOOLEAN);

            case INTEGER:
                return fetchAttrValue(rs, "attr_value_integer", ResultSet::getLong, PrimitiveType.INTEGER);

            case FLOAT:
                return fetchAttrValue(rs, "attr_value_float", ResultSet::getDouble, PrimitiveType.FLOAT);

            case DECIMAL:
                return null;  // TODO

            case STRING:
                return fetchAttrValue(rs, "attr_value_string", ResultSet::getString, PrimitiveType.STRING);

            case DATE:
                return fetchAttrValue(rs, "attr_value_date", ResultSet::getDate, PrimitiveType.DATE, Date::toLocalDate);

            case DATETIME:
                return null;  // TODO

            default:
                return null;  // TODO
        }


    }

    static private <TValue>
    PrimitiveValue fetchAttrValue(
            ResultSet rs, String fieldName,
            AttrGetter<TValue> getter,
            PrimitiveType primitiveType)
            throws SQLException {

        return fetchAttrValue(rs, fieldName, getter, primitiveType, Function.identity());
    }

    static private <TSqlValue, TValue>
    PrimitiveValue fetchAttrValue(
            ResultSet rs, String fieldName,
            AttrGetter<TSqlValue> getter,
            PrimitiveType primitiveType,
            Function<TSqlValue, TValue> mapper)
            throws SQLException {

        var sqlValue = getter.get(rs, fieldName);

        if (rs.wasNull())
            return null;

        var attrValue = mapper.apply(sqlValue);
        var primitiveValue = PrimitiveValue.newBuilder()
                .setType(primitiveType)
                .setStringValue(attrValue.toString())
                .build();

        return primitiveValue;
    }

    @FunctionalInterface
    private interface AttrGetter<TSqlValue> {

        TSqlValue get(ResultSet rs, String fieldName) throws SQLException;
    }
}
