package com.accenture.trac.svc.meta.dal;

import com.accenture.trac.common.metadata.BasicType;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.ObjectType;
import static com.accenture.trac.svc.meta.test.TestData.*;

import java.math.BigDecimal;
import java.time.*;
import java.util.concurrent.CompletableFuture;

import com.accenture.trac.svc.meta.test.IDalTestable;
import com.accenture.trac.svc.meta.test.JdbcH2Impl;
import com.accenture.trac.svc.meta.test.JdbcMariaDbImpl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;


abstract class MetadataDalEncodingTest implements IDalTestable {

    private IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }

    @ExtendWith(JdbcH2Impl.class)
    static class JdbcH2 extends MetadataDalEncodingTest {}

    @Tag("integration")
    @Tag("int-mariadb")
    @ExtendWith(JdbcMariaDbImpl.class)
    static class JdbcMariaDB extends MetadataDalEncodingTest {}

    @Test
    void roundTrip_ok() throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        assertEquals(origTag, unwrap(future));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNRECOGNIZED"})
    void roundTrip_allObjectTypesOk(ObjectType objectType) throws Exception {

        var origTag = dummyTagForObjectType(objectType);
        var origId = MetadataCodec.decode(origTag.getDefinition().getHeader().getObjectId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, objectType, origId, 1, 1));

        assertEquals(origTag, unwrap(future));
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNRECOGNIZED", "ARRAY"})
    void roundTrip_allAttrTypesOk(BasicType attrType) throws Exception {

        var origDef = dummyDataDef(INCLUDE_HEADER);
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getObjectId());

        var attrName = "test_attr_" + attrType.name();
        var attrValue = objectOfType(attrType);

        var testTag = origTag.toBuilder()
                .putAttr(attrName, MetadataCodec.encodeNativeObject(attrValue))
                .build();

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, testTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        assertEquals(testTag, unwrap(future));
    }

    @Test
    @Disabled
    void testLargeAndSmallValues() {

        // Min, max and values very near zero (i.e. very small |x|)

        Assertions.fail();
    }

    @Test
    @Disabled
    void timeOffsetHandling() {
        Assertions.fail();
    }

    @Test
    @Disabled
    void subSecondPrecision() {
        Assertions.fail();
    }

    @Test
    @Disabled
    void unicodeCharacterEncoding() {
        Assertions.fail();
    }

    Object objectOfType(BasicType basicType) {

        switch (basicType) {

            case BOOLEAN: return true;
            case INTEGER: return (long) 42;
            case FLOAT: return Math.PI;
            case STRING: return "Slartibartfast";
            case DECIMAL: return new BigDecimal("1234.567");
            case DATE: return LocalDate.now();

            // Metadata datetime attrs have microsecond precision
            case DATETIME:
                var dateTime = OffsetDateTime.now(ZoneOffset.UTC);
                return truncateMicrosecondPrecision(dateTime);

            default:
                throw new RuntimeException("Test object not available for basic type " + basicType.toString());
        }
    }

    OffsetDateTime truncateMicrosecondPrecision(OffsetDateTime dateTime) {

        int precision = 6;

        var nanos = dateTime.getNano();
        var nanoPrecision = (int) Math.pow(10, 9 - precision);
        var truncatedNanos = (nanos / nanoPrecision) * nanoPrecision;
        return dateTime.withNano(truncatedNanos);
    }
}
