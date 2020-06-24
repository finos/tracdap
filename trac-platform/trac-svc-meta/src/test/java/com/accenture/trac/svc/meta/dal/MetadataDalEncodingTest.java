package com.accenture.trac.svc.meta.dal;

import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.ObjectType;

import static com.accenture.trac.svc.meta.dal.MetadataDalTestData.*;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import com.accenture.trac.svc.meta.dal.impls.JdbcH2Impl;
import com.accenture.trac.svc.meta.dal.impls.JdbcMysqlImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;


abstract class MetadataDalEncodingTest extends MetadataDalTestBase {

    @ExtendWith(JdbcMysqlImpl.class)
    static class JdbcMysql extends MetadataDalEncodingTest {}

    @ExtendWith(JdbcH2Impl.class)
    static class JdbcH2 extends MetadataDalEncodingTest {}

    @Test
    void roundTrip_oneObjectTypeOk() throws Exception {

        var origDef = dummyDataDef();
        var origTag = dummyTag(origDef);
        var origId = MetadataCodec.decode(origDef.getHeader().getId());

        var future = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                .thenCompose(x -> dal.loadTag(TEST_TENANT, ObjectType.DATA, origId, 1, 1));

        assertEquals(origTag, unwrap(future));
    }

    @Test
    void roundTrip_allObjectTypesOk() throws Exception {

        var typesToTest = new ObjectType[] {ObjectType.DATA, ObjectType.MODEL};

        for (var objectType: typesToTest) {

            var origTag = dummyTagForObjectType(objectType);
            var origId = MetadataCodec.decode(origTag.getHeader().getId());

            var future = CompletableFuture.completedFuture(0)
                    .thenCompose(x -> dal.saveNewObject(TEST_TENANT, origTag))
                    .thenCompose(x -> dal.loadTag(TEST_TENANT, objectType, origId, 1, 1));

            assertEquals(origTag, unwrap(future));
        }
    }
}
