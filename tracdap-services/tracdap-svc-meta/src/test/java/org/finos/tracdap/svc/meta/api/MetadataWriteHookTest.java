/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.svc.meta.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.service.ObjectWrite;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.meta.test.TestMetadataWriteHook;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;
import static org.junit.jupiter.api.Assertions.*;


class MetadataWriteHookTest {

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
            .runDbDeploy(true)
            .startService(TracMetadataService.class)
            .build();

    private TracMetadataApiGrpc.TracMetadataApiBlockingStub publicApi;
    private InternalMetadataApiGrpc.InternalMetadataApiBlockingStub internalApi;

    @BeforeEach
    void setup() {
        publicApi = platform.metaClientBlocking();
        internalApi = platform.metaClientInternalBlocking();
    }

    @Test
    void createObject_hookCalled() {

        var schemaType = uniqueSchemaType();
        publicApi.createObject(createRequest(schemaType));

        var write = singleWrite(schemaType);
        assertEquals(TEST_TENANT, write.tenant());
        assertEquals(ObjectType.CUSTOM, write.objectType());
        assertEquals(ObjectWrite.Operation.CREATE, write.operation());
        assertFalse(write.inBatch());
        assertNull(write.priorDefinition());
        assertEquals(schemaType, write.definition().getCustom().getCustomSchemaType());
        assertNotNull(write.userMetadata());
    }

    @Test
    void createObject_internalApiHookCalled() {

        var schemaType = uniqueSchemaType();
        internalApi.createObject(createRequest(schemaType));

        var write = singleWrite(schemaType);
        assertEquals(ObjectWrite.Operation.CREATE, write.operation());
        assertFalse(write.inBatch());
    }

    @Test
    void updateObject_hookCalledWithPrior() {

        var schemaType = uniqueSchemaType();
        var v1 = publicApi.createObject(createRequest(schemaType));
        publicApi.updateObject(updateRequest(v1, schemaType, 2));

        var writes = writesFor(schemaType);
        assertEquals(2, writes.size());

        var update = writes.get(1);
        assertEquals(ObjectWrite.Operation.UPDATE, update.operation());
        assertFalse(update.inBatch());
        assertEquals(1, update.priorDefinition().getCustom().getCustomSchemaVersion());
        assertEquals(2, update.definition().getCustom().getCustomSchemaVersion());
    }

    @Test
    void writeBatch_hookCalledForEachObjectItem() {

        var schemaType = uniqueSchemaType();
        var existing = publicApi.createObject(createRequest(schemaType));

        publicApi.writeBatch(MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addCreateObjects(createRequest(schemaType))
                .addCreateObjects(createRequest(schemaType))
                .addUpdateObjects(updateRequest(existing, schemaType, 2))
                .build());

        var batchWrites = writesFor(schemaType).subList(1, 4);
        assertTrue(batchWrites.stream().allMatch(ObjectWrite::inBatch));
        assertTrue(batchWrites.stream().allMatch(w -> w.tenant().equals(TEST_TENANT)));

        assertEquals(
                List.of(ObjectWrite.Operation.CREATE, ObjectWrite.Operation.CREATE, ObjectWrite.Operation.UPDATE),
                batchWrites.stream().map(ObjectWrite::operation).collect(Collectors.toList()));
    }

    @Test
    void writeBatch_publicPreallocatedObjectHookCalled() {

        var schemaType = uniqueSchemaType();

        var preallocated = publicApi.writeBatch(MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addPreallocateIds(MetadataWriteRequest.newBuilder().setObjectType(ObjectType.CUSTOM))
                .build());

        var preallocatedId = preallocated.getPreallocateIds(0);

        publicApi.writeBatch(MetadataWriteBatchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .addCreatePreallocatedObjects(createRequest(schemaType).toBuilder()
                        .setPriorVersion(MetadataUtil.preallocated(preallocatedId)))
                .build());

        var write = singleWrite(schemaType);
        assertEquals(ObjectWrite.Operation.CREATE_PREALLOCATED, write.operation());
        assertTrue(write.inBatch());
    }

    @Test
    void createPreallocatedObject_internalApiHookCalled() {

        var schemaType = uniqueSchemaType();

        var preallocatedId = internalApi.preallocateId(MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .build());

        internalApi.createPreallocatedObject(createRequest(schemaType).toBuilder()
                .setPriorVersion(MetadataUtil.preallocated(preallocatedId))
                .build());

        var write = singleWrite(schemaType);
        assertEquals(ObjectWrite.Operation.CREATE_PREALLOCATED, write.operation());
        assertFalse(write.inBatch());
    }

    @Test
    void updateTag_hookNotCalled() {

        var schemaType = uniqueSchemaType();
        var v1 = publicApi.createObject(createRequest(schemaType));

        publicApi.updateTag(MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(MetadataUtil.selectorFor(v1))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("test_attr")
                        .setValue(MetadataCodec.encodeValue("value")))
                .build());

        assertEquals(1, writesFor(schemaType).size());
    }

    @Test
    void createObject_refused() {

        var error = assertThrows(StatusRuntimeException.class, () ->
                publicApi.createObject(refused(createRequest(uniqueSchemaType()))));

        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
        assertTrue(error.getStatus().getDescription().contains(TestMetadataWriteHook.REFUSED_MESSAGE));
    }

    @Test
    void updateObject_refused() {

        var schemaType = uniqueSchemaType();
        var v1 = publicApi.createObject(createRequest(schemaType));

        var error = assertThrows(StatusRuntimeException.class, () ->
                publicApi.updateObject(refused(updateRequest(v1, schemaType, 2))));

        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
        assertTrue(error.getStatus().getDescription().contains(TestMetadataWriteHook.REFUSED_MESSAGE));

        var latest = publicApi.readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorForLatest(v1))
                .build());

        assertEquals(1, latest.getHeader().getObjectVersion());
    }

    @Test
    void writeBatch_refusedItemSavesNothing() {

        var schemaType = uniqueSchemaType();

        var error = assertThrows(StatusRuntimeException.class, () ->
                publicApi.writeBatch(MetadataWriteBatchRequest.newBuilder()
                        .setTenant(TEST_TENANT)
                        .addCreateObjects(createRequest(schemaType))
                        .addCreateObjects(refused(createRequest(schemaType)))
                        .build()));

        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
        assertEquals(0, searchBySchemaType(schemaType).getSearchResultCount());
    }

    @Test
    void customSchemaType_searchable() {

        var schemaType = uniqueSchemaType();
        var objectId = publicApi.createObject(createRequest(schemaType));
        publicApi.createObject(createRequest(uniqueSchemaType()));

        var result = searchBySchemaType(schemaType);

        assertEquals(1, result.getSearchResultCount());
        assertEquals(objectId, result.getSearchResult(0).getHeader());
        assertEquals(schemaType, MetadataCodec.decodeStringValue(
                result.getSearchResult(0).getAttrsOrThrow(MetadataConstants.TRAC_CUSTOM_SCHEMA_TYPE_ATTR)));
    }

    private MetadataSearchResponse searchBySchemaType(String schemaType) {

        return publicApi.search(MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                        .setObjectType(ObjectType.CUSTOM)
                        .setSearch(SearchExpression.newBuilder()
                                .setTerm(SearchTerm.newBuilder()
                                        .setAttrName(MetadataConstants.TRAC_CUSTOM_SCHEMA_TYPE_ATTR)
                                        .setAttrType(BasicType.STRING)
                                        .setOperator(SearchOperator.EQ)
                                        .setSearchValue(MetadataCodec.encodeValue(schemaType)))))
                .build());
    }

    private static MetadataWriteRequest refused(MetadataWriteRequest request) {

        var definition = request.getDefinition().toBuilder();
        definition.getCustomBuilder().setCustomData(TestMetadataWriteHook.REFUSED_DATA);

        return request.toBuilder().setDefinition(definition).build();
    }

    private static String uniqueSchemaType() {
        return "test_hook_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static MetadataWriteRequest createRequest(String schemaType) {

        return MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setDefinition(customDef(schemaType, 1))
                .build();
    }

    private static MetadataWriteRequest updateRequest(TagHeader prior, String schemaType, int schemaVersion) {

        return MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(MetadataUtil.selectorFor(prior))
                .setDefinition(customDef(schemaType, schemaVersion))
                .build();
    }

    private static ObjectDefinition customDef(String schemaType, int schemaVersion) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setCustom(CustomDefinition.newBuilder()
                        .setCustomSchemaType(schemaType)
                        .setCustomSchemaVersion(schemaVersion)
                        .setCustomData(ByteString.copyFromUtf8("{}")))
                .build();
    }

    private static List<ObjectWrite> writesFor(String schemaType) {

        return TestMetadataWriteHook.WRITES.stream()
                .filter(w -> w.objectType() == ObjectType.CUSTOM)
                .filter(w -> w.definition().getCustom().getCustomSchemaType().equals(schemaType))
                .collect(Collectors.toList());
    }

    private static ObjectWrite singleWrite(String schemaType) {

        var writes = writesFor(schemaType);
        assertEquals(1, writes.size());
        return writes.get(0);
    }
}
