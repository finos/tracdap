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
import org.finos.tracdap.common.metadata.tag.ObjectUpdateLogic;
import org.finos.tracdap.common.validation.ValidationConstants;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.meta.test.TestMetadataWriteHook;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.finos.tracdap.test.meta.SampleMetadata;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;
import static org.junit.jupiter.api.Assertions.*;


abstract class ControlledTagApiTest {

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TRAC_CONFIG_ENV_VAR = "TRAC_CONFIG_FILE";

    private static final io.grpc.Metadata.Key<TracErrorDetails> TRAC_ERROR_DETAILS_KEY = io.grpc.Metadata.Key.of(
            "trac-error-details-bin", ProtoUtils.metadataMarshaller(TracErrorDetails.getDefaultInstance()));

    protected TracMetadataApiGrpc.TracMetadataApiBlockingStub publicApi;
    protected InternalMetadataApiGrpc.InternalMetadataApiBlockingStub internalApi;

    static class UnitTest extends ControlledTagApiTest {

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
                .runDbDeploy(true)
                .startService(TracMetadataService.class)
                .build();

        @BeforeEach
        void setup() {
            publicApi = platform.metaClientBlocking();
            internalApi = platform.metaClientInternalBlocking();
        }
    }

    @org.junit.jupiter.api.Tag("integration")
    @org.junit.jupiter.api.Tag("int-metadb")
    static class IntegrationTest extends ControlledTagApiTest {

        private static final String TRAC_CONFIG_ENV_FILE = System.getenv(TRAC_CONFIG_ENV_VAR);

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_ENV_FILE)
                .runDbDeploy(false)
                .startService(TracMetadataService.class)
                .build();

        @BeforeEach
        void setup() {
            publicApi = platform.metaClientBlocking();
            internalApi = platform.metaClientInternalBlocking();
        }
    }

    @AfterEach
    void clearHookCallback() {
        TestMetadataWriteHook.tagWriteCallback = null;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // CONTROLLED WRITES FOR ONE VERSION
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void thisVersion_set() {

        var v1 = createObject();
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("Approved at MGC")));

        var tag = read(t2);
        var attrs = tag.getAttrsMap();

        assertEquals("approved", MetadataCodec.decodeStringValue(attrs.get("ctl_this_approved_mgc")));
        assertEquals("Approved at MGC", MetadataCodec.decodeStringValue(attrs.get("ctl_this_approved_mgc__reason")));
        assertLineage(tag, "ctl_this_approved_mgc");
        assertFalse(attrs.containsKey("approved_mgc"));

        // Who wrote the object version is unchanged
        var priorAttrs = read(v1).getAttrsMap();
        assertEquals(priorAttrs.get(MetadataConstants.TRAC_UPDATE_TIME), attrs.get(MetadataConstants.TRAC_UPDATE_TIME));
        assertEquals(priorAttrs.get(MetadataConstants.TRAC_UPDATE_USER_ID), attrs.get(MetadataConstants.TRAC_UPDATE_USER_ID));
    }

    @Test
    void thisVersion_setReasonArray() {

        var v1 = createObject();
        var reason = MetadataCodec.encodeArrayValue(List.of("Approved at MGC", "Minutes item 4"), String.class);
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason));

        var storedReason = read(t2).getAttrsOrThrow("ctl_this_approved_mgc__reason");
        assertEquals(List.of("Approved at MGC", "Minutes item 4"), MetadataCodec.decodeArrayValue(storedReason));
    }

    @Test
    void thisVersion_clearKeepsLineage() {

        var v1 = createObject();
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("Approved")));
        var t3 = publicApi.updateTag(controlled(t2, VersionScope.VERSION_SCOPE_THIS_VERSION, clear("approved_mgc"), reason("Withdrawn")));

        var tag = read(t3);

        assertFalse(tag.containsAttrs("ctl_this_approved_mgc"));
        assertEquals("Withdrawn", MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("ctl_this_approved_mgc__reason")));
        assertLineage(tag, "ctl_this_approved_mgc");
    }

    @Test
    void thisVersion_clearWithNoValue() {

        var v1 = createObject();

        var neverSet = controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, clear("approved_mgc"), reason("Withdrawn"));
        assertCode(Status.Code.FAILED_PRECONDITION, () -> publicApi.updateTag(neverSet));

        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("Approved")));
        var t3 = publicApi.updateTag(controlled(t2, VersionScope.VERSION_SCOPE_THIS_VERSION, clear("approved_mgc"), reason("Withdrawn")));

        var alreadyCleared = controlled(t3, VersionScope.VERSION_SCOPE_THIS_VERSION, clear("approved_mgc"), reason("Withdrawn"));
        assertCode(Status.Code.FAILED_PRECONDITION, () -> publicApi.updateTag(alreadyCleared));

        assertEquals(t3, latestTag(v1).getHeader());
    }

    @Test
    void thisVersion_setCannotChangeType() {

        var v1 = createObject();
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("Approved")));

        var intUpdate = TagUpdate.newBuilder().setAttrName("approved_mgc").setValue(MetadataCodec.encodeValue(1)).build();
        assertCode(Status.Code.FAILED_PRECONDITION, () -> publicApi.updateTag(controlled(t2, VersionScope.VERSION_SCOPE_THIS_VERSION, intUpdate, reason("Approved"))));
    }

    @Test
    void thisVersion_internalApi() {

        var v1 = createObject();
        var t2 = internalApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("Approved")));

        assertEquals("approved", MetadataCodec.decodeStringValue(read(t2).getAttrsOrThrow("ctl_this_approved_mgc")));
    }

    @Test
    void ordinaryEdit_keepsGroupAndRecordsNoUser() {

        var v1 = createObject();
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("Approved")));
        var t3 = publicApi.updateTag(ordinary(t2, set("description", "Edited")));

        var before = read(t2).getAttrsMap();
        var after = read(t3).getAttrsMap();

        for (var attr : before.keySet())
            if (attr.startsWith("ctl_"))
                assertEquals(before.get(attr), after.get(attr), attr);

        assertEquals(before.get(MetadataConstants.TRAC_UPDATE_USER_ID), after.get(MetadataConstants.TRAC_UPDATE_USER_ID));
        assertEquals(before.get(MetadataConstants.TRAC_UPDATE_TIME), after.get(MetadataConstants.TRAC_UPDATE_TIME));
    }

    @Test
    void clearAll_keepsControlledGroups() {

        var v1 = createObject();
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("Approved")));
        var t3 = publicApi.updateTag(ordinary(t2, TagUpdate.newBuilder().setOperation(TagOperation.CLEAR_ALL_ATTR).build()));

        var attrs = read(t3).getAttrsMap();
        assertTrue(attrs.containsKey("ctl_this_approved_mgc"));
        assertTrue(attrs.containsKey("ctl_this_approved_mgc__reason"));
        assertTrue(attrs.containsKey("ctl_this_approved_mgc__user_id"));
        assertFalse(attrs.containsKey("object_attr"));
    }

    @Test
    void search_findsControlledAttrs() {

        var v1 = createObject();
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 1), reason("Preferred")));

        var search = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                .setAttrName("ctl_all_preferred_version")
                .setAttrType(BasicType.INTEGER)
                .setOperator(SearchOperator.EXISTS))))
                .build();

        var results = publicApi.search(search).getSearchResultList();
        assertTrue(results.stream().anyMatch(tag -> tag.getHeader().equals(t2)));
    }


    @Test
    void search_valueOfThisVersionAttr() {

        var v1 = createObject();
        var value = "approved " + UUID.randomUUID();
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", value), reason("x")));

        var results = search(SearchTerm.newBuilder()
                .setAttrName("ctl_this_approved_mgc")
                .setAttrType(BasicType.STRING)
                .setOperator(SearchOperator.EQ)
                .setSearchValue(MetadataCodec.encodeValue(value)), false);

        assertEquals(List.of(t2), results);
    }

    @Test
    void search_thisVersionAttrOnOlderVersion() {

        var v1 = createObject();
        var value = "approved " + UUID.randomUUID();
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", value), reason("x")));
        publicApi.updateObject(newVersionRequest(t2));

        var term = SearchTerm.newBuilder()
                .setAttrName("ctl_this_approved_mgc")
                .setAttrType(BasicType.STRING)
                .setOperator(SearchOperator.EQ)
                .setSearchValue(MetadataCodec.encodeValue(value));

        // The latest version does not hold the tag, a search of prior versions finds the version that does
        assertEquals(List.of(), search(term, false));
        assertEquals(List.of(t2.toBuilder().setIsLatestObject(false).build()), search(term, true));
    }

    @Test
    void read_asOfBeforeLaterWrites() throws Exception {

        var v1 = createObject();
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("Approved")));
        var setTag = read(t2);

        Thread.sleep(10);
        var t3 = publicApi.updateTag(controlled(t2, VersionScope.VERSION_SCOPE_THIS_VERSION, clear("approved_mgc"), reason("Withdrawn")));
        assertFalse(read(t3).containsAttrs("ctl_this_approved_mgc"));

        // As of the set, the group is as it was then
        var asOf = publicApi.readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(v1.getObjectType())
                .setObjectId(v1.getObjectId())
                .setObjectVersion(v1.getObjectVersion())
                .setTagAsOf(t2.getTagTimestamp()))
                .build());

        assertEquals(t2.getTagVersion(), asOf.getHeader().getTagVersion());
        assertEquals(setTag.getAttrsMap(), asOf.getAttrsMap());
    }

    private List<TagHeader> search(SearchTerm.Builder term, boolean priorVersions) {

        var request = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersions(priorVersions)
                .setSearch(SearchExpression.newBuilder().setTerm(term)))
                .build();

        var headers = new ArrayList<TagHeader>();
        publicApi.search(request).getSearchResultList().forEach(tag -> headers.add(tag.getHeader()));

        return headers;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // REQUEST VALIDATION
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void validation_tagUpdateCount() {

        var v1 = createObject();

        var none = controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("Approved"))
                .toBuilder().clearTagUpdates().build();

        var two = controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("Approved"))
                .toBuilder().addTagUpdates(set("approved_dq", "approved")).build();

        assertInvalidEverywhere(none, "A controlled tag write must contain exactly one item in [tagUpdates] (found 0)");
        assertInvalidEverywhere(two, "A controlled tag write must contain exactly one item in [tagUpdates] (found 2)");
        assertEquals(v1, latestTag(v1).getHeader());
    }

    @Test
    void validation_operations() {

        var v1 = createObject();

        for (var operation : List.of(TagOperation.CREATE_ATTR, TagOperation.REPLACE_ATTR,
                TagOperation.APPEND_ATTR, TagOperation.CREATE_OR_APPEND_ATTR)) {

            var update = set("approved_mgc", "approved").toBuilder().setOperation(operation).build();
            assertInvalidEverywhere(
                    controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, update, reason("Approved")),
                    "Operation [" + operation.name() + "] is not allowed for a controlled tag write");
        }

        var clearAll = TagUpdate.newBuilder().setOperation(TagOperation.CLEAR_ALL_ATTR).build();
        assertInvalidEverywhere(
                controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, clearAll, reason("Approved")),
                "Operation [CLEAR_ALL_ATTR] is not allowed for a controlled tag write");
    }

    @Test
    void validation_reason() {

        var v1 = createObject();
        var update = set("approved_mgc", "approved");

        var noReason = controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, update, reason("x")).toBuilder().clearReason().build();
        assertInvalidEverywhere(noReason, "A value is required for [reason] when control == CONTROLLED");

        var empty = "The reason for a controlled tag write cannot be empty";
        assertInvalidEverywhere(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, update, reason("")), empty);
        assertInvalidEverywhere(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, update, MetadataCodec.encodeArrayValue(List.<String>of(), String.class)), empty);
        assertInvalidEverywhere(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, update, MetadataCodec.encodeArrayValue(List.of("ok", ""), String.class)), empty);

        assertInvalidEverywhere(
                controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, update, MetadataCodec.encodeValue(42)),
                "The reason for a controlled tag write must be a string or an array of strings");

        assertInvalidEverywhere(
                controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, update, reason("x".repeat(1001))),
                "The reason for a controlled tag write exceeds the length limit (1000 characters)");

        assertInvalidEverywhere(
                controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, update, reasonItems(21)),
                "The reason for a controlled tag write has too many items (limit is 20)");

        // Reason on an ordinary write
        var ordinaryWithReason = ordinary(v1, set("description", "x")).toBuilder().setReason(reason("Why")).build();
        assertInvalidEverywhere(ordinaryWithReason, "A value cannot be given for [reason] unless control == CONTROLLED");

        // At the limits
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, update, reason("x".repeat(1000))));
        assertDoesNotThrow(() -> publicApi.updateTag(controlled(t2, VersionScope.VERSION_SCOPE_THIS_VERSION, update, reasonItems(20))));
    }

    @Test
    void validation_attrName() {

        var v1 = createObject();

        var refused = List.of(
                List.of("trac_approved", "is a reserved identifier"),
                List.of("TRAC_approved", "is a reserved identifier"),
                List.of("_approved", "is a reserved identifier"),
                List.of("ctl_this_approved", "is reserved for controlled attributes"),
                List.of("CTL_all_approved", "is reserved for controlled attributes"),
                List.of("approved__reason", "cannot contain a double underscore in a controlled tag write"),
                List.of("not-an-identifier", "is not a valid identifier"));

        for (var item : refused) {

            var update = set("placeholder", "approved").toBuilder().setAttrName(item.get(0)).build();

            assertInvalidEverywhere(
                    controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, update, reason("Approved")),
                    item.get(1));
        }
    }

    @Test
    void validation_attrNameLength() {

        var v1 = createObject();

        // The longest stored name is the this-version prefix, the name and the longest lineage suffix
        var atLimit = "a".repeat(ValidationConstants.CONTROLLED_ATTR_NAME_LENGTH_LIMIT);
        var overLimit = atLimit + "a";

        var tooLong = set("placeholder", "approved").toBuilder().setAttrName(overLimit).build();

        assertInvalidEverywhere(
                controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, tooLong, reason("Approved")),
                "exceeds the length limit for a controlled tag write (236 characters)");

        for (var versionScope : List.of(VersionScope.VERSION_SCOPE_THIS_VERSION, VersionScope.VERSION_SCOPE_ALL_VERSIONS)) {

            var prior = latestTag(v1).getHeader();
            var update = set("placeholder", "approved").toBuilder().setAttrName(atLimit).build();
            var saved = read(publicApi.updateTag(controlled(prior, versionScope, update, reason("Approved"))));

            var storedName = ObjectUpdateLogic.controlledAttrName(atLimit, versionScope);
            assertTrue(saved.containsAttrs(storedName));
            assertTrue(saved.containsAttrs(storedName + MetadataConstants.CONTROLLED_USER_NAME_SUFFIX));
        }
    }

    @Test
    void validation_enums() {

        var v1 = createObject();
        var update = set("approved_mgc", "approved");

        // All versions is only available for controlled writes
        var ordinaryAllVersions = ordinary(v1, update).toBuilder().setVersionScope(VersionScope.VERSION_SCOPE_ALL_VERSIONS).build();
        assertInvalidEverywhere(ordinaryAllVersions, "A value cannot be given for [versionScope] unless control == CONTROLLED");

        var badControl = ordinary(v1, update).toBuilder().setControlValue(99).build();
        var badScope = controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, update, reason("x")).toBuilder().setVersionScopeValue(99).build();

        assertInvalidEverywhere(badControl, "Unrecognised value specified for [control]: [99]");
        assertInvalidEverywhere(badScope, "Unrecognised value specified for [versionScope]: [99]");
    }

    @Test
    void validation_configObjects() {

        var configRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CONFIG)
                .setDefinition(SampleMetadata.dummyConfigDef())
                .build();

        var configId = internalApi.createObject(configRequest);

        var request = controlled(configId, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("x"));
        assertInvalidEverywhere(request, "Controlled tag writes are not available for [CONFIG] objects");

        var allVersions = controlled(configId, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("approved_mgc", "approved"), reason("x"));
        assertInvalidEverywhere(allVersions, "Controlled tag writes are not available for [CONFIG] objects");
    }

    @Test
    void validation_controlledNamesRefusedOnOrdinaryWrites() {

        var v1 = createObject();

        for (var name : List.of("ctl_this_approved_mgc", "CTL_this_approved_mgc", "ctl_all_preferred_version", "ctl_this_x__user_id")) {

            var update = set("placeholder", "x").toBuilder().setAttrName(name).build();
            var reserved = "Attribute name [" + name + "] is reserved for controlled attributes";

            // updateTag on the public and internal APIs
            assertInvalidEverywhere(ordinary(v1, update), reserved);

            // createObject and updateObject
            var create = MetadataWriteRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setObjectType(ObjectType.CUSTOM)
                    .setDefinition(SampleMetadata.dummyCustomDef())
                    .addTagUpdates(update)
                    .build();

            assertInvalid(reserved, () -> publicApi.createObject(create));
            assertInvalid(reserved, () -> internalApi.createObject(create));

            var newVersion = MetadataWriteRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setObjectType(ObjectType.CUSTOM)
                    .setPriorVersion(MetadataUtil.selectorFor(v1))
                    .setDefinition(SampleMetadata.nextCustomDef(read(v1).getDefinition()))
                    .addTagUpdates(update)
                    .build();

            assertInvalid(reserved, () -> publicApi.updateObject(newVersion));
            assertInvalid(reserved, () -> internalApi.updateObject(newVersion));

            // Batch items, including preallocated objects
            var tagBatch = MetadataWriteBatchRequest.newBuilder().setTenant(TEST_TENANT)
                    .addUpdateTags(ordinary(v1, update).toBuilder().clearTenant()).build();

            assertInvalid(reserved, () -> publicApi.writeBatch(tagBatch));
            assertInvalid(reserved, () -> internalApi.writeBatch(tagBatch));

            var preallocated = internalApi.preallocateId(MetadataWriteRequest.newBuilder()
                    .setTenant(TEST_TENANT).setObjectType(ObjectType.CUSTOM).build());

            var preallocatedBatch = MetadataWriteBatchRequest.newBuilder().setTenant(TEST_TENANT)
                    .addCreatePreallocatedObjects(MetadataWriteRequest.newBuilder()
                    .setObjectType(ObjectType.CUSTOM)
                    .setPriorVersion(MetadataUtil.preallocated(MetadataUtil.selectorFor(preallocated)))
                    .setDefinition(SampleMetadata.dummyCustomDef())
                    .addTagUpdates(update))
                    .build();

            assertInvalid(reserved, () -> publicApi.writeBatch(preallocatedBatch));
            assertInvalid(reserved, () -> internalApi.writeBatch(preallocatedBatch));

            var createBatch = MetadataWriteBatchRequest.newBuilder().setTenant(TEST_TENANT)
                    .addCreateObjects(create.toBuilder().clearTenant()).build();

            assertInvalid(reserved, () -> publicApi.writeBatch(createBatch));
            assertInvalid(reserved, () -> internalApi.writeBatch(createBatch));

            var versionBatch = MetadataWriteBatchRequest.newBuilder().setTenant(TEST_TENANT)
                    .addUpdateObjects(newVersion.toBuilder().clearTenant()).build();

            assertInvalid(reserved, () -> publicApi.writeBatch(versionBatch));
            assertInvalid(reserved, () -> internalApi.writeBatch(versionBatch));

            var preallocatedSingle = MetadataWriteRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setObjectType(ObjectType.CUSTOM)
                    .setPriorVersion(MetadataUtil.preallocated(MetadataUtil.selectorFor(preallocated)))
                    .setDefinition(SampleMetadata.dummyCustomDef())
                    .addTagUpdates(update)
                    .build();

            assertInvalid(reserved, () -> internalApi.createPreallocatedObject(preallocatedSingle));
        }

        assertEquals(v1, latestTag(v1).getHeader());
    }

    @Test
    void validation_controlFieldsOnlyOnUpdateTag() {

        var v1 = createObject();

        var create = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setDefinition(SampleMetadata.dummyCustomDef())
                .build();

        assertInvalid("A value cannot be given for [control]", () -> publicApi.createObject(create.toBuilder().setControl(TagControl.CONTROLLED).build()));
        assertInvalid("A value cannot be given for [reason]", () -> internalApi.createObject(create.toBuilder().setReason(reason("x")).build()));

        var newVersion = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(MetadataUtil.selectorFor(v1))
                .setDefinition(SampleMetadata.nextCustomDef(read(v1).getDefinition()))
                .setVersionScope(VersionScope.VERSION_SCOPE_ALL_VERSIONS)
                .build();

        assertInvalid("A value cannot be given for [versionScope]", () -> publicApi.updateObject(newVersion));
        assertInvalid("A value cannot be given for [versionScope]", () -> internalApi.updateObject(newVersion));

        // Preallocation requests
        var preallocate = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setControl(TagControl.CONTROLLED)
                .build();

        assertInvalid("A value cannot be given for [control]", () -> internalApi.preallocateId(preallocate));

        var preallocated = internalApi.preallocateId(preallocate.toBuilder().clearControl().build());

        var createPreallocated = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(MetadataUtil.preallocated(MetadataUtil.selectorFor(preallocated)))
                .setDefinition(SampleMetadata.dummyCustomDef())
                .setVersionScope(VersionScope.VERSION_SCOPE_ALL_VERSIONS)
                .build();

        assertInvalid("A value cannot be given for [versionScope]", () -> internalApi.createPreallocatedObject(createPreallocated));

        var preallocatedBatch = MetadataWriteBatchRequest.newBuilder().setTenant(TEST_TENANT)
                .addCreatePreallocatedObjects(createPreallocated.toBuilder().clearTenant().clearVersionScope().setReason(reason("x")))
                .build();

        assertInvalid("A value cannot be given for [reason]", () -> internalApi.writeBatch(preallocatedBatch));

        // A controlled write is never an item in a batch
        var controlledItem = controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("x"));
        var batch = MetadataWriteBatchRequest.newBuilder().setTenant(TEST_TENANT)
                .addUpdateTags(controlledItem.toBuilder().clearTenant()).build();

        assertInvalid("A value cannot be given for [control]", () -> publicApi.writeBatch(batch));
        assertInvalid("A value cannot be given for [control]", () -> internalApi.writeBatch(batch));

        assertEquals(v1, latestTag(v1).getHeader());
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ALL VERSIONS
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void allVersions_set() {

        var v3 = createVersions(3);
        var result = publicApi.updateTag(controlled(v3, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 2), reason("Reviewed")));

        assertEquals(3, result.getObjectVersion());
        assertEquals(v3.getTagVersion() + 1, result.getTagVersion());

        var lineage = new ArrayList<Value>();

        for (var version = 1; version <= 3; version++) {

            var tag = latestTag(v3, version);
            assertEquals(2, MetadataCodec.decodeIntegerValue(tag.getAttrsOrThrow("ctl_all_preferred_version")));
            assertEquals("Reviewed", MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("ctl_all_preferred_version__reason")));
            lineage.add(tag.getAttrsOrThrow("ctl_all_preferred_version__time"));
        }

        assertEquals(1, lineage.stream().distinct().count());
    }

    @Test
    void allVersions_moveAndClear() {

        var v2 = createVersions(2);
        var t2 = publicApi.updateTag(controlled(v2, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 1), reason("First")));
        var t3 = publicApi.updateTag(controlled(t2, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 2), reason("Moved")));
        var t4 = publicApi.updateTag(controlled(t3, VersionScope.VERSION_SCOPE_ALL_VERSIONS, clear("preferred_version"), reason("Cleared")));

        for (var version = 1; version <= 2; version++) {
            var tag = latestTag(v2, version);
            assertFalse(tag.containsAttrs("ctl_all_preferred_version"));
            assertEquals("Cleared", MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("ctl_all_preferred_version__reason")));
        }

        var nothingToClear = controlled(t4, VersionScope.VERSION_SCOPE_ALL_VERSIONS, clear("preferred_version"), reason("Again"));
        assertCode(Status.Code.FAILED_PRECONDITION, () -> publicApi.updateTag(nothingToClear));
    }

    @Test
    void allVersions_notLatestVersion() {

        var v2 = createVersions(2);
        var v1 = latestTag(v2, 1).getHeader();

        var request = controlled(v1, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 1), reason("x"));
        assertCode(Status.Code.FAILED_PRECONDITION, () -> publicApi.updateTag(request));
        assertFalse(latestTag(v2, 1).containsAttrs("ctl_all_preferred_version"));
    }

    @Test
    void allVersions_staleTag() {

        var v2 = createVersions(2);
        publicApi.updateTag(ordinary(v2, set("description", "Edited")));

        var request = controlled(v2, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 1), reason("x"));
        assertCode(Status.Code.ALREADY_EXISTS, () -> publicApi.updateTag(request));

        assertFalse(latestTag(v2, 1).containsAttrs("ctl_all_preferred_version"));
        assertFalse(latestTag(v2, 2).containsAttrs("ctl_all_preferred_version"));
    }

    @Test
    void allVersions_typeMismatch() {

        var v2 = createVersions(2);
        var t2 = publicApi.updateTag(controlled(v2, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 1), reason("x")));

        var request = controlled(t2, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", "one"), reason("x"));
        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.updateTag(request));

        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
        assertTrue(error.getStatus().getDescription().contains("object version [1]"));
        assertEquals(t2, latestTag(v2, 2).getHeader());
    }

    @Test
    void allVersions_staleOlderVersion() {

        var v2 = createVersions(2);
        var v1 = latestTag(v2, 1).getHeader();
        var fired = new AtomicBoolean(false);

        // Another write to version 1 lands after the versions are loaded and before they are saved
        onTagWrite(write -> {
            if (write.versionScope() == VersionScope.VERSION_SCOPE_ALL_VERSIONS && fired.compareAndSet(false, true))
                publicApi.updateTag(ordinary(v1, set("description", "Edited meanwhile")));
        });

        var request = controlled(v2, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 1), reason("x"));
        var error = assertThrows(StatusRuntimeException.class, () -> publicApi.updateTag(request));

        assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
        assertTrue(error.getStatus().getDescription().startsWith("Prior tag has been superseded for one or more tag updates"));

        assertFalse(latestTag(v2, 1).containsAttrs("ctl_all_preferred_version"));
        assertEquals(v2, latestTag(v2, 2).getHeader());
    }

    @Test
    void allVersions_raceWithNewVersionConverges() {

        var v2 = createVersions(2);
        var fired = new AtomicBoolean(false);
        var v3 = new TagHeader[1];

        // A new version is saved after the versions are loaded, it copies version 2 without the value
        onTagWrite(write -> {
            if (write.versionScope() == VersionScope.VERSION_SCOPE_ALL_VERSIONS && fired.compareAndSet(false, true))
                v3[0] = publicApi.updateObject(newVersionRequest(v2));
        });

        publicApi.updateTag(controlled(v2, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 1), reason("x")));

        assertNotNull(v3[0]);
        assertTrue(latestTag(v2, 1).containsAttrs("ctl_all_preferred_version"));
        assertFalse(latestTag(v2, 3).containsAttrs("ctl_all_preferred_version"));

        // The latest version missed the set, a clear is still allowed and converges every version
        var clear = publicApi.updateTag(controlled(latestTag(v2, 3).getHeader(), VersionScope.VERSION_SCOPE_ALL_VERSIONS, clear("preferred_version"), reason("Cleared")));
        assertEquals(3, clear.getObjectVersion());

        for (var version = 1; version <= 3; version++) {
            var tag = latestTag(v2, version);
            assertFalse(tag.containsAttrs("ctl_all_preferred_version"));
            assertEquals("Cleared", MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("ctl_all_preferred_version__reason")));
        }
    }

    @Test
    void allVersions_versionLimit() {

        var latest = createVersions(1000);

        var atLimit = publicApi.updateTag(controlled(latest, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 1000), reason("x")));
        assertEquals(1000, MetadataCodec.decodeIntegerValue(latestTag(latest, 1).getAttrsOrThrow("ctl_all_preferred_version")));

        var overLimit = publicApi.updateObject(newVersionRequest(atLimit));

        var request = controlled(overLimit, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 1001), reason("x"));
        assertCode(Status.Code.INVALID_ARGUMENT, () -> publicApi.updateTag(request));
        assertEquals(overLimit, latestTag(overLimit).getHeader());
    }


    // -----------------------------------------------------------------------------------------------------------------
    // NEW VERSIONS
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void newVersion_dropsThisVersionAndCopiesAllVersions() {

        var v1 = createObject();
        var t2 = publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("x")));
        var t3 = publicApi.updateTag(controlled(t2, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 1), reason("y")));

        var v2 = publicApi.updateObject(newVersionRequest(t3));
        var attrs = read(v2).getAttrsMap();

        assertTrue(attrs.keySet().stream().noneMatch(attr -> attr.startsWith("ctl_this_")));
        assertEquals(1, MetadataCodec.decodeIntegerValue(attrs.get("ctl_all_preferred_version")));
        assertEquals(read(t3).getAttrsOrThrow("ctl_all_preferred_version__time"), attrs.get("ctl_all_preferred_version__time"));
    }

    @Test
    void newVersion_allVersionsFromLatestTag() {

        var v1 = createObject();
        publicApi.updateTag(controlled(v1, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 1), reason("Set after the user opened t1")));

        // The new version names tag 1, opened before the set
        var v2 = publicApi.updateObject(newVersionRequest(v1));
        var attrs = read(v2).getAttrsMap();

        assertEquals(1, MetadataCodec.decodeIntegerValue(attrs.get("ctl_all_preferred_version")));
        assertEquals("Set after the user opened t1", MetadataCodec.decodeStringValue(attrs.get("ctl_all_preferred_version__reason")));
    }

    @Test
    void newVersion_olderTagWithoutControlledAttrsUnchanged() {

        var v1 = createObject();
        publicApi.updateTag(ordinary(v1, set("description", "Edited after the user opened t1")));

        var v2 = publicApi.updateObject(newVersionRequest(v1));

        var priorAttrs = read(v1).getAttrsMap();
        var newAttrs = read(v2).getAttrsMap();

        // The new version copies the tag it names, as before
        assertEquals(priorAttrs.get("description"), newAttrs.get("description"));
        assertTrue(newAttrs.keySet().stream().noneMatch(attr -> attr.startsWith("ctl_")));

        for (var attr : priorAttrs.keySet())
            if (!attr.startsWith("trac_update_"))
                assertEquals(priorAttrs.get(attr), newAttrs.get(attr), attr);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // HOOK
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void hook_calledForControlledAndOrdinaryWrites() {

        var v2 = createVersions(2);

        TestMetadataWriteHook.TAG_WRITES.clear();

        var t2 = publicApi.updateTag(controlled(v2, VersionScope.VERSION_SCOPE_THIS_VERSION, set("approved_mgc", "approved"), reason("x")));
        publicApi.updateTag(controlled(t2, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set("preferred_version", 2), reason("y")));

        var writes = TestMetadataWriteHook.TAG_WRITES.stream()
                .filter(write -> write.priorTag().getHeader().getObjectId().equals(v2.getObjectId()))
                .toArray(org.finos.tracdap.common.service.TagWrite[]::new);

        assertEquals(2, writes.length);

        assertEquals(TagControl.CONTROLLED, writes[0].control());
        assertEquals(VersionScope.VERSION_SCOPE_THIS_VERSION, writes[0].versionScope());
        assertEquals("approved_mgc", writes[0].tagUpdates().get(0).getAttrName());
        assertEquals(0, writes[0].latestObjectVersion());
        assertFalse(writes[0].inBatch());
        assertNotNull(writes[0].reader());
        assertNotNull(writes[0].userMetadata());

        // One call for a write to all versions, with the latest version number
        assertEquals(VersionScope.VERSION_SCOPE_ALL_VERSIONS, writes[1].versionScope());
        assertEquals(2, writes[1].latestObjectVersion());
        assertEquals(t2, writes[1].priorTag().getHeader());
    }

    @Test
    void hook_calledForEachBatchItem() {

        var a = createObject();
        var b = createObject();

        TestMetadataWriteHook.TAG_WRITES.clear();

        publicApi.writeBatch(MetadataWriteBatchRequest.newBuilder().setTenant(TEST_TENANT)
                .addUpdateTags(ordinary(a, set("description", "a")).toBuilder().clearTenant())
                .addUpdateTags(ordinary(b, set("description", "b")).toBuilder().clearTenant())
                .build());

        var writes = TestMetadataWriteHook.TAG_WRITES.stream()
                .filter(write -> List.of(a.getObjectId(), b.getObjectId()).contains(write.priorTag().getHeader().getObjectId()))
                .toArray(org.finos.tracdap.common.service.TagWrite[]::new);

        assertEquals(2, writes.length);
        assertTrue(writes[0].inBatch());
        assertTrue(writes[1].inBatch());
        assertEquals(TagControl.CONTROL_NONE, writes[0].control());
    }

    @Test
    void hook_refusalSavesNothing() {

        var v2 = createVersions(2);

        var thisVersion = controlled(v2, VersionScope.VERSION_SCOPE_THIS_VERSION, set(TestMetadataWriteHook.REFUSED_TAG_ATTR, "x"), reason("x"));
        var allVersions = controlled(v2, VersionScope.VERSION_SCOPE_ALL_VERSIONS, set(TestMetadataWriteHook.REFUSED_TAG_ATTR, "x"), reason("x"));
        var ordinaryWrite = ordinary(v2, set(TestMetadataWriteHook.REFUSED_TAG_ATTR, "x"));

        assertCode(Status.Code.FAILED_PRECONDITION, () -> publicApi.updateTag(thisVersion));
        assertCode(Status.Code.FAILED_PRECONDITION, () -> publicApi.updateTag(allVersions));
        assertCode(Status.Code.FAILED_PRECONDITION, () -> publicApi.updateTag(ordinaryWrite));

        assertEquals(v2, latestTag(v2, 2).getHeader());
        assertEquals(1, latestTag(v2, 1).getHeader().getTagVersion());
    }

    @Test
    void hook_batchItemRefusalSavesNothing() {

        var a = createObject();
        var b = createObject();

        var batch = MetadataWriteBatchRequest.newBuilder().setTenant(TEST_TENANT)
                .addUpdateTags(ordinary(a, set("description", "a")).toBuilder().clearTenant())
                .addUpdateTags(ordinary(b, set(TestMetadataWriteHook.REFUSED_TAG_ATTR, "x")).toBuilder().clearTenant())
                .build();

        assertCode(Status.Code.FAILED_PRECONDITION, () -> publicApi.writeBatch(batch));

        assertEquals(a, latestTag(a).getHeader());
        assertEquals(b, latestTag(b).getHeader());
    }


    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS
    // -----------------------------------------------------------------------------------------------------------------

    private TagHeader createObject() {

        var request = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setDefinition(SampleMetadata.dummyCustomDef())
                .addTagUpdates(set("object_attr", "object " + UUID.randomUUID()))
                .build();

        return publicApi.createObject(request);
    }

    private TagHeader createVersions(int nVersions) {

        var header = createObject();

        for (var version = 2; version <= nVersions; version++)
            header = publicApi.updateObject(newVersionRequest(header));

        return header;
    }

    private MetadataWriteRequest newVersionRequest(TagHeader prior) {

        return MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(MetadataUtil.selectorFor(prior))
                .setDefinition(SampleMetadata.nextCustomDef(read(prior).getDefinition()))
                .build();
    }

    private static MetadataWriteRequest controlled(TagHeader prior, VersionScope versionScope, TagUpdate update, Value reason) {

        return MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(prior.getObjectType())
                .setPriorVersion(MetadataUtil.selectorFor(prior))
                .setControl(TagControl.CONTROLLED)
                .setVersionScope(versionScope)
                .addTagUpdates(update)
                .setReason(reason)
                .build();
    }

    private static MetadataWriteRequest ordinary(TagHeader prior, TagUpdate update) {

        return MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(prior.getObjectType())
                .setPriorVersion(MetadataUtil.selectorFor(prior))
                .addTagUpdates(update)
                .build();
    }

    private static TagUpdate set(String attrName, String value) {
        return TagUpdate.newBuilder().setAttrName(attrName).setValue(MetadataCodec.encodeValue(value)).build();
    }

    private static TagUpdate set(String attrName, int value) {
        return TagUpdate.newBuilder().setAttrName(attrName).setValue(MetadataCodec.encodeValue(value)).build();
    }

    private static TagUpdate clear(String attrName) {
        return TagUpdate.newBuilder().setOperation(TagOperation.DELETE_ATTR).setAttrName(attrName).build();
    }

    private static Value reason(String reason) {
        return MetadataCodec.encodeValue(reason);
    }

    private static Value reasonItems(int nItems) {

        var items = new ArrayList<String>();

        for (var i = 0; i < nItems; i++)
            items.add("Reason " + i);

        return MetadataCodec.encodeArrayValue(items, String.class);
    }

    private Tag read(TagHeader header) {

        return publicApi.readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(header))
                .build());
    }

    private Tag latestTag(TagHeader header) {

        return latestTag(header, header.getObjectVersion());
    }

    private Tag latestTag(TagHeader header, int objectVersion) {

        return publicApi.readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(TagSelector.newBuilder()
                .setObjectType(header.getObjectType())
                .setObjectId(header.getObjectId())
                .setObjectVersion(objectVersion)
                .setLatestTag(true))
                .build());
    }

    private void assertLineage(Tag tag, String storedName) {

        var attrs = tag.getAttrsMap();

        assertEquals(
                attrs.get(MetadataConstants.TRAC_CREATE_USER_ID),
                attrs.get(storedName + MetadataConstants.CONTROLLED_USER_ID_SUFFIX));

        assertEquals(
                attrs.get(MetadataConstants.TRAC_CREATE_USER_NAME),
                attrs.get(storedName + MetadataConstants.CONTROLLED_USER_NAME_SUFFIX));

        var time = MetadataCodec.decodeDateTimeValue(attrs.get(storedName + MetadataConstants.CONTROLLED_TIME_SUFFIX));
        assertEquals(MetadataCodec.decodeDatetime(tag.getHeader().getTagTimestamp()), time);
    }

    private void assertInvalidEverywhere(MetadataWriteRequest request, String expectedMessage) {

        assertInvalid(expectedMessage, () -> publicApi.updateTag(request));
        assertInvalid(expectedMessage, () -> internalApi.updateTag(request));
    }

    private static void assertInvalid(String expectedMessage, Runnable call) {

        var error = assertThrows(StatusRuntimeException.class, call::run);
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode(), error.getStatus().getDescription());

        var trailers = error.getTrailers();
        var details = trailers != null ? trailers.get(TRAC_ERROR_DETAILS_KEY) : null;

        var messages = details != null
                ? details.getItemsList().stream().map(TracErrorItem::getDetail).collect(Collectors.toList())
                : new ArrayList<String>();

        messages.add(error.getStatus().getDescription());

        assertTrue(
                messages.stream().anyMatch(message -> message != null && message.contains(expectedMessage)),
                String.format("Expected an error containing [%s], got %s", expectedMessage, messages));
    }

    private static void assertCode(Status.Code code, Runnable call) {

        var error = assertThrows(StatusRuntimeException.class, call::run);
        assertEquals(code, error.getStatus().getCode(), error.getStatus().getDescription());
    }

    private static void onTagWrite(Consumer<org.finos.tracdap.common.service.TagWrite> callback) {
        TestMetadataWriteHook.tagWriteCallback = callback;
    }
}
