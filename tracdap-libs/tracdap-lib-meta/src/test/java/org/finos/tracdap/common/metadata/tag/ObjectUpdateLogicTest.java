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

package org.finos.tracdap.common.metadata.tag;

import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.exception.EMetadataBadUpdate;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.Set;


class ObjectUpdateLogicTest {

    @Test
    void structuredModelAttrs_standardModel() {

        var modelDef = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setVersion("v1.0.0")
                .setEntryPoint("acme.models.test_model.BasicTestModel")
                .setModelType(ModelType.STANDARD_MODEL)
                .build();

        var attrs = attrsByName(ObjectUpdateLogic.structuredModelAttrs(modelDef));

        assertEquals("STANDARD_MODEL", MetadataCodec.decodeStringValue(attrs.get(MetadataConstants.TRAC_MODEL_TYPE)));
    }

    @Test
    void structuredModelAttrs_dataImportModel() {

        var modelDef = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setVersion("v1.0.0")
                .setEntryPoint("acme.models.test_model.BasicDataImportModel")
                .setModelType(ModelType.DATA_IMPORT_MODEL)
                .build();

        var attrs = attrsByName(ObjectUpdateLogic.structuredModelAttrs(modelDef));

        assertEquals("DATA_IMPORT_MODEL", MetadataCodec.decodeStringValue(attrs.get(MetadataConstants.TRAC_MODEL_TYPE)));
    }

    @Test
    void structuredModelAttrs_dataExportModel() {

        var modelDef = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("UNIT_TEST_REPO")
                .setVersion("v1.0.0")
                .setEntryPoint("acme.models.test_model.BasicDataExportModel")
                .setModelType(ModelType.DATA_EXPORT_MODEL)
                .build();

        var attrs = attrsByName(ObjectUpdateLogic.structuredModelAttrs(modelDef));

        assertEquals("DATA_EXPORT_MODEL", MetadataCodec.decodeStringValue(attrs.get(MetadataConstants.TRAC_MODEL_TYPE)));
    }

    @Test
    void structuredCustomAttrs_schemaType() {

        var customDef = customDef("acme_widget");

        var attrs = attrsByName(ObjectUpdateLogic.structuredAttrs(customDef, null));

        assertEquals("acme_widget", MetadataCodec.decodeStringValue(attrs.get(MetadataConstants.TRAC_CUSTOM_SCHEMA_TYPE_ATTR)));
    }

    @Test
    void structuredCustomAttrs_setOnCreateAndVersion() {

        var v1 = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), customDef("acme_widget"), List.of(),
                null, requestMetadata(), userMetadata());

        var v2 = ObjectUpdateLogic.buildNewVersion(
                v1, customDef("acme_widget"), List.of(),
                null, requestMetadata(), userMetadata());

        assertEquals("acme_widget", MetadataCodec.decodeStringValue(v1.getAttrsOrThrow(MetadataConstants.TRAC_CUSTOM_SCHEMA_TYPE_ATTR)));
        assertEquals("acme_widget", MetadataCodec.decodeStringValue(v2.getAttrsOrThrow(MetadataConstants.TRAC_CUSTOM_SCHEMA_TYPE_ATTR)));
    }

    @Test
    void structuredJobAttrs_context() {

        var contextId = UUID.randomUUID().toString();

        var jobDef = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setContext(TagSelector.newBuilder()
                        .setObjectType(ObjectType.CUSTOM)
                        .setObjectId(contextId)
                        .setObjectVersion(3)
                        .setLatestTag(true))
                .build();

        var attrs = attrsByName(ObjectUpdateLogic.structuredJobAttrs(jobDef));

        assertEquals("RUN_MODEL", MetadataCodec.decodeStringValue(attrs.get(MetadataConstants.TRAC_JOB_TYPE_ATTR)));
        assertEquals(contextId, MetadataCodec.decodeStringValue(attrs.get(MetadataConstants.TRAC_JOB_CONTEXT_ATTR)));
        assertEquals(3, MetadataCodec.decodeIntegerValue(attrs.get(MetadataConstants.TRAC_JOB_CONTEXT_VERSION_ATTR)));
    }

    @Test
    void structuredJobAttrs_noContext() {

        var jobDef = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .build();

        var attrs = attrsByName(ObjectUpdateLogic.structuredJobAttrs(jobDef));

        assertEquals(Set.of(MetadataConstants.TRAC_JOB_TYPE_ATTR), attrs.keySet());
    }

    @Test
    void buildNewVersion_dropsVersionScopedAttrs() {

        var priorTag = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), customDef("acme_widget"),
                List.of(attr("trac_job_context", "context_id"), intAttr("trac_job_context_version", 1),
                        attr("TRAC_JOB_CONTEXT", "context_id_upper"), attr("trac_job_type", "RUN_MODEL"),
                        attr("trac_job_context_other", "kept"), attr("business_key", "kept")),
                null, requestMetadata(), userMetadata());

        var newTag = ObjectUpdateLogic.buildNewVersion(
                priorTag, customDef("acme_widget"), List.of(),
                null, requestMetadata(), userMetadata());

        assertFalse(newTag.containsAttrs("trac_job_context"));
        assertFalse(newTag.containsAttrs("trac_job_context_version"));
        assertFalse(newTag.containsAttrs("TRAC_JOB_CONTEXT"));

        assertTrue(newTag.containsAttrs("trac_job_type"));
        assertTrue(newTag.containsAttrs("trac_job_context_other"));
        assertTrue(newTag.containsAttrs("business_key"));
        assertTrue(newTag.containsAttrs(MetadataConstants.TRAC_CREATE_TIME));
    }

    @Test
    void buildNewVersion_versionScopedAttrsFromTheRequestKept() {

        var priorTag = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), customDef("acme_widget"),
                List.of(attr("trac_job_context", "prior_context")),
                null, requestMetadata(), userMetadata());

        var newTag = ObjectUpdateLogic.buildNewVersion(
                priorTag, customDef("acme_widget"),
                List.of(attr("trac_job_context", "new_context")),
                null, requestMetadata(), userMetadata());

        assertEquals("new_context", MetadataCodec.decodeStringValue(newTag.getAttrsOrThrow("trac_job_context")));
    }

    @Test
    void buildNewTag_keepsVersionScopedAttrs() {

        var priorTag = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), customDef("acme_widget"),
                List.of(attr("trac_job_context", "context_id"), intAttr("trac_job_context_version", 1)),
                null, requestMetadata(), userMetadata());

        var newTag = ObjectUpdateLogic.buildNewTag(
                priorTag, List.of(attr("business_key", "added")),
                requestMetadata(), userMetadata());

        assertEquals("context_id", MetadataCodec.decodeStringValue(newTag.getAttrsOrThrow("trac_job_context")));
        assertEquals(1, MetadataCodec.decodeIntegerValue(newTag.getAttrsOrThrow("trac_job_context_version")));
    }

    @Test
    void buildNewVersion_dropsThisVersionControlledAttrs() {

        var priorTag = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), customDef("acme_widget"), List.of(attr("business_key", "kept")),
                null, requestMetadata(), userMetadata());

        priorTag = controlledTag(priorTag, VersionScope.VERSION_SCOPE_THIS_VERSION, attr("approved", "yes"));
        priorTag = controlledTag(priorTag, VersionScope.VERSION_SCOPE_ALL_VERSIONS, intAttr("preferred_version", 1));

        var newTag = ObjectUpdateLogic.buildNewVersion(
                priorTag, customDef("acme_widget"), List.of(),
                null, requestMetadata(), userMetadata());

        assertTrue(newTag.getAttrsMap().keySet().stream().noneMatch(name -> name.startsWith("ctl_this_")));
        assertTrue(newTag.containsAttrs("ctl_all_preferred_version"));
        assertTrue(newTag.containsAttrs("ctl_all_preferred_version__reason"));
        assertTrue(newTag.containsAttrs("business_key"));
    }

    @Test
    void buildNewVersion_allVersionsAttrsFromLatestPriorTag() {

        var priorTag = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), customDef("acme_widget"), List.of(attr("business_key", "prior")),
                null, requestMetadata(), userMetadata());

        priorTag = controlledTag(priorTag, VersionScope.VERSION_SCOPE_ALL_VERSIONS, intAttr("preferred_version", 1));

        var latestPriorTag = ObjectUpdateLogic.buildNewTag(priorTag, List.of(attr("business_key", "latest")), requestMetadata(), userMetadata());
        latestPriorTag = controlledTag(latestPriorTag, VersionScope.VERSION_SCOPE_ALL_VERSIONS, clear("preferred_version"));
        latestPriorTag = controlledTag(latestPriorTag, VersionScope.VERSION_SCOPE_ALL_VERSIONS, attr("housekeeping", "keep"));

        var newTag = ObjectUpdateLogic.buildNewVersion(
                priorTag, latestPriorTag, customDef("acme_widget"), List.of(),
                null, requestMetadata(), userMetadata());

        // Controlled attributes for all versions come from the latest tag, everything else from the named tag
        assertFalse(newTag.containsAttrs("ctl_all_preferred_version"));
        assertEquals(latestPriorTag.getAttrsOrThrow("ctl_all_preferred_version__time"), newTag.getAttrsOrThrow("ctl_all_preferred_version__time"));
        assertEquals("keep", MetadataCodec.decodeStringValue(newTag.getAttrsOrThrow("ctl_all_housekeeping")));
        assertEquals("prior", MetadataCodec.decodeStringValue(newTag.getAttrsOrThrow("business_key")));
    }

    @Test
    void buildNewVersion_noLatestPriorTagUnchanged() {

        var priorTag = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), customDef("acme_widget"), List.of(attr("business_key", "prior")),
                null, requestMetadata(), userMetadata());

        var request = requestMetadata();
        var user = userMetadata();

        var withNull = ObjectUpdateLogic.buildNewVersion(priorTag, null, customDef("acme_widget"), List.of(), null, request, user);
        var original = ObjectUpdateLogic.buildNewVersion(priorTag, customDef("acme_widget"), List.of(), null, request, user);

        assertEquals(original, withNull);
    }

    @Test
    void buildNewControlledTag_set() {

        var priorTag = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), customDef("acme_widget"), List.of(),
                null, requestMetadata(), userMetadata());

        var request = requestMetadata();
        var user = new UserMetadata("approver", "An Approver");

        var newTag = ObjectUpdateLogic.buildNewControlledTag(
                priorTag, attr("approved", "yes"), VersionScope.VERSION_SCOPE_THIS_VERSION,
                MetadataCodec.encodeValue("Approved at MGC"), true, request, user);

        assertEquals(priorTag.getHeader().getTagVersion() + 1, newTag.getHeader().getTagVersion());
        assertEquals("yes", MetadataCodec.decodeStringValue(newTag.getAttrsOrThrow("ctl_this_approved")));
        assertEquals("Approved at MGC", MetadataCodec.decodeStringValue(newTag.getAttrsOrThrow("ctl_this_approved__reason")));
        assertEquals("approver", MetadataCodec.decodeStringValue(newTag.getAttrsOrThrow("ctl_this_approved__user_id")));
        assertEquals("An Approver", MetadataCodec.decodeStringValue(newTag.getAttrsOrThrow("ctl_this_approved__user_name")));
        assertEquals(MetadataCodec.encodeValue(request.requestTimestamp()), newTag.getAttrsOrThrow("ctl_this_approved__time"));

        // Tag-only writes still do not change who wrote the object version
        assertEquals(priorTag.getAttrsOrThrow(MetadataConstants.TRAC_UPDATE_USER_ID), newTag.getAttrsOrThrow(MetadataConstants.TRAC_UPDATE_USER_ID));
    }

    @Test
    void buildNewControlledTag_clear() {

        var priorTag = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), customDef("acme_widget"), List.of(),
                null, requestMetadata(), userMetadata());

        var setTag = controlledTag(priorTag, VersionScope.VERSION_SCOPE_THIS_VERSION, attr("approved", "yes"));

        var clearTag = ObjectUpdateLogic.buildNewControlledTag(
                setTag, clear("approved"), VersionScope.VERSION_SCOPE_THIS_VERSION,
                MetadataCodec.encodeValue("Withdrawn"), true, requestMetadata(), new UserMetadata("other", "Other User"));

        assertFalse(clearTag.containsAttrs("ctl_this_approved"));
        assertEquals("Withdrawn", MetadataCodec.decodeStringValue(clearTag.getAttrsOrThrow("ctl_this_approved__reason")));
        assertEquals("other", MetadataCodec.decodeStringValue(clearTag.getAttrsOrThrow("ctl_this_approved__user_id")));
    }

    @Test
    void buildNewControlledTag_clearWithNoValue() {

        var priorTag = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), customDef("acme_widget"), List.of(),
                null, requestMetadata(), userMetadata());

        // Refused when the value is required
        assertThrows(EMetadataBadUpdate.class, () -> ObjectUpdateLogic.buildNewControlledTag(
                priorTag, clear("approved"), VersionScope.VERSION_SCOPE_ALL_VERSIONS,
                MetadataCodec.encodeValue("Withdrawn"), true, requestMetadata(), userMetadata()));

        // Otherwise only the lineage is written
        var lineageOnly = ObjectUpdateLogic.buildNewControlledTag(
                priorTag, clear("approved"), VersionScope.VERSION_SCOPE_ALL_VERSIONS,
                MetadataCodec.encodeValue("Withdrawn"), false, requestMetadata(), userMetadata());

        assertFalse(lineageOnly.containsAttrs("ctl_all_approved"));
        assertTrue(lineageOnly.containsAttrs("ctl_all_approved__reason"));
        assertTrue(lineageOnly.containsAttrs("ctl_all_approved__time"));
    }

    @Test
    void buildNewControlledTag_typeMismatch() {

        var priorTag = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), customDef("acme_widget"), List.of(),
                null, requestMetadata(), userMetadata());

        var setTag = controlledTag(priorTag, VersionScope.VERSION_SCOPE_ALL_VERSIONS, intAttr("preferred_version", 1));

        assertThrows(EMetadataBadUpdate.class, () -> ObjectUpdateLogic.buildNewControlledTag(
                setTag, attr("preferred_version", "one"), VersionScope.VERSION_SCOPE_ALL_VERSIONS,
                MetadataCodec.encodeValue("x"), true, requestMetadata(), userMetadata()));
    }

    @Test
    void buildNewVersion_configObjectUnchanged() {

        var configDef = ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.CONFIG)
                .setConfig(ConfigDefinition.newBuilder().setConfigType(ConfigType.PROPERTIES))
                .build();

        var priorTag = ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), configDef, List.of(attr("business_key", "kept")),
                null, requestMetadata(), userMetadata());

        var newTag = ObjectUpdateLogic.buildNewVersion(
                priorTag, configDef, List.of(),
                null, requestMetadata(), userMetadata());

        assertEquals(priorTag.getAttrsMap().keySet(), newTag.getAttrsMap().keySet());
    }

    private static Tag controlledTag(Tag priorTag, VersionScope versionScope, TagUpdate update) {

        return ObjectUpdateLogic.buildNewControlledTag(
                priorTag, update, versionScope, MetadataCodec.encodeValue("A reason"),
                true, requestMetadata(), userMetadata());
    }

    private static TagUpdate clear(String attrName) {

        return TagUpdate.newBuilder()
                .setOperation(TagOperation.DELETE_ATTR)
                .setAttrName(attrName)
                .build();
    }

    private static ObjectDefinition customDef(String schemaType) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setCustom(CustomDefinition.newBuilder()
                        .setCustomSchemaType(schemaType)
                        .setCustomSchemaVersion(1)
                        .setCustomData(ByteString.copyFromUtf8("{}")))
                .build();
    }

    private static TagUpdate attr(String attrName, String value) {

        return TagUpdate.newBuilder()
                .setAttrName(attrName)
                .setValue(MetadataCodec.encodeValue(value))
                .build();
    }

    private static TagUpdate intAttr(String attrName, int value) {

        return TagUpdate.newBuilder()
                .setAttrName(attrName)
                .setValue(MetadataCodec.encodeValue(value))
                .build();
    }

    private static RequestMetadata requestMetadata() {
        return new RequestMetadata(UUID.randomUUID().toString(), OffsetDateTime.now(ZoneOffset.UTC));
    }

    private static UserMetadata userMetadata() {
        return new UserMetadata("test_user", "Test User");
    }

    private static Map<String, Value> attrsByName(java.util.List<TagUpdate> attrs) {

        return attrs.stream().collect(Collectors.toMap(TagUpdate::getAttrName, TagUpdate::getValue));
    }
}
