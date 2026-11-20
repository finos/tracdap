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
package org.finos.tracdap.svc.meta.services;

import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.store.IMetadataStore;
import org.finos.tracdap.common.metadata.tag.ObjectUpdateLogic;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.common.service.IConfigReader;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.test.meta.SampleMetadata;

import io.grpc.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;
import static org.junit.jupiter.api.Assertions.*;


// The service on its own, with a mock store and no metadata write hook registered
class MetadataWriteServiceTest {

    private static final UserMetadata USER = new UserMetadata("approver", "An Approver");

    private IMetadataStore store;
    private MetadataWriteService writeService;

    @BeforeEach
    void setup() {

        store = Mockito.mock(IMetadataStore.class);
        writeService = new MetadataWriteService(store, new PluginRegistry(), Mockito.mock(IConfigReader.class));
    }

    @Test
    void createObject_withoutHook() throws Exception {

        var request = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setDefinition(SampleMetadata.dummyCustomDef())
                .build();

        var header = inContext(() -> writeService.createObject(TEST_TENANT, request));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Tag>> saved = ArgumentCaptor.forClass(List.class);
        Mockito.verify(store).saveNewObjects(ArgumentMatchers.eq(TEST_TENANT), saved.capture());

        assertEquals(1, saved.getValue().size());
        assertEquals(header, saved.getValue().get(0).getHeader());
        assertEquals(request.getDefinition(), saved.getValue().get(0).getDefinition());
    }

    @Test
    void createPreallocatedObject_withoutHook() throws Exception {

        var preallocatedId = SampleMetadata.newHeader(ObjectType.CUSTOM);

        var request = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(MetadataUtil.preallocated(MetadataUtil.selectorFor(preallocatedId)))
                .setDefinition(SampleMetadata.dummyCustomDef())
                .build();

        var header = inContext(() -> writeService.createPreallocatedObject(TEST_TENANT, request));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Tag>> saved = ArgumentCaptor.forClass(List.class);
        Mockito.verify(store).savePreallocatedObjects(ArgumentMatchers.eq(TEST_TENANT), saved.capture());

        assertEquals(1, saved.getValue().size());
        assertEquals(preallocatedId.getObjectId(), header.getObjectId());
        assertEquals(request.getDefinition(), saved.getValue().get(0).getDefinition());
    }

    @Test
    void updateObject_latestTagNotReread() throws Exception {

        var prior = withAllVersionsAttr(createdTag(), 1);

        Mockito.when(store.loadPriorObjects(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(List.of(prior));

        inContext(() -> writeService.updateObject(TEST_TENANT, newVersionRequest(prior)));

        Mockito.verify(store, Mockito.never()).loadPriorTags(ArgumentMatchers.any(), ArgumentMatchers.any());

        var saved = savedVersion();
        assertEquals(prior.getAttrsOrThrow("ctl_all_preferred_version"), saved.getAttrsOrThrow("ctl_all_preferred_version"));
    }

    @Test
    void updateObject_olderTagRefreshedFromLatest() throws Exception {

        var olderTag = createdTag();
        var latestTag = withAllVersionsAttr(olderTag, 1);

        var named = olderTag.toBuilder()
                .setHeader(olderTag.getHeader().toBuilder().setIsLatestTag(false))
                .build();

        Mockito.when(store.loadPriorObjects(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(List.of(named));
        Mockito.when(store.loadPriorTags(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(List.of(latestTag));

        inContext(() -> writeService.updateObject(TEST_TENANT, newVersionRequest(named)));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<TagSelector>> selectors = ArgumentCaptor.forClass(List.class);
        Mockito.verify(store).loadPriorTags(ArgumentMatchers.eq(TEST_TENANT), selectors.capture());
        assertEquals(List.of(MetadataUtil.selectorFor(named.getHeader(), false, true)), selectors.getValue());

        var saved = savedVersion();
        assertEquals(latestTag.getAttrsOrThrow("ctl_all_preferred_version"), saved.getAttrsOrThrow("ctl_all_preferred_version"));
    }

    @Test
    void updateTag_controlledWithoutHook() throws Exception {

        var prior = createdTag();

        Mockito.when(store.loadPriorTags(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(List.of(prior));

        inContext(() -> writeService.updateTag(TEST_TENANT, controlledRequest(prior, VersionScope.VERSION_SCOPE_THIS_VERSION)));

        var saved = savedTags();
        assertEquals(1, saved.size());

        var attrs = saved.get(0).getAttrsMap();
        assertEquals("yes", MetadataCodec.decodeStringValue(attrs.get("ctl_this_approved")));
        assertEquals("Approved", MetadataCodec.decodeStringValue(attrs.get("ctl_this_approved__reason")));
        assertEquals("approver", MetadataCodec.decodeStringValue(attrs.get("ctl_this_approved__user_id")));
        assertEquals("An Approver", MetadataCodec.decodeStringValue(attrs.get("ctl_this_approved__user_name")));
        assertTrue(attrs.containsKey("ctl_this_approved__time"));
    }

    @Test
    void updateTag_allVersionsWithoutHook() throws Exception {

        var v1 = createdTag();
        var v2 = ObjectUpdateLogic.buildNewVersion(v1, SampleMetadata.nextCustomDef(v1.getDefinition()), List.of(), null, request(), USER);

        Mockito.when(store.loadPriorTags(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(List.of(v2));
        Mockito.when(store.loadObjects(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(List.of(v1));

        inContext(() -> writeService.updateTag(TEST_TENANT, controlledRequest(v2, VersionScope.VERSION_SCOPE_ALL_VERSIONS)));

        var saved = savedTags();
        assertEquals(2, saved.size());
        assertEquals(1, saved.get(0).getHeader().getObjectVersion());
        assertEquals(2, saved.get(1).getHeader().getObjectVersion());

        for (var tag : saved) {
            assertEquals("yes", MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("ctl_all_approved")));
            assertEquals("approver", MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("ctl_all_approved__user_id")));
        }
    }

    private Tag createdTag() {

        return ObjectUpdateLogic.buildNewObject(
                UUID.randomUUID(), SampleMetadata.dummyCustomDef(), List.of(),
                null, request(), USER);
    }

    private Tag withAllVersionsAttr(Tag tag, int preferredVersion) {

        var update = TagUpdate.newBuilder()
                .setAttrName("preferred_version")
                .setValue(MetadataCodec.encodeValue(preferredVersion))
                .build();

        return ObjectUpdateLogic.buildNewControlledTag(
                tag, update, VersionScope.VERSION_SCOPE_ALL_VERSIONS, MetadataCodec.encodeValue("Preferred"),
                true, request(), USER);
    }

    private static MetadataWriteRequest newVersionRequest(Tag prior) {

        return MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(MetadataUtil.selectorFor(prior.getHeader()))
                .setDefinition(SampleMetadata.nextCustomDef(prior.getDefinition()))
                .build();
    }

    private static MetadataWriteRequest controlledRequest(Tag prior, VersionScope versionScope) {

        return MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(MetadataUtil.selectorFor(prior.getHeader()))
                .setControl(TagControl.CONTROLLED)
                .setVersionScope(versionScope)
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("approved")
                        .setValue(MetadataCodec.encodeValue("yes")))
                .setReason(MetadataCodec.encodeValue("Approved"))
                .build();
    }

    private Tag savedVersion() {

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Tag>> saved = ArgumentCaptor.forClass(List.class);
        Mockito.verify(store).saveNewVersions(ArgumentMatchers.eq(TEST_TENANT), saved.capture());

        return saved.getValue().get(0);
    }

    private List<Tag> savedTags() {

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Tag>> saved = ArgumentCaptor.forClass(List.class);
        Mockito.verify(store).saveNewTags(ArgumentMatchers.eq(TEST_TENANT), saved.capture());

        return saved.getValue();
    }

    private static RequestMetadata request() {
        return new RequestMetadata(UUID.randomUUID().toString(), OffsetDateTime.now(ZoneOffset.UTC));
    }

    private static <T> T inContext(Callable<T> call) throws Exception {

        var context = UserMetadata.set(RequestMetadata.set(Context.current(), request()), USER);
        return context.call(call);
    }
}
