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
package org.finos.tracdap.common.service;

import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.metadata.TagControl;
import org.finos.tracdap.metadata.TagUpdate;
import org.finos.tracdap.metadata.VersionScope;

import java.util.List;

/**
 * A single tag write, as seen by an {@link IMetadataWriteHook}.
 *
 * <p>A write for all versions of an object is one tag write, its prior tag is the tag the request names, which is
 * the latest tag of the latest version.</p>
 */
public class TagWrite {

    private final String tenant;
    private final ObjectType objectType;
    private final Tag priorTag;
    private final TagControl control;
    private final VersionScope versionScope;
    private final List<TagUpdate> tagUpdates;
    private final int latestObjectVersion;
    private final boolean inBatch;
    private final UserMetadata userMetadata;
    private final IConfigReader reader;

    public TagWrite(
            String tenant, ObjectType objectType, Tag priorTag,
            TagControl control, VersionScope versionScope, List<TagUpdate> tagUpdates,
            int latestObjectVersion, boolean inBatch,
            UserMetadata userMetadata, IConfigReader reader) {

        this.tenant = tenant;
        this.objectType = objectType;
        this.priorTag = priorTag;
        this.control = control;
        this.versionScope = versionScope;
        this.tagUpdates = tagUpdates;
        this.latestObjectVersion = latestObjectVersion;
        this.inBatch = inBatch;
        this.userMetadata = userMetadata;
        this.reader = reader;
    }

    public String tenant() {
        return tenant;
    }

    public ObjectType objectType() {
        return objectType;
    }

    /** The tag the write is applied to, as named in the request */
    public Tag priorTag() {
        return priorTag;
    }

    public TagControl control() {
        return control;
    }

    public VersionScope versionScope() {
        return versionScope;
    }

    /** The tag updates as sent in the request, with bare attribute names for a controlled write */
    public List<TagUpdate> tagUpdates() {
        return tagUpdates;
    }

    /**
     * The latest object version number, for a write to all versions.
     * For other writes it is 0, the prior tag header says whether its version is the latest.
     */
    public int latestObjectVersion() {
        return latestObjectVersion;
    }

    /** True if the write is one item in a writeBatch request */
    public boolean inBatch() {
        return inBatch;
    }

    public UserMetadata userMetadata() {
        return userMetadata;
    }

    public IConfigReader reader() {
        return reader;
    }
}
