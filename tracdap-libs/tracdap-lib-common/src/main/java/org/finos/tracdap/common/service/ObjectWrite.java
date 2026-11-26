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
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.Tag;

/**
 * A single metadata object write, as seen by an {@link IMetadataWriteHook}.
 */
public class ObjectWrite {

    public enum Operation {
        CREATE,
        CREATE_PREALLOCATED,
        UPDATE
    }

    private final String tenant;
    private final ObjectType objectType;
    private final Operation operation;
    private final boolean inBatch;
    private final ObjectDefinition priorDefinition;
    private final Tag priorTag;
    private final ObjectDefinition definition;
    private final UserMetadata userMetadata;

    public ObjectWrite(
            String tenant, ObjectType objectType, Operation operation, boolean inBatch,
            ObjectDefinition priorDefinition, Tag priorTag, ObjectDefinition definition,
            UserMetadata userMetadata) {

        this.tenant = tenant;
        this.objectType = objectType;
        this.operation = operation;
        this.inBatch = inBatch;
        this.priorDefinition = priorDefinition;
        this.priorTag = priorTag;
        this.definition = definition;
        this.userMetadata = userMetadata;
    }

    public String tenant() {
        return tenant;
    }

    public ObjectType objectType() {
        return objectType;
    }

    public Operation operation() {
        return operation;
    }

    /** True if the write is one item in a writeBatch request */
    public boolean inBatch() {
        return inBatch;
    }

    /** The prior definition, for updates */
    public ObjectDefinition priorDefinition() {
        return priorDefinition;
    }

    /** The latest tag of the prior version, without its definition, for updates */
    public Tag priorTag() {
        return priorTag;
    }

    public ObjectDefinition definition() {
        return definition;
    }

    public UserMetadata userMetadata() {
        return userMetadata;
    }
}
