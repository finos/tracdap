/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.meta.dal.operations;

import org.finos.tracdap.metadata.ObjectType;

import java.util.List;
import java.util.UUID;

public final class PreallocateObjectId extends DalWriteOperation {
    final private List<ObjectType> objectTypes;
    final private List<UUID> objectIds;

    public PreallocateObjectId(List<ObjectType> objectTypes, List<UUID> objectIds) {
        this.objectTypes = objectTypes;
        this.objectIds = objectIds;
    }

    public List<ObjectType> getObjectTypes() {
        return objectTypes;
    }

    public List<UUID> getObjectIds() {
        return objectIds;
    }
}

