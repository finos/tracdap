/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.meta.dal;

import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.metadata.TagHeader;

import java.util.List;


public class MetadataBatchUpdate {

    private final List<TagHeader> preallocatedIds;
    private final List<Tag> preallocatedObjects;
    private final List<Tag> newObjects;
    private final List<Tag> newVersions;
    private final List<Tag> newTags;


    public MetadataBatchUpdate(
            List<TagHeader> preallocatedIds,
            List<Tag> preallocatedObjects,
            List<Tag> newObjects,
            List<Tag> newVersions,
            List<Tag> newTags) {

        this.preallocatedIds = preallocatedIds;
        this.preallocatedObjects = preallocatedObjects;
        this.newObjects = newObjects;
        this.newVersions = newVersions;
        this.newTags = newTags;
    }

    public List<TagHeader> getPreallocatedIds() {
        return preallocatedIds;
    }

    public List<Tag> getPreallocatedObjects() {
        return preallocatedObjects;
    }

    public List<Tag> getNewObjects() {
        return newObjects;
    }

    public List<Tag> getNewVersions() {
        return newVersions;
    }

    public List<Tag> getNewTags() {
        return newTags;
    }

    @Override
    public String toString() {

        // For logging / debugging

        var nPrealloatedIds = preallocatedIds == null ? "(null)" : preallocatedIds.size();
        var nPreallocatedObjects = preallocatedIds == null ? "(null)" : preallocatedObjects.size();
        var nNewObjects = preallocatedIds == null ? "(null)" : newObjects.size();
        var nNewVersions = preallocatedIds == null ? "(null)" : newVersions.size();
        var nNewTags = preallocatedIds == null ? "(null)" : newTags.size();

        return String.format(
                "{preallocatedIds = %s, preallocatedObjects = %s, newObjects = %s, newVersions = %s, newTags = %s}",
                nPrealloatedIds, nPreallocatedObjects,
                nNewObjects, nNewVersions, nNewTags);
    }
}
