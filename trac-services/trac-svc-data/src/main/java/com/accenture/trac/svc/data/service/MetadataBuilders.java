/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.data.service;

import com.accenture.trac.api.MetadataBatchRequest;
import com.accenture.trac.api.MetadataReadRequest;
import com.accenture.trac.api.MetadataWriteRequest;
import com.accenture.trac.metadata.ObjectType;
import com.accenture.trac.metadata.TagHeader;
import com.accenture.trac.metadata.TagSelector;

public class MetadataBuilders {

    static MetadataWriteRequest preallocateRequest(String tenant, ObjectType objectType) {

        return MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(objectType)
                .build();
    }

    static MetadataReadRequest requestForSelector(String tenant, TagSelector selector) {

        return MetadataReadRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(selector)
                .build();
    }

    static MetadataBatchRequest requestForBatch(String tenant, TagSelector... selectors) {

        var batch = MetadataBatchRequest.newBuilder()
                .setTenant(tenant);

        for (var selector : selectors)
            batch.addSelector(selector);

        return batch.build();
    }

    static TagHeader bumpVersion(TagHeader priorVersion) {

        return priorVersion.toBuilder()
                .setObjectVersion(priorVersion.getObjectVersion() + 1)
                .setTagVersion(1)
                .build();
    }
}
