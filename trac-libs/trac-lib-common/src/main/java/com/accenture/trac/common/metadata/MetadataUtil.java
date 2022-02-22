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

package com.accenture.trac.common.metadata;

import com.accenture.trac.metadata.TagHeader;
import com.accenture.trac.metadata.TagSelector;


public class MetadataUtil {

    private static final String OBJECT_KEY_TEMPLATE = "%s:%s:%d";

    public static TagSelector selectorFor(TagHeader header) {
        return selectorFor(header, false, false);
    }

    public static TagSelector selectorForLatest(TagHeader header) {
        return selectorFor(header, true, true);
    }

    public static TagSelector selectorFor(TagHeader header, boolean latestObject, boolean latestTag) {

        var selector = TagSelector.newBuilder()
                .setObjectType(header.getObjectType())
                .setObjectId(header.getObjectId());

        if (latestObject)
            selector.setLatestObject(true);
        else
            selector.setObjectVersion(header.getObjectVersion());

        if (latestTag)
            selector.setLatestTag(true);
        else
            selector.setTagVersion(header.getTagVersion());

        return selector.build();
    }

    public static String objectKey(TagHeader header) {

        return String.format(OBJECT_KEY_TEMPLATE,
                header.getObjectType(),
                header.getObjectId(),
                header.getObjectVersion());
    }
}
