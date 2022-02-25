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

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.metadata.TagHeader;
import com.accenture.trac.metadata.TagSelector;


public class MetadataUtil {

    private static final String OBJECT_KEY_FOR_VERSION = "%s-%s-v%d";
    private static final String OBJECT_KEY_FOR_ASOF = "%s-%s-asof-%s";
    private static final String OBJECT_KEY_FOR_LATEST = "%s-%s-latest";

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

        return String.format(OBJECT_KEY_FOR_VERSION,
                header.getObjectType(),
                header.getObjectId(),
                header.getObjectVersion());
    }

    public static String objectKey(TagSelector selector) {

        if (selector.hasObjectVersion())

            return String.format(OBJECT_KEY_FOR_VERSION,
                    selector.getObjectType(),
                    selector.getObjectId(),
                    selector.getObjectVersion());

        if (selector.hasObjectAsOf())

            return String.format(OBJECT_KEY_FOR_ASOF,
                    selector.getObjectType(),
                    selector.getObjectId(),
                    selector.getObjectAsOf().getIsoDatetime());

        if (selector.hasLatestObject())

            return String.format(OBJECT_KEY_FOR_LATEST,
                    selector.getObjectType(),
                    selector.getObjectId());

        throw new EUnexpected();
    }
}
