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
import com.accenture.trac.metadata.PartKey;
import com.accenture.trac.metadata.PartType;

public class PartKeys {

    private static final String ROOT_OPAQUE_KEY = "part-root";

    public static final PartKey ROOT = PartKey.newBuilder()
            .setPartType(PartType.PART_ROOT)
            .setOpaqueKey(ROOT_OPAQUE_KEY)
            .build();

    public static String opaqueKey(PartKey partKey) {

        if (partKey.getPartType() == PartType.PART_ROOT)
            return ROOT_OPAQUE_KEY;

        throw new EUnexpected();
    }
}
