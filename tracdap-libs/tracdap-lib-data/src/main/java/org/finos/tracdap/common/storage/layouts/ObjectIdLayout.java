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

package org.finos.tracdap.common.storage.layouts;

import org.finos.tracdap.common.storage.IStorageLayout;
import org.finos.tracdap.common.storage.LayoutItem;
import org.finos.tracdap.metadata.SchemaType;

import java.util.Random;


public class ObjectIdLayout implements IStorageLayout {

    private static final String STORAGE_PATH_TEMPLATE = "data/%s/%s/%s/snap-%d/delta-%d-x%06x";
    private static final String STRICT_STORAGE_PATH_TEMPLATE = "data/%s/%s/%s/snap-%d/delta-%d-x%06x.%s";
    private static final String FILE_STORAGE_PATH_TEMPLATE = "file/%s/version-%d%s/%s";
    private static final String FILE_STORAGE_PATH_SUFFIX_TEMPLATE = "-x%06x";

    private final Random random;

    public ObjectIdLayout() {
        this.random = new Random();
    }

    @Override
    public String newFilePath(LayoutItem layoutItem) {

        var fileUuid = layoutItem.header().getObjectId();
        var fileVersion = layoutItem.header().getObjectVersion();
        var fileName = layoutItem.file().getName();

        var storageSuffixBytes = random.nextInt(1 << 24);
        var storageSuffix = String.format(FILE_STORAGE_PATH_SUFFIX_TEMPLATE, storageSuffixBytes);

        return String.format(FILE_STORAGE_PATH_TEMPLATE, fileUuid, fileVersion, storageSuffix, fileName);
    }

    @Override
    public String updateFilePath(LayoutItem layoutItem, LayoutItem priorLayoutItem) {

        // Object ID layout is fully deterministic, not affected by the layout of prior items
        return newFilePath(layoutItem);
    }

    public String newDataPath(LayoutItem layoutItem) {

        var dataType = layoutItem.schema().getSchemaType().name().toLowerCase();
        var objectId = layoutItem.header().getObjectId();
        var partKey = layoutItem.part().getOpaqueKey();
        var suffixBytes = random.nextInt(1 << 24);

        if (layoutItem.schema().getSchemaType() == SchemaType.STRUCT_SCHEMA) {

            return String.format(STRICT_STORAGE_PATH_TEMPLATE,
                    dataType, objectId,
                    partKey, layoutItem.snap(), layoutItem.delta(),
                    suffixBytes, layoutItem.extension());
        }
        else {

            return String.format(STORAGE_PATH_TEMPLATE,
                    dataType, objectId,
                    partKey, layoutItem.snap(), layoutItem.delta(),
                    suffixBytes);
        }
    }

    @Override
    public String updateDataPath(LayoutItem layoutItem, LayoutItem priorLayoutItem) {

        // Object ID layout is fully deterministic, not affected by the layout of prior items
        return newDataPath(layoutItem);
    }
}
