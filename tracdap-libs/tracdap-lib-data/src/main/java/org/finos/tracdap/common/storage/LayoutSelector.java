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

package org.finos.tracdap.common.storage;

import org.finos.tracdap.common.config.ConfigDefaults;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.storage.layouts.DateSnapLayout;
import org.finos.tracdap.common.storage.layouts.ObjectIdLayout;
import org.finos.tracdap.metadata.StorageLayout;

import java.util.Map;


public class LayoutSelector {

    // Layout selection logic is static
    // Defaults for new objects is passed into newObjectLayout()

    private static final Map<StorageLayout, IStorageLayout> STORAGE_LAYOUTS = Map.ofEntries(
            Map.entry(StorageLayout.OBJECT_ID_LAYOUT, new ObjectIdLayout()),
            Map.entry(StorageLayout.DATE_SNAP_LAYOUT, new DateSnapLayout()));

    // For new objects, use ConfigDefaults if a default layout is set in the storage configuration
    private static final StorageLayout DEFAULT_STORAGE_LAYOUT = ConfigDefaults.STORAGE_DEFAULT_LAYOUT;

    // For backwards compatibility with 0.9, assume this layout for updates to objects with no layout set
    private static final StorageLayout BACKWARDS_COMPATIBLE_LAYOUT = StorageLayout.OBJECT_ID_LAYOUT;

    public static IStorageLayout newObjectLayout(StorageLayout layoutId) {

        if (layoutId == StorageLayout.STORAGE_LAYOUT_NOT_SET)
            return getStorageLayout(DEFAULT_STORAGE_LAYOUT);
        else
            return getStorageLayout(layoutId);
    }

    public static IStorageLayout updateObjectLayout(StorageLayout layoutId) {

        if (layoutId == StorageLayout.STORAGE_LAYOUT_NOT_SET)
            return getStorageLayout(BACKWARDS_COMPATIBLE_LAYOUT);
        else
            return getStorageLayout(layoutId);
    }

    private static IStorageLayout getStorageLayout(StorageLayout layoutId) {

        var layout = STORAGE_LAYOUTS.get(layoutId);

        if (layout == null)
            throw new ETracInternal(String.format("Unsupported storage layout: [%s]", layoutId.name()));

        return layout;
    }
}
