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

package org.finos.tracdap.common.metadata.dal.jdbc;

import java.time.Instant;

class KeyedItems<TItem> {

    final long[] keys;
    final int[] versions;
    final Instant[] timestamps;
    final TItem[] items;
    final boolean[] isLatest;
    final boolean[] deleted;

    // For listConfigEntries
    final String[] configKeys;

    KeyedItems(long[] keys, int[] versions, Instant[] timestamps, TItem[] items, boolean[] isLatest, boolean[] deleted, String[] configKeys) {
        this.keys = keys;
        this.versions = versions;
        this.timestamps = timestamps;
        this.items = items;
        this.isLatest = isLatest;
        this.deleted = deleted;
        this.configKeys = configKeys;
    }

    KeyedItems(long[] keys, int[] versions, Instant[] timestamps, TItem[] items, boolean[] isLatest, boolean[] deleted) {
        this(keys, versions, timestamps, items, isLatest, deleted, null);
    }

    KeyedItems(long[] keys, int[] versions, Instant[] timestamps, TItem[] items, boolean[] isLatest) {
        this(keys, versions, timestamps, items, isLatest, null);
    }

    KeyedItems(long[] keys, int[] versions, TItem[] items) {
        this(keys, versions, null, items, null);
    }

    KeyedItems(long[] keys, TItem[] items) {
        this(keys, null, items);
    }
}
