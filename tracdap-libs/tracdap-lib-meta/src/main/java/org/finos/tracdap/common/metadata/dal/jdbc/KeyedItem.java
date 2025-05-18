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

class KeyedItem<TItem> {

    final long key;
    final int version;
    final Instant timestamp;
    final TItem item;
    final boolean isLatest;
    final boolean deleted;

    KeyedItem(long key, int version, Instant timestamp, TItem item, boolean isLatest, boolean deleted) {
        this.key = key;
        this.version = version;
        this.timestamp = timestamp;
        this.item = item;
        this.isLatest = isLatest;
        this.deleted = deleted;
    }

    KeyedItem(long key, int version, Instant timestamp, TItem item, boolean isLatest) {
        this(key, version, timestamp, item, isLatest, false);
    }

    KeyedItem(long key, TItem item) {
        this(key, 0, null, item, false);
    }
}
