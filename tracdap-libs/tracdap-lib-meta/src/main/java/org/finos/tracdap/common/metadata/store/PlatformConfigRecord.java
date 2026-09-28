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

package org.finos.tracdap.common.metadata.store;

import org.finos.tracdap.metadata.PlatformConfigEntry;

import java.time.Instant;

public class PlatformConfigRecord {

    private final PlatformConfigEntry entry;
    private final byte[] value;

    // Null for rows written before provenance was recorded
    private final Instant createTime;
    private final String createUserId;
    private final String createUserName;
    private final String updateUserId;
    private final String updateUserName;

    public PlatformConfigRecord(
            PlatformConfigEntry entry, byte[] value,
            Instant createTime, String createUserId, String createUserName,
            String updateUserId, String updateUserName) {

        this.entry = entry;
        this.value = value;
        this.createTime = createTime;
        this.createUserId = createUserId;
        this.createUserName = createUserName;
        this.updateUserId = updateUserId;
        this.updateUserName = updateUserName;
    }

    public PlatformConfigEntry entry() {
        return entry;
    }

    public byte[] value() {
        return value;
    }

    public Instant createTime() {
        return createTime;
    }

    public String createUserId() {
        return createUserId;
    }

    public String createUserName() {
        return createUserName;
    }

    public String updateUserId() {
        return updateUserId;
    }

    public String updateUserName() {
        return updateUserName;
    }
}
