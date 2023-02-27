/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.data.service;

import io.grpc.CallCredentials;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.metadata.*;

import java.time.Instant;
import java.util.List;


class RequestState {

    UserInfo requestOwner;
    Instant requestTimestamp;
    CallCredentials credentials;

    List<TagUpdate> dataTags;
    List<TagUpdate> fileTags;
    List<TagUpdate> storageTags;

    TagHeader dataId, preAllocDataId;
    TagHeader fileId, preAllocFileId;
    TagHeader storageId, preAllocStorageId;

    DataDefinition data;
    SchemaDefinition schema;
    FileDefinition file;
    StorageDefinition storage;

    PartKey part;
    int snap;
    int delta;

    long offset;
    long limit;

    StorageCopy copy;
}