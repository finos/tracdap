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

package com.accenture.trac.common.storage;


import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.metadata.DataDefinition;
import com.accenture.trac.metadata.SchemaDefinition;
import com.accenture.trac.metadata.StorageCopy;
import com.accenture.trac.metadata.StorageDefinition;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;


public interface IDataStorage {

    Flow.Publisher<ArrowRecordBatch> reader(
            SchemaDefinition schemaDef,
            StorageCopy storageCopy,
            IExecutionContext execContext);

    Flow.Subscriber<ArrowRecordBatch> writer(
            SchemaDefinition schemaDef,
            StorageCopy storageCopy,
            CompletableFuture<Long> signal,
            IExecutionContext execContext);
}
