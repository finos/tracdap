/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.webserver;

import org.finos.tracdap.common.concurrent.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.storage.FileStat;
import org.finos.tracdap.common.storage.IFileStorage;

import java.util.concurrent.CompletionStage;


public class ContentServer {

    private final IFileStorage storage;

    public ContentServer(IFileStorage storage) {
        this.storage = storage;
    }

    public CompletionStage<ContentResponse> headRequest(String storagePath, IExecutionContext execCtx) {

        return storage.stat(storagePath, execCtx)
                .thenApply(this::buildResponse);
    }

    public void getRequest(String storagePath, IDataContext dataCtx) {

        storage.reader(storagePath, dataCtx);
    }

    private ContentResponse buildResponse(FileStat fileStat) {

        return null;
    }
}
