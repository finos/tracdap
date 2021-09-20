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

package com.accenture.trac.svc.data.core;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


public interface IFileStorage {

    interface FileWriter extends Flow.Subscriber<ByteBuf> {}
    interface FileReader extends Flow.Publisher<ByteBuf> {}

    CompletionStage<Boolean> exists(String storagePath);

    CompletionStage<Long> size(String storagePath);

    CompletionStage<FileStat> stat(String storagePath);

    CompletionStage<Void> ls(String storagePath);

    CompletionStage<Void> mkdir(String storagePath, boolean recursive, boolean existsOk);

    CompletionStage<Void> rm(String storagePath, boolean recursive);

    FileWriter reader(String storagePath);

    FileReader writer(String storagePath);
}
