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
import java.util.concurrent.Flow;


public interface IFileStorage {

    boolean exists(String storagePath);

    long size(String storagePath);

    void stat(String storagePath);

    void ls(String storagePath);

    void mkdir(String storagePath, boolean recursive, boolean existsOk);

    void rm(String storagePath, boolean recursive);

    Flow.Subscriber<ByteBuf> reader(String storagePath);

    Flow.Publisher<ByteBuf> writer(String storagePath);
}
