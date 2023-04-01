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

package org.finos.tracdap.common.storage;

import io.netty.buffer.ByteBuf;
import org.finos.tracdap.common.concurrent.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;

public abstract class CommonFileStorage implements IFileStorage {

    protected final StoragePath rootPath;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final StorageErrors errors;

    protected CommonFileStorage(StoragePath rootPath) {
        this.rootPath = rootPath;
    }

    protected abstract CompletionStage<Boolean> _exists(StoragePath storagePath);



    @Override
    public CompletionStage<Boolean> exists(String storagePath, IExecutionContext execContext) {


    }

    @Override
    public CompletionStage<Long> size(String storagePath, IExecutionContext execContext) {
        return null;
    }

    @Override
    public CompletionStage<FileStat> stat(String storagePath, IExecutionContext execContext) {
        return null;
    }

    @Override
    public CompletionStage<List<FileStat>> ls(String storagePath, IExecutionContext execContext) {
        return null;
    }

    @Override
    public CompletionStage<Void> mkdir(String storagePath, boolean recursive, IExecutionContext execContext) {
        return null;
    }

    @Override
    public CompletionStage<Void> rm(String storagePath, boolean recursive, IExecutionContext execContext) {
        return null;
    }

    @Override
    public Flow.Publisher<ByteBuf> reader(String storagePath, IDataContext dataContext) {
        return null;
    }

    @Override
    public Flow.Subscriber<ByteBuf> writer(String storagePath, CompletableFuture<Long> signal, IDataContext dataContext) {
        return null;
    }

    private String resolvePath(String storagePath, boolean allowRootDir, String operationName) {


        try {

            var isWindows = System.getProperty("os.name").toLowerCase().contains("win");

            if (storagePath == null || storagePath.isBlank())
                throw errors.explicitError(STORAGE_PATH_NULL_OR_BLANK, storagePath, operationName);

//            if self._ILLEGAL_PATH_CHARS.match(storage_path):
//            raise self._explicit_error(self.ExplicitError.STORAGE_PATH_INVALID, operation_name, storage_path)

            var relativePath = Paths.get(storagePath);

            if (relativePath.isAbsolute())
                throw errors.explicitError(STORAGE_PATH_NOT_RELATIVE, storagePath, operationName);

            var rootPath = isWindows ? Paths.get("C:\\root") : Paths.get("/root");
            var absolutePath = rootPath.resolve(relativePath).normalize();

            if (absolutePath.equals(rootPath)) {

                if (!allowRootDir)
                    throw errors.explicitError(STORAGE_PATH_IS_ROOT, storagePath, operationName);

                return "";
            }
            else {

                if (!absolutePath.startsWith(rootPath))
                    throw errors.explicitError(STORAGE_PATH_OUTSIDE_ROOT, storagePath, operationName);

                var resolvedPath = rootPath.relativize(absolutePath);

                var sections = new ArrayList<String>(resolvedPath.getNameCount());
                resolvedPath.forEach(section -> sections.add(section.toString()));

                return String.join(BACKSLASH, sections);
            }
        }
        catch (InvalidPathException e) {

            throw errors.explicitError(STORAGE_PATH_INVALID, storagePath, operationName);  // todo: cause
        }
    }
}
