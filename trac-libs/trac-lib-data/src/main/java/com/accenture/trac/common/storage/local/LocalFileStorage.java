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

package com.accenture.trac.common.storage.local;

import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.exception.EStorageRequest;
import com.accenture.trac.common.storage.FileStat;
import com.accenture.trac.common.storage.IFileStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class LocalFileStorage implements IFileStorage {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storageKey;
    private final Path rootPath;

    public LocalFileStorage(String storageKey, String storageRootPath) {

        this.storageKey = storageKey;
        this.rootPath = Paths.get(storageRootPath)
                .toAbsolutePath()
                .normalize();

        if (!Files.exists(this.rootPath)) {
            var err = String.format("Storage root path does not exist: %s [%s]", storageKey, rootPath);
            log.error(err);
            throw new EStartup(err);
        }

        if (!Files.isDirectory(this.rootPath)) {
            var err = String.format("Storage root path is not a directory: %s [%s]", storageKey, rootPath);
            log.error(err);
            throw new EStartup(err);
        }

        if (!Files.isReadable(this.rootPath) || !Files.isWritable(this.rootPath)) {

            var err = String.format(
                    "Storage root path has insufficient access (read/write required): %s [%s]",
                    storageKey, rootPath);

            log.error(err);
            throw new EStartup(err);
        }
    }

    @Override
    public CompletionStage<Boolean> exists(String storagePath) {

        var absolutePath = rootPath.resolve(storagePath);
        var exists = Files.exists(absolutePath);

        return CompletableFuture.completedFuture(exists);
    }

    @Override
    public CompletableFuture<Long> size(String storagePath) {

        try {
            var absolutePath = rootPath.resolve(storagePath);
            var size = Files.size(absolutePath);

            return CompletableFuture.completedFuture(size);
        }
        catch (FileNotFoundException e) {

            var err = String.format("File not found in storage layer: %s [%s]", storageKey, storagePath);
            log.error(err);
            throw new EStorageRequest(err, e);
        }
        catch (AccessDeniedException e) {

            var err = String.format("Access denied in storage layer: %s [%s]", storageKey, storagePath);
            log.error(err);
            throw new EStorageRequest(err, e);
        }
        catch (IOException e) {

            var err = String.format("An error occurred in the storage layer: %s [%s]", storageKey, storagePath);
            log.error(err, e);
            throw new EStorageRequest(err, e);
        }
    }

    @Override
    public CompletionStage<FileStat> stat(String storagePath) {

        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public CompletionStage<Void> ls(String storagePath) {

        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public CompletionStage<Void> mkdir(String storagePath, boolean recursive, boolean existsOk) {

        var absolutePath = rootPath.resolve(storagePath);

        if (!existsOk && Files.exists(absolutePath)) {
            // TODO: error
        }

        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public CompletionStage<Void> rm(String storagePath, boolean recursive) {

        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public FileWriter reader(String storagePath) {
        return null;
    }

    @Override
    public FileReader writer(String storagePath) {
        return null;
    }
}
