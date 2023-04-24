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

package org.finos.tracdap.common.storage.local;

import org.finos.tracdap.common.storage.StorageErrors;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Map;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;


public class LocalStorageErrors extends StorageErrors {

    private static final List<Map.Entry<Class<? extends Exception>, StorageErrors.ExplicitError>> EXCEPTION_CLASS_MAP = List.of(
            Map.entry(NoSuchFileException.class, OBJECT_NOT_FOUND),
            Map.entry(FileAlreadyExistsException.class, OBJECT_ALREADY_EXISTS),
            Map.entry(NotDirectoryException.class, NOT_A_DIRECTORY),
            Map.entry(AccessDeniedException.class, ACCESS_DENIED),
            Map.entry(SecurityException.class, ACCESS_DENIED),
            // IOException must be last in the list, not to obscure most specific exceptions
            Map.entry(IOException.class, IO_ERROR));

    public LocalStorageErrors(String storageKey) {

        super(storageKey);
    }

    @Override
    protected ExplicitError checkKnownExceptions(Throwable e) {

        // Look in the map of error types to see if e is an expected exception
        for (var error : EXCEPTION_CLASS_MAP) {

            var errorClass = error.getKey();

            if (errorClass.isInstance(e)) {
                return error.getValue();
            }
        }

        return null;
    }
}
