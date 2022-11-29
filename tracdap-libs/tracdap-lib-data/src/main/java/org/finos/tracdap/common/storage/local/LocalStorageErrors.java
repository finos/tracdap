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
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Map;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;


public class LocalStorageErrors extends StorageErrors {

    private static final List<Map.Entry<Class<? extends Exception>, StorageErrors.ExplicitError>> EXCEPTION_CLASS_MAP = List.of(
            Map.entry(NoSuchFileException.class, NO_SUCH_FILE_EXCEPTION),
            Map.entry(FileAlreadyExistsException.class, FILE_ALREADY_EXISTS_EXCEPTION),
            Map.entry(DirectoryNotEmptyException.class, DIRECTORY_NOT_FOUND_EXCEPTION),
            Map.entry(NotDirectoryException.class, NOT_DIRECTORY_EXCEPTION),
            Map.entry(AccessDeniedException.class, ACCESS_DENIED_EXCEPTION),
            Map.entry(SecurityException.class, SECURITY_EXCEPTION),
            // IOException must be last in the list, not to obscure most specific exceptions
            Map.entry(IOException.class, IO_EXCEPTION));

    public LocalStorageErrors(String storageKey, Logger log) {

        super(EXCEPTION_CLASS_MAP, storageKey, log);
    }
}
