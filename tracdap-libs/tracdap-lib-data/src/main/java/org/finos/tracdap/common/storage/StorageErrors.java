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

package org.finos.tracdap.common.storage;

import org.finos.tracdap.common.exception.*;

import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;


public abstract class StorageErrors {

    public enum ExplicitError {

        // Validation failures
        STORAGE_PATH_NULL_OR_BLANK,
        STORAGE_PATH_NOT_RELATIVE,
        STORAGE_PATH_OUTSIDE_ROOT,
        STORAGE_PATH_IS_ROOT,
        STORAGE_PATH_INVALID,

        // Explicit errors file in operations
        SIZE_OF_DIR,
        STAT_NOT_FILE_OR_DIR,
        RM_DIR_NOT_RECURSIVE,

        // Exceptions
        NO_SUCH_FILE_EXCEPTION,
        FILE_ALREADY_EXISTS_EXCEPTION,
        DIRECTORY_NOT_FOUND_EXCEPTION,
        NOT_DIRECTORY_EXCEPTION,
        ACCESS_DENIED_EXCEPTION,
        SECURITY_EXCEPTION,
        IO_EXCEPTION,

        // Errors in stream (Flow pub/sub) implementation
        DUPLICATE_SUBSCRIPTION,

        // Unhandled / unexpected error
        UNKNOWN_ERROR,

        // These errors have special parameterization for their error messages
        CHUNK_NOT_FULLY_WRITTEN,
    }

    private static final Map<ExplicitError, String> ERROR_MESSAGE_MAP = Map.ofEntries(
            Map.entry(STORAGE_PATH_NULL_OR_BLANK, "Requested storage path is null or blank: %s %s [%s]"),
            Map.entry(STORAGE_PATH_NOT_RELATIVE, "Requested storage path is not a relative path: %s %s [%s]"),
            Map.entry(STORAGE_PATH_OUTSIDE_ROOT, "Requested storage path is outside the storage root directory: %s %s [%s]"),
            Map.entry(STORAGE_PATH_IS_ROOT, "Requested operation not allowed on the storage root directory: %s %s [%s]"),
            Map.entry(STORAGE_PATH_INVALID, "Requested storage path is invalid: %s %s [%s]"),

            Map.entry(SIZE_OF_DIR, "Size operation is not available for directories: %s %s [%s]"),
            Map.entry(STAT_NOT_FILE_OR_DIR, "Object is not a file or directory: %s %s [%s]"),
            Map.entry(RM_DIR_NOT_RECURSIVE, "Regular delete operation not available for directories (use recursive delete): %s %s [%s]"),

            Map.entry(NO_SUCH_FILE_EXCEPTION, "File not found in storage layer: %s %s [%s]"),
            Map.entry(FILE_ALREADY_EXISTS_EXCEPTION, "File already exists in storage layer: %s %s [%s]"),
            Map.entry(DIRECTORY_NOT_FOUND_EXCEPTION, "Directory not found in storage layer: %s %s [%s]"),
            Map.entry(NOT_DIRECTORY_EXCEPTION, "Path is not a directory in storage layer: %s %s [%s]"),
            Map.entry(ACCESS_DENIED_EXCEPTION, "Access denied in storage layer: %s %s [%s]"),
            Map.entry(SECURITY_EXCEPTION, "Access denied in storage layer: %s %s [%s]"),
            Map.entry(IO_EXCEPTION, "An IO error occurred in the storage layer: %s %s [%s]"),

            Map.entry(DUPLICATE_SUBSCRIPTION, "Duplicate subscription detected in the storage layer: %s %s [%s]"),

            Map.entry(UNKNOWN_ERROR, "An unexpected error occurred in the storage layer: %s %s [%s]"),

            Map.entry(CHUNK_NOT_FULLY_WRITTEN, "Chunk was not fully written, chunk size = %d B, written = %d B"));

    private static final Map<ExplicitError, Class<? extends ETrac>> ERROR_TYPE_MAP = Map.ofEntries(
            Map.entry(STORAGE_PATH_NULL_OR_BLANK, EValidationGap.class),
            Map.entry(STORAGE_PATH_NOT_RELATIVE, EValidationGap.class),
            Map.entry(STORAGE_PATH_OUTSIDE_ROOT, EValidationGap.class),
            Map.entry(STORAGE_PATH_IS_ROOT, EValidationGap.class),
            Map.entry(STORAGE_PATH_INVALID, EValidationGap.class),

            Map.entry(SIZE_OF_DIR, EStorageRequest.class),
            Map.entry(RM_DIR_NOT_RECURSIVE, EStorageRequest.class),
            Map.entry(STAT_NOT_FILE_OR_DIR, EStorageRequest.class),

            Map.entry(NO_SUCH_FILE_EXCEPTION, EStorageRequest.class),
            Map.entry(FILE_ALREADY_EXISTS_EXCEPTION, EStorageRequest.class),
            Map.entry(DIRECTORY_NOT_FOUND_EXCEPTION, EStorageRequest.class),
            Map.entry(NOT_DIRECTORY_EXCEPTION, EStorageRequest.class),
            Map.entry(ACCESS_DENIED_EXCEPTION, EStorageAccess.class),
            Map.entry(SECURITY_EXCEPTION, EStorageAccess.class),
            Map.entry(IO_EXCEPTION, EStorage.class),

            Map.entry(DUPLICATE_SUBSCRIPTION, ETracInternal.class),

            Map.entry(UNKNOWN_ERROR, ETracInternal.class),

            Map.entry(CHUNK_NOT_FULLY_WRITTEN, EStorageCommunication.class));

    private final String storageKey;
    private final Logger log;

    protected StorageErrors(String storageKey, Logger log) {

        this.storageKey = storageKey;
        this.log = log;
    }

    protected abstract ExplicitError checkKnownExceptions(Throwable e);

    public ETrac handleException(Throwable e, String storagePath, String operationName) {

        if (e instanceof CompletionException && e.getCause() != null)
            e = e.getCause();

        // Error of type ETrac means the error is already handled
        if (e instanceof ETrac)
            return (ETrac) e;

        var knownException = checkKnownExceptions(e);

        return knownException != null
                ? exception(knownException, e, storagePath, operationName)
                : exception(UNKNOWN_ERROR, e, storagePath, operationName);
    }

    public ETrac explicitError(ExplicitError error, String path, String operation) {

        try {

            var messageTemplate = ERROR_MESSAGE_MAP.get(error);
            var message = String.format(messageTemplate, storageKey, operation, path);

            var errType = EXPLICIT_CONSTRUCTOR_MAP.get(error);
            var err = errType.newInstance(message);

            log.error(message, err);

            return err;
        }
        catch (
                InstantiationException |
                IllegalAccessException |
                InvocationTargetException e) {

            return new EUnexpected(e);
        }
    }

    public ETrac exception(ExplicitError error, Throwable cause, String path, String operation) {

        try {

            var messageTemplate = ERROR_MESSAGE_MAP.get(error);
            var message = String.format(messageTemplate, storageKey, operation, path);

            var errType = EXCEPTION_CONSTRUCTOR_MAP.get(error);
            var err = errType.newInstance(message, cause);

            log.error(message, err);

            return err;
        }
        catch (
                InstantiationException |
                IllegalAccessException |
                InvocationTargetException e) {

            return new EUnexpected(e);
        }
    }

    public ETrac chunkNotFullyWritten(int chunkBytes, int writtenBytes) {

        try {

            var messageTemplate = ERROR_MESSAGE_MAP.get(CHUNK_NOT_FULLY_WRITTEN);
            var message = String.format(messageTemplate, chunkBytes, writtenBytes);log.error(message);

            var errType = EXPLICIT_CONSTRUCTOR_MAP.get(CHUNK_NOT_FULLY_WRITTEN);
            var err = errType.newInstance(message);

            log.error(message, err);

            return err;
        }
        catch (
                InstantiationException |
                IllegalAccessException |
                InvocationTargetException e) {

            return new EUnexpected(e);
        }
    }

    // Look up all the exception constructors at startup, avoid weird reflection errors at runtime!

    private static final Map<ExplicitError, Constructor<? extends ETrac>> EXPLICIT_CONSTRUCTOR_MAP =
            Arrays.stream(ExplicitError.values())
            .collect(Collectors.toMap(
                    e -> e,
                    e -> explicitErrorConstructor(ERROR_TYPE_MAP.get(e))));


    private static final Map<ExplicitError, Constructor<? extends ETrac>> EXCEPTION_CONSTRUCTOR_MAP =
            Arrays.stream(ExplicitError.values())
            .collect(Collectors.toMap(
                    e -> e,
                    e -> exceptionErrorConstructor(ERROR_TYPE_MAP.get(e))));

    private static Constructor<? extends ETrac> explicitErrorConstructor(Class<? extends ETrac> errorClass) {

        try {
            return errorClass.getConstructor(String.class);
        }
        catch (NoSuchMethodException e) {
            throw new EUnexpected(e);
        }
    }

    private static Constructor<? extends ETrac> exceptionErrorConstructor(Class<? extends ETrac> errorClass) {

        try {
            return errorClass.getConstructor(String.class, Throwable.class);
        }
        catch (NoSuchMethodException e) {
            throw new EUnexpected(e);
        }
    }
}
