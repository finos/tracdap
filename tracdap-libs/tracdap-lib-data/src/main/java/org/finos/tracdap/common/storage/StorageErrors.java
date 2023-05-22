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
        STORAGE_PARAMS_INVALID,

        // Exceptions
        OBJECT_NOT_FOUND,
        OBJECT_ALREADY_EXISTS,
        OBJECT_SIZE_TOO_SMALL,
        NOT_A_FILE,
        NOT_A_DIRECTORY,
        NOT_A_FILE_OR_DIRECTORY,
        RM_DIR_NOT_RECURSIVE,
        IO_ERROR,

        // Permissions
        ACCESS_DENIED,

        // Unhandled / unexpected error
        UNKNOWN_ERROR,

        // Errors in stream (Flow pub/sub) implementation
        DUPLICATE_SUBSCRIPTION,
        CHUNK_NOT_FULLY_WRITTEN,
    }

    private static final Map<ExplicitError, String> ERROR_MESSAGE_MAP = Map.ofEntries(
            Map.entry(STORAGE_PATH_NULL_OR_BLANK, "Requested storage path is null or blank: %s %s [%s]"),
            Map.entry(STORAGE_PATH_NOT_RELATIVE, "Requested storage path is not a relative path: %s %s [%s]"),
            Map.entry(STORAGE_PATH_OUTSIDE_ROOT, "Requested storage path is outside the storage root directory: %s %s [%s]"),
            Map.entry(STORAGE_PATH_IS_ROOT, "Requested operation not allowed on the storage root directory: %s %s [%s]"),
            Map.entry(STORAGE_PATH_INVALID, "Requested storage path is invalid: %s %s [%s]"),
            Map.entry(STORAGE_PARAMS_INVALID, "Requested storage operation is invalid: %s %s [%s]: %s"),

            Map.entry(OBJECT_NOT_FOUND, "Object not found in storage layer: %s %s [%s]"),
            Map.entry(OBJECT_ALREADY_EXISTS, "Object already exists in storage layer: %s %s [%s]"),
            Map.entry(OBJECT_SIZE_TOO_SMALL, "Object is smaller than expected: %s %s [%s]"),
            Map.entry(NOT_A_FILE, "Object is not a file: %s %s [%s]"),
            Map.entry(NOT_A_DIRECTORY, "Object is not a directory: %s %s [%s]"),
            Map.entry(NOT_A_FILE_OR_DIRECTORY, "Object is not a file or directory: %s %s [%s]"),
            Map.entry(RM_DIR_NOT_RECURSIVE, "Regular delete operation not available for directories (use recursive delete): %s %s [%s]"),
            Map.entry(IO_ERROR, "An IO error occurred in the storage layer: %s %s [%s]"),

            Map.entry(ACCESS_DENIED, "Access denied in storage layer: %s %s [%s]"),

            Map.entry(UNKNOWN_ERROR, "An unexpected error occurred in the storage layer: %s %s [%s]"),

            Map.entry(DUPLICATE_SUBSCRIPTION, "Duplicate subscription detected in the storage layer: %s %s [%s]"),
            Map.entry(CHUNK_NOT_FULLY_WRITTEN, "Chunk was not fully written, chunk size = %d B, written = %d B"));

    private static final Map<ExplicitError, Class<? extends ETrac>> ERROR_TYPE_MAP = Map.ofEntries(
            Map.entry(STORAGE_PATH_NULL_OR_BLANK, EValidationGap.class),
            Map.entry(STORAGE_PATH_NOT_RELATIVE, EValidationGap.class),
            Map.entry(STORAGE_PATH_OUTSIDE_ROOT, EValidationGap.class),
            Map.entry(STORAGE_PATH_IS_ROOT, EValidationGap.class),
            Map.entry(STORAGE_PATH_INVALID, EValidationGap.class),
            Map.entry(STORAGE_PARAMS_INVALID, EValidationGap.class),

            Map.entry(OBJECT_NOT_FOUND, EStorageRequest.class),
            Map.entry(OBJECT_ALREADY_EXISTS, EStorageRequest.class),
            Map.entry(OBJECT_SIZE_TOO_SMALL, EStorageRequest.class),
            Map.entry(NOT_A_FILE, EStorageRequest.class),
            Map.entry(NOT_A_DIRECTORY, EStorageRequest.class),
            Map.entry(NOT_A_FILE_OR_DIRECTORY, EStorageRequest.class),
            Map.entry(RM_DIR_NOT_RECURSIVE, EStorageRequest.class),
            Map.entry(IO_ERROR, EStorage.class),

            Map.entry(ACCESS_DENIED, EStorageAccess.class),

            Map.entry(UNKNOWN_ERROR, ETracInternal.class),

            Map.entry(DUPLICATE_SUBSCRIPTION, ETracInternal.class),
            Map.entry(CHUNK_NOT_FULLY_WRITTEN, EStorageCommunication.class));

    private final String storageKey;

    protected StorageErrors(String storageKey) {

        this.storageKey = storageKey;
    }

    protected abstract ExplicitError checkKnownExceptions(Throwable e);

    public ETrac handleException(String operation, String path, Throwable error) {

        if (error instanceof CompletionException && error.getCause() != null)
            error = error.getCause();

        // Error of type ETrac means the error is already handled
        if (error instanceof ETrac)
            return (ETrac) error;

        var knownException = checkKnownExceptions(error);

        return knownException != null
                ? explicitError(operation, path, knownException, error)
                : explicitError(operation, path, UNKNOWN_ERROR, error);
    }

    public ETrac explicitError(String operation, String path, ExplicitError error) {

        try {

            var messageTemplate = ERROR_MESSAGE_MAP.get(error);
            var message = String.format(messageTemplate, operation, storageKey, path);

            var errType = EXPLICIT_CONSTRUCTOR_MAP.get(error);
            return errType.newInstance(message);
        }
        catch (
                InstantiationException |
                IllegalAccessException |
                InvocationTargetException e) {

            return new EUnexpected(e);
        }
    }

    public ETrac explicitError(String operation, String path, ExplicitError error, String detail) {

        try {

            var messageTemplate = ERROR_MESSAGE_MAP.get(error);
            var message = String.format(messageTemplate, operation, storageKey, operation, path, detail);

            var errType = EXPLICIT_CONSTRUCTOR_MAP.get(error);
            return errType.newInstance(message);
        }
        catch (
                InstantiationException |
                IllegalAccessException |
                InvocationTargetException e) {

            return new EUnexpected(e);
        }
    }

    public ETrac explicitError(String operation, String path, ExplicitError error, Throwable cause) {

        try {

            var messageTemplate = ERROR_MESSAGE_MAP.get(error);
            var message = String.format(messageTemplate, operation, storageKey, path);

            var errType = EXCEPTION_CONSTRUCTOR_MAP.get(error);
            return errType.newInstance(message, cause);
        }
        catch (
                InstantiationException |
                IllegalAccessException |
                InvocationTargetException e) {

            return new EUnexpected(e);
        }
    }

    public ETrac chunkNotFullyWritten(long chunkBytes, long writtenBytes) {

        try {

            var messageTemplate = ERROR_MESSAGE_MAP.get(CHUNK_NOT_FULLY_WRITTEN);
            var message = String.format(messageTemplate, chunkBytes, writtenBytes);

            var errType = EXPLICIT_CONSTRUCTOR_MAP.get(CHUNK_NOT_FULLY_WRITTEN);
            return errType.newInstance(message);
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
