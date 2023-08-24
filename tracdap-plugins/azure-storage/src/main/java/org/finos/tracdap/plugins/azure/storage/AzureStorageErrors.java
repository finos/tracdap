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

package org.finos.tracdap.plugins.azure.storage;

import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import org.finos.tracdap.common.storage.StorageErrors;

import java.util.Map;


public class AzureStorageErrors extends StorageErrors {

    public static Map<BlobErrorCode, ExplicitError> BLOB_ERROR_CODE_MAP = Map.ofEntries(
            Map.entry(BlobErrorCode.BLOB_NOT_FOUND, ExplicitError.OBJECT_NOT_FOUND),
            Map.entry(BlobErrorCode.INVALID_RANGE, ExplicitError.OBJECT_SIZE_TOO_SMALL));

    public AzureStorageErrors(String storageKey) {
        super(storageKey);
    }

    @Override
    protected ExplicitError checkKnownExceptions(Throwable error) {

        if (error instanceof BlobStorageException) {
            return checkBlobStorageErrorCode((BlobStorageException) error);
        }

        return null;
    }

    private ExplicitError checkBlobStorageErrorCode(BlobStorageException error) {

        return BLOB_ERROR_CODE_MAP.getOrDefault(error.getErrorCode(), null);
    }
}
