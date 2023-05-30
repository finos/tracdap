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

package org.finos.tracdap.plugins.gcp.storage;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.PermissionDeniedException;
import org.finos.tracdap.common.storage.StorageErrors;

import java.util.List;
import java.util.Map;


public class GcsStorageErrors extends StorageErrors {

    private static final List<Map.Entry<Class<? extends Exception>, ExplicitError>> EXCEPTION_CLASS_MAP = List.of(
            Map.entry(PermissionDeniedException.class, ExplicitError.ACCESS_DENIED),
            // Top-level error for GCP API calls over gRPC - catch all mapped to generic IO error
            Map.entry(ApiException.class, ExplicitError.IO_ERROR));

    public GcsStorageErrors(String storageKey) {
        super(storageKey);
    }

    @Override
    protected ExplicitError checkKnownExceptions(Throwable error) {

        // Look in the map of error types to see if e is an expected exception
        for (var knownError : EXCEPTION_CLASS_MAP) {

            var errorClass = knownError.getKey();

            if (errorClass.isInstance(error)) {
                return knownError.getValue();
            }
        }

        return null;
    }
}
