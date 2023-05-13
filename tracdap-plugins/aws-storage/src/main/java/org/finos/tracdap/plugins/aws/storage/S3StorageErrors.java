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

package org.finos.tracdap.plugins.aws.storage;

import org.finos.tracdap.common.storage.StorageErrors;

import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;


public class S3StorageErrors extends StorageErrors {

    private static final int RANGE_NOT_SATISFIABLE = 416;

    private static final List<Map.Entry<Integer, ExplicitError>> HTTP_ERROR_CODE_MAP = List.of(
            Map.entry(HttpStatusCode.NOT_FOUND, OBJECT_NOT_FOUND),
            Map.entry(RANGE_NOT_SATISFIABLE, OBJECT_SIZE_TOO_SMALL),
            Map.entry(HttpStatusCode.FORBIDDEN, ACCESS_DENIED));

    public S3StorageErrors(String storageKey) {

        super(storageKey);
    }

    @Override
    protected ExplicitError checkKnownExceptions(Throwable e) {

        var cause = (e instanceof CompletionException) ? e.getCause() : e;

        if (!(cause instanceof S3Exception))
            return null;

        var s3Error = (S3Exception) cause;

        for (var entry : HTTP_ERROR_CODE_MAP) {

            if (s3Error.statusCode() == entry.getKey())
                return entry.getValue();
        }

        return null;
    }
}
