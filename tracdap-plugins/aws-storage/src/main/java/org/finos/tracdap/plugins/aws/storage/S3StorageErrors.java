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
import org.slf4j.Logger;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.List;
import java.util.Map;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;


public class S3StorageErrors extends StorageErrors {

    private static final List<Map.Entry<Integer, ExplicitError>> HTTP_ERROR_CODE_MAP = List.of(
            Map.entry(HttpStatusCode.NOT_FOUND, OBJECT_NOT_FOUND),
            Map.entry(HttpStatusCode.FORBIDDEN, ACCESS_DENIED));

//            Map.entry(DirectoryNotEmptyException.class, DIRECTORY_NOT_FOUND_EXCEPTION),
//            Map.entry(NotDirectoryException.class, NOT_DIRECTORY_EXCEPTION),
//            Map.entry(AccessDeniedException.class, ACCESS_DENIED_EXCEPTION),
//            Map.entry(SecurityException.class, SECURITY_EXCEPTION),
//            // IOException must be last in the list, not to obscure most specific exceptions
//            Map.entry(IOException.class, IO_EXCEPTION));

    public S3StorageErrors(String storageKey, Logger log) {

        super(storageKey, log);
    }

    @Override
    protected ExplicitError checkKnownExceptions(Throwable e) {

        if (!(e instanceof S3Exception))
            return null;

        var s3Error = (S3Exception) e;

        for (var entry : HTTP_ERROR_CODE_MAP) {

            if (s3Error.statusCode() == entry.getKey())
                return entry.getValue();
        }

        return null;
    }
}
