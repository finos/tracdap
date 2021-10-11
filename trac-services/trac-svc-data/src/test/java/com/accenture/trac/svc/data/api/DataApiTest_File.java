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

package com.accenture.trac.svc.data.api;

public class DataApiTest_File {

    void testCreateFile_dataOk() {}

    void testCreateFile_metadataOk() {}

    void testCreateFile_sizeOptional() {}

    void testCreateFile_noTenant() {}

    void testCreateFile_invalidTenant() {}

    void testCreateFile_unknownTenant() {}

    void testCreateFile_priorVersionNotNull() {}

    void testCreateFile_noName() {}

    void testCreateFile_invalidName() {}

    void testCreateFile_noExtension() {}

    void testCreateFile_invalidExtension() {}

    void testCreateFile_noMimeType() {}

    void testCreateFile_invalidMimeType() {}

    void testCreateFile_sizeMismatch() {}

    // TODO: Update tests

    void testReadFile_ok() {}

    void testReadFile_objectVersionLatest() {}

    void testReadFile_objectVersionExplicit() {}

    void testReadFile_objectVersionAsOf() {}

    void testReadFile_tagVersionLatest() {}

    void testReadFile_tagVersionExplicit() {}

    void testReadFile_tagVersionAsOf() {}

    void testReadFile_tenantOmitted() {}

    void testReadFile_tenantInvalid() {}

    void testReadFile_tenantNotFound() {}

    void testReadFile_selectorTypeOmitted() {}

    void testReadFile_selectorTypeNotFile() {}

    void testReadFile_selectorIdOmitted() {}

    void testReadFile_selectorIdInvalid() {}

    void testReadFile_selectorIdNotFound() {}

    void testReadFile_objectVersionMissing() {}

    void testReadFile_objectVersionInvalid() {}

    void testReadFile_objectVersionNotFound() {}

    void testReadFile_objectVersionNotFoundAsOf() {}

    void testReadFile_tagVersionMissing() {}

    void testReadFile_tagVersionInvalid() {}

    void testReadFile_tagVersionNotFound() {}

    void testReadFile_tagVersionNotFoundAsOf() {}
}
