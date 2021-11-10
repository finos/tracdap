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

import com.accenture.trac.api.FileWriteRequest;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.metadata.TagOperation;
import com.accenture.trac.metadata.TagUpdate;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;


public class DatasetRwOperationsTest extends DataApiTestBase {

    // Functional test cases for dataset operations in the data API
    // (createDataset, updateDataset, readDataset)

    static final List<TagUpdate> BASIC_TAG_UPDATES = List.of(
            TagUpdate.newBuilder()
                    .setAttrName("app_template")
                    .setOperation(TagOperation.CREATE_ATTR)
                    .setValue(MetadataCodec.encodeValue("template_name"))
                    .build(),
            TagUpdate.newBuilder()
                    .setAttrName("description")
                    .setOperation(TagOperation.CREATE_ATTR)
                    .setValue(MetadataCodec.encodeValue("Describes what this template does in the app"))
                    .build());

    static final ByteString BASIC_FILE_CONTENT = ByteString
            .copyFrom("Sample content\n", StandardCharsets.UTF_8);

    static final FileWriteRequest BASIC_CREATE_FILE_REQUEST = FileWriteRequest.newBuilder()
            .setTenant(TEST_TENANT)
            .addAllTagUpdates(BASIC_TAG_UPDATES)
            .setName("some_file.txt")
            .setMimeType("text/plain")
            .setSize(BASIC_FILE_CONTENT.size())
            .setContent(BASIC_FILE_CONTENT)
            .build();


    // -----------------------------------------------------------------------------------------------------------------
    // CREATE DATASET
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void createDataset_dataOk() {
        Assertions.fail();
    }

    @Test
    void createDataset_metadataOk() {
        Assertions.fail();
    }

    @Test
    void createDataset_priorVersionNotNull() {
        Assertions.fail();
    }

    @Test
    void createDataset_tagUpdateInvalid() {
        Assertions.fail();
    }

    @Test
    void createDataset_tagUpdateReserved() {
        Assertions.fail();
    }

    @Test
    void createDataset_schemaOmitted() {
        Assertions.fail();
    }

    @Test
    void createDataset_schemaIdNotFound() {
        Assertions.fail();
    }

    @Test
    void createDataset_schemaInvalid() {
        Assertions.fail();
    }

    @Test
    void createDataset_schemaEmpty() {
        Assertions.fail();
    }

    @Test
    void createDataset_schemaDoesNotMatch_multipleOptions() {
        Assertions.fail();
    }

    @Test
    void createDataset_formatEmpty() {
        Assertions.fail();
    }

    @Test
    void createDataset_formatInvalid() {
        Assertions.fail();
    }

    @Test
    void createDataset_formatNotAvailable() {
        Assertions.fail();
    }

    @Test
    void createDataset_formatDoesNotMatch() {
        Assertions.fail();
    }

    @Test
    void createDataset_noContent() {
        Assertions.fail();
    }


    // -----------------------------------------------------------------------------------------------------------------
    // UPDATE DATASET
    // -----------------------------------------------------------------------------------------------------------------

    // TODO


    // -----------------------------------------------------------------------------------------------------------------
    // READ DATASET
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testReadDataset_ok_data() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_ok_metadata() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_ok_latestVersion() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_ok_explicitVersion() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_ok_versionAsOf() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_selectorTypeOmitted() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_selectorTypeNotData() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_selectorIdOmitted() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_selectorIdInvalid() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_selectorIdNotFound() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_objectVersionOmitted() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_objectVersionInvalid() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_objectVersionNotFound() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_objectVersionNotFoundAsOf() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_tagVersionOmitted() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_tagVersionInvalid() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_tagVersionNotFound() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_tagVersionNotFoundAsOf() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_formatOmitted() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_formatInvalid() {
        Assertions.fail();
    }

    @Test
    void testReadDataset_formatNotSupported() {
        Assertions.fail();
    }
}
