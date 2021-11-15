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

import com.accenture.trac.api.*;
import com.accenture.trac.common.concurrent.Futures;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.metadata.*;
import com.accenture.trac.test.data.SampleDataFormats;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.Vector;
import java.util.stream.Collectors;

import static com.accenture.trac.common.metadata.MetadataUtil.selectorFor;
import static com.accenture.trac.common.metadata.MetadataUtil.selectorForLatest;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.resultOf;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.waitFor;
import static com.accenture.trac.test.helpers.TestResourceHelpers.loadResourceAsByteString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class DatasetRwOperationsTest extends DataApiTestBase {

    // Functional test cases for dataset operations in the data API
    // (createDataset, updateDataset, readDataset)

    // Reuse sample data from the test lib

    static final SchemaDefinition BASIC_SCHEMA = SampleDataFormats.BASIC_TABLE_SCHEMA;
    static final ByteString BASIC_CSV_CONTENT = loadResourceAsByteString(SampleDataFormats.BASIC_CSV_DATA_RESOURCE);

    static final List<TagUpdate> BASIC_TAG_UPDATES = List.of(
            TagUpdate.newBuilder()
                    .setAttrName("dataset_name")
                    .setOperation(TagOperation.CREATE_ATTR)
                    .setValue(MetadataCodec.encodeValue("test_dataset"))
                    .build(),
            TagUpdate.newBuilder()
                    .setAttrName("description")
                    .setOperation(TagOperation.CREATE_ATTR)
                    .setValue(MetadataCodec.encodeValue("Describes what this dataset does in the app"))
                    .build());

    static final DataWriteRequest BASIC_CREATE_DATASET_REQUEST = DataWriteRequest.newBuilder()
            .setTenant(TEST_TENANT)
            .addAllTagUpdates(BASIC_TAG_UPDATES)
            .setSchema(BASIC_SCHEMA)
            .setFormat("text/csv")
            .setContent(BASIC_CSV_CONTENT)
            .build();

    static final SchemaDefinition BASIC_SCHEMA_V2 = SampleDataFormats.BASIC_TABLE_SCHEMA_V2;
    static final ByteString BASIC_CSV_CONTENT_V2 = loadResourceAsByteString(SampleDataFormats.BASIC_CSV_DATA_RESOURCE_V2);

    static final List<TagUpdate> BASIC_TAG_UPDATES_V2 = List.of(
            TagUpdate.newBuilder()
                    .setAttrName("description")
                    .setOperation(TagOperation.REPLACE_ATTR)
                    .setValue(MetadataCodec.encodeValue("Describes what is in the V2 template"))
                    .build());

    static final DataWriteRequest BASIC_UPDATE_DATASET_REQUEST = DataWriteRequest.newBuilder()
            .setTenant(TEST_TENANT)
            .addAllTagUpdates(BASIC_TAG_UPDATES_V2)
            .setSchema(BASIC_SCHEMA_V2)
            .setFormat("text/csv")
            .setContent(BASIC_CSV_CONTENT_V2)
            .build();


    // -----------------------------------------------------------------------------------------------------------------
    // CREATE DATASET
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void createDataset_ok_data() throws Exception {

        // Basic create dataset call

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var dataId = resultOf(createDataset);

        // Get data and storage def

        var readDataDef = Futures.javaFuture(metaClient.readObject(metaReadRequest(dataId)));
        waitFor(TEST_TIMEOUT, readDataDef);
        var dataTag = resultOf(readDataDef);
        var dataDef = dataTag.getDefinition().getData();

        var readStorageDef = Futures.javaFuture(metaClient.readObject(metaReadRequest(dataDef.getStorageId())));
        waitFor(TEST_TIMEOUT, readStorageDef);
        var storageTag = resultOf(readStorageDef);
        var storageDef = storageTag.getDefinition().getStorage();

        // Get the storage path out of the metadata

        var rootPartKey = "part-root";  // TODO: Use a defined constant
        var delta = dataDef.getPartsOrThrow(rootPartKey).getSnap().getDeltas(0);
        var copy = storageDef.getDataItemsOrThrow(delta.getDataItem()).getIncarnations(0).getCopies(0);

        var storageRoot = tempDir.resolve(STORAGE_ROOT_DIR);
        var copyDir = storageRoot.resolve(copy.getStoragePath());

        var copyFiles = Files.walk(copyDir)
                .filter(p -> !p.equals(copyDir))  // Do not include the copyDir in the file list
                .collect(Collectors.toList());

        // Data should be stored in one piece (even if chunking is implemented, dataset is too small to chunk up)

        Assertions.assertEquals(1, copyFiles.size());

        var copyFile = copyFiles.get(0);

        // Check the file exists

        Assertions.assertTrue(Files.isRegularFile(copyFile));
        Assertions.assertTrue(Files.size(copyFile) > 0);

        // Decode data directly from the file in storage and check against the original
        // For this test case, data is stored in arrow file format

        var channel = Files.newByteChannel(copyFile, StandardOpenOption.READ);
        var storedData = DataApiTestHelpers.decodeArrowFile(dataDef.getSchema(), channel);

        var originalData = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA, List.of(BASIC_CSV_CONTENT));

        assertDataEqual(originalData, storedData);
    }

    @Test
    void createDataset_ok_metadata() throws Exception {

        // Basic create call
        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var dataId = resultOf(createDataset);

        Assertions.fail();
    }

    @Test
    void createDataset_ok_externalSchema() throws Exception {

        // Create external schema
        var schemaRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder().setObjectType(ObjectType.SCHEMA)
                .setSchema(BASIC_SCHEMA)).build();

        var createSchema = Futures.javaFuture(metaClient.createObject(schemaRequest));
        waitFor(TEST_TIMEOUT, createSchema);
        var schemaId = resultOf(createSchema);

        // Create a dataset using the schema
        var createRequest = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(selectorFor(schemaId))
                .build();

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, createRequest);
        waitFor(TEST_TIMEOUT, createDataset);
        var dataId = resultOf(createDataset);

        // Get data and storage def

        var readDataDef = Futures.javaFuture(metaClient.readObject(metaReadRequest(dataId)));
        waitFor(TEST_TIMEOUT, readDataDef);
        var dataTag = resultOf(readDataDef);
        var dataDef = dataTag.getDefinition().getData();

        var readStorageDef = Futures.javaFuture(metaClient.readObject(metaReadRequest(dataDef.getStorageId())));
        waitFor(TEST_TIMEOUT, readStorageDef);
        var storageTag = resultOf(readStorageDef);
        var storageDef = storageTag.getDefinition().getStorage();

        // Get the storage path out of the metadata

        var rootPartKey = "part-root";  // TODO: Use a defined constant
        var delta = dataDef.getPartsOrThrow(rootPartKey).getSnap().getDeltas(0);
        var copy = storageDef.getDataItemsOrThrow(delta.getDataItem()).getIncarnations(0).getCopies(0);

        var storageRoot = tempDir.resolve(STORAGE_ROOT_DIR);
        var copyDir = storageRoot.resolve(copy.getStoragePath());

        var copyFiles = Files.walk(copyDir)
                .filter(p -> !p.equals(copyDir))  // Do not include the copyDir in the file list
                .collect(Collectors.toList());

        // Data should be stored in one piece (even if chunking is implemented, dataset is too small to chunk up)

        Assertions.assertEquals(1, copyFiles.size());

        var copyFile = copyFiles.get(0);

        // Check the file exists

        Assertions.assertTrue(Files.isRegularFile(copyFile));
        Assertions.assertTrue(Files.size(copyFile) > 0);

        // Decode data directly from the file in storage and check against the original
        // For this test case, data is stored in arrow file format

        var channel = Files.newByteChannel(copyFile, StandardOpenOption.READ);
        var storedData = DataApiTestHelpers.decodeArrowFile(dataDef.getSchema(), channel);

        var originalData = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA, List.of(BASIC_CSV_CONTENT));

        assertDataEqual(originalData, storedData);

        // TODO: External schema metadata check
    }

    @Test
    void createDataset_priorVersionNotNull() throws Exception {

        // Prior version should not be set when creating a new dataset

        // Create an object to use as the prior, so errors will not come because the prior does not exist
        var priorResult = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, priorResult);
        var priorId = resultOf(priorResult);

        var priorNotNull = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(priorId))
                .build();

        var priorNotNullResult = DataApiTestHelpers.clientStreaming(dataClient::createDataset, priorNotNull);
        waitFor(TEST_TIMEOUT, priorNotNullResult);
        var priorNotNullError = assertThrows(StatusRuntimeException.class, () -> resultOf(priorNotNullResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, priorNotNullError.getStatus().getCode());
    }

    @Test
    void createDataset_tagUpdateInvalid() {

        var invalidTagUpdate = TagUpdate.newBuilder()
                .setAttrName("£!£$%£$%")
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        var invalidTagRequest = BASIC_CREATE_DATASET_REQUEST.toBuilder().addTagUpdates(invalidTagUpdate).build();
        var invalidTagResult = DataApiTestHelpers.clientStreaming(dataClient::createDataset, invalidTagRequest);
        waitFor(TEST_TIMEOUT, invalidTagResult);
        var invalidTagError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTagResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalidTagError.getStatus().getCode());
    }

    @Test
    void createDataset_tagUpdateReserved() {

        var reservedAttrs = List.of("trac_data_attr", "_data_attr", "__data_attr");

        for (var attrName: reservedAttrs) {

            var invalidTagUpdate = TagUpdate.newBuilder()
                    .setAttrName(attrName)
                    .setOperation(TagOperation.CREATE_ATTR)
                    .setValue(MetadataCodec.encodeValue("some_value"))
                    .build();

            var invalidTagRequest = BASIC_CREATE_DATASET_REQUEST.toBuilder().addTagUpdates(invalidTagUpdate).build();
            var invalidTagResult = DataApiTestHelpers.clientStreaming(dataClient::createDataset, invalidTagRequest);
            waitFor(TEST_TIMEOUT, invalidTagResult);
            var invalidTagError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTagResult));
            assertEquals(Status.Code.INVALID_ARGUMENT, invalidTagError.getStatus().getCode());
        }
    }

    @Test
    void createDataset_schemaOmitted() {

        // No schema info present at all, this is an invalid request

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder().clearSchemaDefinition().build();
        var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void createDataset_schemaIdInvalid() {

        var invalidSchemaId = TagSelector.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setObjectId("not_a_valid_id")
                .setLatestObject(true)
                .setLatestTag(true);

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(invalidSchemaId)
                .build();

        var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void createDataset_schemaIdNotASchema() {

        var notASchemaId = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true);

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(notASchemaId)
                .build();

        var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void createDataset_schemaIdNotFound() {

        // Create dataset with a schema ID that can't be found, should result in NOT_FOUND error code

        var unknownSchemaId = TagSelector.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true);

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder().setSchemaId(unknownSchemaId).build();
        var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void createDataset_schemaInvalid() {

        // Invalid schema is an invalid argument

        var invalidSchema = BASIC_SCHEMA.toBuilder()
                .setTable(BASIC_SCHEMA.getTable().toBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("duplicate_field_0")
                        .setFieldOrder(0)
                        .setFieldType(BasicType.BOOLEAN)));

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder().setSchema(invalidSchema).build();
        var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void createDataset_schemaEmpty() {

        // Empty schema is an invalid argument

        var emptySchema = BASIC_SCHEMA.toBuilder()
                .setSchemaType(SchemaType.TABLE)
                .setTable(TableSchema.newBuilder().build())
                .build();

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder().setSchema(emptySchema).build();
        var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void createDataset_schemaReservedFields() {

        // Empty schema is an invalid argument

        var reservedNames = List.of("trac_field_name", "_field_name", "__field_name");

        for (var fieldName: reservedNames) {

            var reservedSchema = BASIC_SCHEMA.toBuilder()
                    .setSchemaType(SchemaType.TABLE)
                    .setTable(TableSchema.newBuilder()
                            .addFields(FieldSchema.newBuilder()
                                    .setFieldName(fieldName)
                                    .setFieldOrder(0)
                                    .setFieldType(BasicType.STRING)))
                    .build();

            var request = BASIC_CREATE_DATASET_REQUEST.toBuilder().setSchema(reservedSchema).build();
            var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
            waitFor(TEST_TIMEOUT, response);
            var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
            assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
        }
    }

    @Test
    void createDataset_schemaDoesNotMatchData_multipleOptions() {
        Assertions.fail();
    }

    @Test
    void createDataset_formatEmpty() {

        // No format specified is an invalid argument

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder().clearFormat().build();
        var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void createDataset_formatInvalid() {

        // Garbled format code is an invalid argument

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder().setFormat("  $%^&-\n\n").build();
        var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void createDataset_formatNotAvailable() {

        // Format code for an unsupported type, i.e. no plugin available to support this format
        // Should be reported back as "UNIMPLEMENTED"

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder().setFormat("audio/mpeg").build();
        var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.UNIMPLEMENTED, error.getStatus().getCode());
    }

    @Test
    void createDataset_formatDoesNotMatchData() {

        // Format code does not match the supplied data (but is a supported format)
        // This should be detected as data corruption, and reported back as DATA_LOSS

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder().setFormat("text/json").build();
        var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.DATA_LOSS, error.getStatus().getCode());
    }

    @Test
    void createDataset_noContent() {

        // Empty data content should be reported as DATA_LOSS

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder().clearContent().build();
        var response = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.DATA_LOSS, error.getStatus().getCode());
    }


    // -----------------------------------------------------------------------------------------------------------------
    // UPDATE DATASET
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void updateDataset_ok_data() throws Exception {

        // Create V1 dataset

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        // Create V2 dataset

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var v2Id = resultOf(updateDataset);

        // Now check the V2 data

        // Get data and storage def

        var readDataDef = Futures.javaFuture(metaClient.readObject(metaReadRequest(v2Id)));
        waitFor(TEST_TIMEOUT, readDataDef);
        var dataTag = resultOf(readDataDef);
        var dataDef = dataTag.getDefinition().getData();

        var readStorageDef = Futures.javaFuture(metaClient.readObject(metaReadRequest(dataDef.getStorageId())));
        waitFor(TEST_TIMEOUT, readStorageDef);
        var storageTag = resultOf(readStorageDef);
        var storageDef = storageTag.getDefinition().getStorage();

        // Get the storage path out of the metadata

        var rootPartKey = "part-root";  // TODO: Use a defined constant
        var delta = dataDef.getPartsOrThrow(rootPartKey).getSnap().getDeltas(0);
        var copy = storageDef.getDataItemsOrThrow(delta.getDataItem()).getIncarnations(0).getCopies(0);

        var storageRoot = tempDir.resolve(STORAGE_ROOT_DIR);
        var copyDir = storageRoot.resolve(copy.getStoragePath());

        var copyFiles = Files.walk(copyDir)
                .filter(p -> !p.equals(copyDir))  // Do not include the copyDir in the file list
                .collect(Collectors.toList());

        // Data should be stored in one piece (even if chunking is implemented, dataset is too small to chunk up)

        Assertions.assertEquals(1, copyFiles.size());

        var copyFile = copyFiles.get(0);

        // Check the file exists

        Assertions.assertTrue(Files.isRegularFile(copyFile));
        Assertions.assertTrue(Files.size(copyFile) > 0);

        // Decode data directly from the file in storage and check against the original
        // For this test case, data is stored in arrow file format

        var channel = Files.newByteChannel(copyFile, StandardOpenOption.READ);
        var storedData = DataApiTestHelpers.decodeArrowFile(dataDef.getSchema(), channel);

        var originalData = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA_V2, List.of(BASIC_CSV_CONTENT_V2));

        assertDataEqual(originalData, storedData);
    }

    @Test
    void updateDataset_ok_metadata() {
        Assertions.fail();
    }

    @Test
    void updateDataset_ok_externalSchema() {
        Assertions.fail();
    }

    @Test
    void updateDataset_versionDuplicated() throws Exception {

        // Basic first version
        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // Use v1 as the prior version
        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .build();

        // First update should succeed

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        Assertions.assertDoesNotThrow(() -> resultOf(updateDataset));

        // Second update on the same version number should fail

        var updateDataset2 = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset2);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset2));
        Assertions.assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
    }

    @Test
    void updateDataset_versionMissing() throws Exception {

        // Basic first version
        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // Use v2 as the prior version, which does not exist
        var v2Id = v1Id.toBuilder()
                .setObjectVersion(2)
                .build();

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v2Id))
                .build();

        // Update should fail with version not found

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void updateDataset_priorVersionNull() {

        // prior version not set for an update is an invalid request

        // Basic first version
        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        Assertions.assertDoesNotThrow(() -> resultOf(createV1));

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .clearPriorVersion()
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

    }

    @Test
    void updateDataset_priorVersionInvalid() throws Exception {

        // Basic first version
        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        var invalidId = v1Id.toBuilder().setObjectId("not_a_valid_id").build();
        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder().setPriorVersion(selectorFor(invalidId)).build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void updateDataset_priorVersionWrongType() throws Exception {

        // Basic first version
        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        var wrongTypeId = v1Id.toBuilder().setObjectType(ObjectType.SCHEMA).build();
        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder().setPriorVersion(selectorFor(wrongTypeId)).build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void updateDataset_priorVersionNotFound() {

        // Look for a complete fictitious dataset, should come back as not found

        var missingDataId = TagSelector.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true);

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(missingDataId)
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void updateDataset_tagUpdateInvalid() throws Exception {

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        var invalidTagUpdate = TagUpdate.newBuilder()
                .setAttrName("£!£$%£$%")
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        var updateRequest = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .addTagUpdates(invalidTagUpdate)
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, updateRequest);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void updateDataset_tagUpdateReserved() throws Exception {

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        var reservedAttrs = List.of("trac_data_attr", "_data_attr", "__data_attr");

        for (var attrName : reservedAttrs) {

            var invalidTagUpdate = TagUpdate.newBuilder()
                    .setAttrName(attrName)
                    .setOperation(TagOperation.CREATE_ATTR)
                    .setValue(MetadataCodec.encodeValue("some_value"))
                    .build();

            var updateRequest = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                    .setPriorVersion(selectorFor(v1Id))
                    .addTagUpdates(invalidTagUpdate)
                    .build();

            var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, updateRequest);
            waitFor(TEST_TIMEOUT, updateDataset);
            var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
            Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
        }
    }

    @Test @Disabled("Required error handling not implemented yet in metadata service")
    void updateDataset_tagUpdateAttrNotFound() throws Exception {

        // Attempt to replace an attr that doesn't already exist
        // Report this as failed_precondition, rather than not_found
        // not_found would imply the file object/version itself was missing

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        var invalidTagUpdate = TagUpdate.newBuilder()
                .setAttrName("an_attr_that_is_not_set")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        var updateRequest = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .addTagUpdates(invalidTagUpdate)
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, updateRequest);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaOmitted() throws Exception {

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .clearSchemaDefinition()
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaIdInvalid() throws Exception {

        // Create external schema
        var schemaRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.SCHEMA)
                        .setSchema(BASIC_SCHEMA)).build();

        var createSchema = Futures.javaFuture(metaClient.createObject(schemaRequest));
        waitFor(TEST_TIMEOUT, createSchema);
        var schemaId = resultOf(createSchema);

        // Create V1 using external schema

        var v1Req = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(selectorFor(schemaId))
                .build();

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, v1Req);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // Invalid schema selector

        var invalidSchemaId = selectorFor(schemaId).toBuilder()
                .setObjectId("not_a_valid_id");

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchemaId(invalidSchemaId)
                .build();

        var response = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaIdNotASchema() throws Exception {

        // Create external schema
        var schemaRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.SCHEMA)
                        .setSchema(BASIC_SCHEMA)).build();

        var createSchema = Futures.javaFuture(metaClient.createObject(schemaRequest));
        waitFor(TEST_TIMEOUT, createSchema);
        var schemaId = resultOf(createSchema);

        // Create V1 using external schema

        var v1Req = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(selectorFor(schemaId))
                .build();

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, v1Req);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // Use DATA object type for schema selector

        var invalidSchemaId = selectorFor(schemaId).toBuilder()
                .setObjectType(ObjectType.DATA);

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchemaId(invalidSchemaId)
                .build();

        var response = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaIdNotFound() throws Exception {

        // External schema ID cannot be found - should be reported as NOT_FOUND

        // Create external schema
        var schemaRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                        .setObjectType(ObjectType.SCHEMA)
                        .setSchema(BASIC_SCHEMA)).build();

        var createSchema = Futures.javaFuture(metaClient.createObject(schemaRequest));
        waitFor(TEST_TIMEOUT, createSchema);
        var schemaId = resultOf(createSchema);

        // Create V1 using external schema

        var v1Req = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(selectorFor(schemaId))
                .build();

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, v1Req);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // Set a random object ID in the schema selector

        var invalidSchemaId = selectorFor(schemaId).toBuilder()
                .setObjectId(UUID.randomUUID().toString());

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchemaId(invalidSchemaId)
                .build();

        var response = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, response);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(response));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaInvalid() throws Exception {

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // Create an invalid schema - add a field with a duplicate field name

        var invalidSchema = BASIC_SCHEMA_V2.toBuilder()
                .setTable(BASIC_SCHEMA_V2.getTable().toBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("string_field")
                        .setFieldOrder(8)
                        .setFieldType(BasicType.STRING)))
                .build();

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchema(invalidSchema)
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaEmpty() throws Exception {

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        var emptySchema = BASIC_SCHEMA_V2.toBuilder()
                .setTable(BASIC_SCHEMA_V2.getTable().toBuilder()
                .clearFields())
                .build();

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchema(emptySchema)
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaIncompatible_forExternal() {
        Assertions.fail();
    }

    @Test
    void updateDataset_schemaIncompatible_forEmbedded() {
        Assertions.fail();
    }

    @Test
    void updateDataset_schemaIncompatible_externalToEmbedded() {
        Assertions.fail();
    }

    @Test
    void updateDataset_schemaIncompatible_embeddedToExternal() {
        Assertions.fail();
    }

    @Test
    void updateDataset_schemaDoesNotMatchData_multipleOptions() {
        Assertions.fail();
    }

    @Test
    void updateDataset_formatBlank() throws Exception {

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .clearFormat()
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void updateDataset_formatInvalid() throws Exception {

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setFormat("|/.,¬!£$$£^")
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void updateDataset_formatNotAvailable() throws Exception {

        // No format plugin available for the requested upload format
        // Should be reported as unimplemented

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setFormat("audio/mpeg")
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.UNIMPLEMENTED, error.getStatus().getCode());
    }

    @Test
    void updateDataset_formatDoesNotMatchData() throws Exception {

        // Format code does not match the supplied data (but is a supported format)
        // This should be detected as data corruption, and reported back as DATA_LOSS

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setFormat("application/vnd.apache.arrow.stream")
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.DATA_LOSS, error.getStatus().getCode());
    }

    @Test
    void updateDataset_noContent() {
        Assertions.fail();
    }


    // -----------------------------------------------------------------------------------------------------------------
    // READ DATASET
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void readDataset_ok_data() throws Exception {

        // Create an object to read
        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var dataId = resultOf(createDataset);

        var request = readRequest(dataId);
        var readDataset = DataApiTestHelpers.serverStreaming(dataClient::readDataset, request, execContext);
        waitFor(TEST_TIMEOUT, readDataset);
        var responseList = resultOf(readDataset);

        // First response message should contain metadata only, with an empty buffer
        var response0 = responseList.get(0);
        Assertions.assertEquals(ByteString.EMPTY, response0.getContent());

        // The remainder of the list should contain the file content
        var content = responseList.stream().skip(1)
                .map(DataReadResponse::getContent)
                .reduce(ByteString.EMPTY, ByteString::concat);

        var originalData = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA, List.of(BASIC_CSV_CONTENT));
        var responseData = DataApiTestHelpers.decodeCsv(response0.getSchema(), List.of(content));

        assertDataEqual(originalData, responseData);
    }

    private void assertDataEqual(List<Vector<Object>> originalData, List<Vector<Object>> responseData) {

        Assertions.assertEquals(originalData.size(), responseData.size());

        for (var col = 0; col < originalData.size(); col++) {
            Assertions.assertEquals(originalData.get(col).size(), responseData.get(col).size());
            for (var row = 0; row < originalData.get(col).size(); row++) {

                var originalVal = originalData.get(col).get(row);
                var responseVal = responseData.get(col).get(row);

                // Decimals must be checked using compareTo, equals() does not handle equal values with different scale
                if (originalVal instanceof BigDecimal)
                    Assertions.assertEquals(0, ((BigDecimal) originalVal).compareTo((BigDecimal) responseVal));
                else
                    Assertions.assertEquals(originalVal, responseVal);
            }
        }
    }

    @Test
    void readDataset_ok_metadata() throws Exception {

        // Create an object to read
        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var dataId = resultOf(createDataset);

        var request = readRequest(dataId);
        var readDataset = DataApiTestHelpers.serverStreaming(dataClient::readDataset, request, execContext);
        waitFor(TEST_TIMEOUT, readDataset);
        var responseList = resultOf(readDataset);

        // First response message should contain the full schema
        var response0 = responseList.get(0);
        Assertions.assertEquals(BASIC_SCHEMA, response0.getSchema());

        // No subsequent messages should contain any metadata
        for (var i = 1; i < responseList.size(); i++) {
            var msg = responseList.get(i);
            Assertions.assertFalse(msg.hasSchema());
        }
    }

    @Test
    void readDataset_ok_externalSchema() throws Exception {

        // Create external schema
        var schemaRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder().setObjectType(ObjectType.SCHEMA)
                .setSchema(BASIC_SCHEMA)).build();

        var createSchema = Futures.javaFuture(metaClient.createObject(schemaRequest));
        waitFor(TEST_TIMEOUT, createSchema);
        var schemaId = resultOf(createSchema);

        // Create a dataset using the schema
        var createRequest = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(selectorFor(schemaId))
                .build();

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, createRequest);
        waitFor(TEST_TIMEOUT, createDataset);
        var dataId = resultOf(createDataset);

        var request = readRequest(dataId);
        var readDataset = DataApiTestHelpers.serverStreaming(dataClient::readDataset, request, execContext);
        waitFor(TEST_TIMEOUT, readDataset);
        var responseList = resultOf(readDataset);

        // First response message should contain the full schema
        // Even though the schema is an external object, its schema def is still included in the read response

        var response0 = responseList.get(0);
        Assertions.assertEquals(BASIC_SCHEMA, response0.getSchema());

        // Now also check the data

        var content = responseList.stream().skip(1)
                .map(DataReadResponse::getContent)
                .reduce(ByteString.EMPTY, ByteString::concat);

        var originalData = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA, List.of(BASIC_CSV_CONTENT));
        var responseData = DataApiTestHelpers.decodeCsv(response0.getSchema(), List.of(content));

        assertDataEqual(originalData, responseData);
    }

    @Test
    void readDataset_ok_latestVersion() throws Exception {

        // Basic first version
        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // Create a request to read the latest version of the dataset

        var latestSelector = selectorForLatest(v1Id);
        var latestDataRequest = readRequest(latestSelector);

        // Read latest - should return V1

        var readDataset = DataApiTestHelpers.serverStreaming(dataClient::readDataset, latestDataRequest, execContext);
        waitFor(TEST_TIMEOUT, readDataset);
        var responseList = resultOf(readDataset);

        var response0 = responseList.get(0);
        var content = responseList.stream().skip(1)
                .map(DataReadResponse::getContent)
                .reduce(ByteString.EMPTY, ByteString::concat);

        var originalData = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA, List.of(BASIC_CSV_CONTENT));
        var responseData = DataApiTestHelpers.decodeCsv(response0.getSchema(), List.of(content));

        Assertions.assertEquals(BASIC_SCHEMA, response0.getSchema());
        assertDataEqual(originalData, responseData);

        // Now update the dataset to V2

        var updateRequest = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, updateRequest);
        waitFor(TEST_TIMEOUT, updateDataset);
        Assertions.assertDoesNotThrow(() -> resultOf(updateDataset));

        // Re-read the dataset with the same request object, i.e. read latest
        // Should now return V2

        var readDataset2 = DataApiTestHelpers.serverStreaming(dataClient::readDataset, latestDataRequest, execContext);
        waitFor(TEST_TIMEOUT, readDataset2);
        var responseList2 = resultOf(readDataset2);

        var response0_2 = responseList2.get(0);
        var content2 = responseList2.stream().skip(1)
                .map(DataReadResponse::getContent)
                .reduce(ByteString.EMPTY, ByteString::concat);

        var originalData2 = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA_V2, List.of(BASIC_CSV_CONTENT_V2));
        var responseData2 = DataApiTestHelpers.decodeCsv(response0_2.getSchema(), List.of(content2));

        Assertions.assertEquals(BASIC_SCHEMA_V2, response0_2.getSchema());
        assertDataEqual(originalData2, responseData2);
    }

    @Test
    void readDataset_ok_explicitVersion() throws Exception {

        // Basic first version
        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // Now update the dataset to V2

        var updateRequest = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, updateRequest);
        waitFor(TEST_TIMEOUT, updateDataset);
        var v2Id = resultOf(updateDataset);

        // Explicitly read V2 version

        var v2Selector = selectorFor(v2Id);
        var v2Request = readRequest(v2Selector);
        var readDataset2 = DataApiTestHelpers.serverStreaming(dataClient::readDataset, v2Request, execContext);
        waitFor(TEST_TIMEOUT, readDataset2);
        var responseList2 = resultOf(readDataset2);

        var response0_2 = responseList2.get(0);
        var content2 = responseList2.stream().skip(1)
                .map(DataReadResponse::getContent)
                .reduce(ByteString.EMPTY, ByteString::concat);

        var originalData2 = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA_V2, List.of(BASIC_CSV_CONTENT_V2));
        var responseData2 = DataApiTestHelpers.decodeCsv(response0_2.getSchema(), List.of(content2));

        Assertions.assertEquals(BASIC_SCHEMA_V2, response0_2.getSchema());
        assertDataEqual(originalData2, responseData2);

        // Explicitly read V1 version, should be unchanged

        var v1Selector = selectorFor(v1Id);
        var v1Request = readRequest(v1Selector);
        var readDataset = DataApiTestHelpers.serverStreaming(dataClient::readDataset, v1Request, execContext);
        waitFor(TEST_TIMEOUT, readDataset);
        var responseList = resultOf(readDataset);

        var response0 = responseList.get(0);
        var content = responseList.stream().skip(1)
                .map(DataReadResponse::getContent)
                .reduce(ByteString.EMPTY, ByteString::concat);

        var originalData = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA, List.of(BASIC_CSV_CONTENT));
        var responseData = DataApiTestHelpers.decodeCsv(response0.getSchema(), List.of(content));

        Assertions.assertEquals(BASIC_SCHEMA, response0.getSchema());
        assertDataEqual(originalData, responseData);
    }

    @Test
    void readDataset_ok_versionAsOf() throws Exception {

        // Basic first version
        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // V1 timestamp

        Thread.sleep(10);
        var v1Timestamp = Instant.now().atOffset(ZoneOffset.UTC);
        Thread.sleep(10);

        // Now update the dataset to V2

        var updateRequest = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, updateRequest);
        waitFor(TEST_TIMEOUT, updateDataset);
        var v2Id = resultOf(updateDataset);

        // V2 timestamp

        Thread.sleep(10);
        var v2Timestamp = Instant.now().atOffset(ZoneOffset.UTC);
        Thread.sleep(10);

        // Read V2 by as-of time

        var v2Selector = selectorFor(v2Id).toBuilder()
                .setObjectAsOf(MetadataCodec.encodeDatetime(v2Timestamp))
                .setLatestTag(true)
                .build();

        var v2Request = readRequest(v2Selector);
        var readDataset2 = DataApiTestHelpers.serverStreaming(dataClient::readDataset, v2Request, execContext);
        waitFor(TEST_TIMEOUT, readDataset2);
        var responseList2 = resultOf(readDataset2);

        var response0_2 = responseList2.get(0);
        var content2 = responseList2.stream().skip(1)
                .map(DataReadResponse::getContent)
                .reduce(ByteString.EMPTY, ByteString::concat);

        var originalData2 = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA_V2, List.of(BASIC_CSV_CONTENT_V2));
        var responseData2 = DataApiTestHelpers.decodeCsv(response0_2.getSchema(), List.of(content2));

        Assertions.assertEquals(BASIC_SCHEMA_V2, response0_2.getSchema());
        assertDataEqual(originalData2, responseData2);

        // Read V1 by as-of time

        var v1Selector = selectorFor(v1Id).toBuilder()
                .setObjectAsOf(MetadataCodec.encodeDatetime(v1Timestamp))
                .setLatestTag(true)
                .build();

        var v1Request = readRequest(v1Selector);
        var readDataset = DataApiTestHelpers.serverStreaming(dataClient::readDataset, v1Request, execContext);
        waitFor(TEST_TIMEOUT, readDataset);
        var responseList = resultOf(readDataset);

        var response0 = responseList.get(0);
        var content = responseList.stream().skip(1)
                .map(DataReadResponse::getContent)
                .reduce(ByteString.EMPTY, ByteString::concat);

        var originalData = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA, List.of(BASIC_CSV_CONTENT));
        var responseData = DataApiTestHelpers.decodeCsv(response0.getSchema(), List.of(content));

        Assertions.assertEquals(BASIC_SCHEMA, response0.getSchema());
        assertDataEqual(originalData, responseData);
    }

    @Test
    void readDataset_selectorTypeOmitted() throws Exception {

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .clearObjectType())
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void readDataset_selectorTypeNotData() throws Exception {

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectType(ObjectType.SCHEMA))
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void readDataset_selectorIdOmitted() throws Exception {

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .clearObjectId())
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void readDataset_selectorIdInvalid() throws Exception {

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectId("not_a_valid_id"))
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void readDataset_selectorIdNotFound() throws Exception {

        var objId = UUID.randomUUID().toString();  // non-existent object ID to look for

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectId(objId))
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void readDataset_objectVersionOmitted() throws Exception {

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .clearObjectVersionCriteria())
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void readDataset_objectVersionInvalid() throws Exception {

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        // Try to read obj version 0 - versions should start at 1

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectVersion(0))
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        // Try to read as-of an invalid datetime

        var readRequest2 = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectAsOf(DatetimeValue.newBuilder()
                .setIsoDatetime("2021-10-03T12:34:56+01:00+invalid/part")))
                .build();

        var readDataset2 = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest2, execContext);

        waitFor(TEST_TIMEOUT, readDataset2);
        var error2 = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset2));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    void readDataset_objectVersionNotFound() throws Exception {

        var objVer = 2;  // non-existent object version to look for

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                        .setObjectVersion(objVer))
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void readDataset_objectVersionNotFoundAsOf() throws Exception {

        var timeBeforeTest = Instant.now().atOffset(ZoneOffset.UTC);
        Thread.sleep(10);

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectAsOf(MetadataCodec.encodeDatetime(timeBeforeTest)))
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void readDataset_tagVersionOmitted() throws Exception {

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .clearTagVersionCriteria())
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void readDataset_tagVersionInvalid() throws Exception {

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        // Try to read obj version 0 - versions should start at 1

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setTagVersion(0))
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void readDataset_tagVersionNotFound() throws Exception {

        var tagVer = 2;  // non-existent tag version to look for

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setTagVersion(tagVer))
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void readDataset_tagVersionNotFoundAsOf() throws Exception {

        var timeBeforeTest = Instant.now().atOffset(ZoneOffset.UTC);
        Thread.sleep(10);

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setTagAsOf(MetadataCodec.encodeDatetime(timeBeforeTest)))
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    }

    @Test
    void readDataset_formatOmitted() throws Exception {

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .clearFormat()
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void readDataset_formatInvalid() throws Exception {

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setFormat("£$T^H$)@--0234509345asd3fg!§")
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void readDataset_formatNotSupported() throws Exception {

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setFormat("audio/mpeg")
                .build();

        var readDataset = DataApiTestHelpers.serverStreamingDiscard(dataClient::readDataset, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readDataset);
        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(readDataset));
        assertEquals(Status.Code.UNIMPLEMENTED, error.getStatus().getCode());
    }

    static DataReadRequest readRequest(TagHeader dataId) {

        var dataSelector = selectorFor(dataId);

        return readRequest(dataSelector);
    }

    static DataReadRequest readRequest(TagSelector dataSelector) {

        return DataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(dataSelector)
                .setFormat("text/csv")
                .build();
    }

    static MetadataReadRequest metaReadRequest(TagHeader header) {

        return metaReadRequest(selectorFor(header));
    }

    static MetadataReadRequest metaReadRequest(TagSelector selector) {

        return MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selector)
                .build();
    }
}
