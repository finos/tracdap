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

import com.accenture.trac.api.DataWriteRequest;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.MetadataUtil;
import com.accenture.trac.metadata.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.resultOf;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class DatasetRwOperationsTest extends DataApiTestBase {

    // Functional test cases for dataset operations in the data API
    // (createDataset, updateDataset, readDataset)

    static final String BASIC_CSV_RESOURCE_PATH = "/basic_csv_data.csv";

    static final ByteString BASIC_CSV_CONTENT = loadTextResource(
            DatasetRwOperationsTest.class, BASIC_CSV_RESOURCE_PATH);

    static final SchemaDefinition BASIC_SCHEMA = SchemaDefinition.newBuilder()
            .setSchemaType(SchemaType.TABLE)
            .setTable(TableSchema.newBuilder()
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("boolean_field")
                    .setFieldOrder(0)
                    .setFieldType(BasicType.BOOLEAN))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("integer_field")
                    .setFieldOrder(1)
                    .setFieldType(BasicType.INTEGER))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("float_field")
                    .setFieldOrder(2)
                    .setFieldType(BasicType.FLOAT))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("decimal_field")
                    .setFieldOrder(3)
                    .setFieldType(BasicType.DECIMAL))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("string_field")
                    .setFieldOrder(4)
                    .setFieldType(BasicType.DATE))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("date_field")
                    .setFieldOrder(5)
                    .setFieldType(BasicType.DATETIME))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("datetime_field")
                    .setFieldOrder(6)
                    .setFieldType(BasicType.BOOLEAN)))
            .build();

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

    private static ByteString loadTextResource(Class<?> clazz, String resourcePath) {

        try (var stream = clazz.getResourceAsStream(resourcePath)) {

            if (stream == null)
                throw new FileNotFoundException(resourcePath);

            var bytes = stream.readAllBytes();
            return ByteString.copyFrom(bytes);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


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
    void createDataset_priorVersionNotNull() throws Exception {

        // Prior version should not be set when creating a new dataset

        // Create an object to use as the prior, so errors will not come because the prior does not exist
        var priorResult = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, priorResult);
        var priorId = resultOf(priorResult);

        var priorNotNull = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(MetadataUtil.selectorFor(priorId))
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
    void createDataset_schemaDoesNotMatch_multipleOptions() {
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
    void createDataset_formatDoesNotMatch() {

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
