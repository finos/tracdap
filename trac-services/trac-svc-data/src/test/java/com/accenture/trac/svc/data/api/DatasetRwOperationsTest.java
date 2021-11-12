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

import com.accenture.trac.api.DataReadRequest;
import com.accenture.trac.api.DataReadResponse;
import com.accenture.trac.api.DataWriteRequest;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.metadata.*;
import com.accenture.trac.test.data.SampleDataFormats;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static com.accenture.trac.common.metadata.MetadataUtil.selectorFor;
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


    // -----------------------------------------------------------------------------------------------------------------
    // CREATE DATASET
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void createDataset_ok_data() throws Exception {
        Assertions.fail();
    }

    @Test
    void createDataset_ok_metadata() {
        Assertions.fail();
    }

    @Test
    void createDataset_ok_externalSchema() {
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
        Assertions.fail();
    }

    @Test
    void createDataset_schemaIdNotASchema() {
        Assertions.fail();
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
    void updateDataset_ok_data() {
        Assertions.fail();
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
    void updateDataset_versionDuplicated() {
        Assertions.fail();
    }

    @Test
    void updateDataset_versionMissing() {
        Assertions.fail();
    }

    @Test
    void updateDataset_priorVersionNull() {
        Assertions.fail();
    }

    @Test
    void updateDataset_priorVersionInvalid() {
        Assertions.fail();
    }

    @Test
    void updateDataset_priorVersionWrongType() {
        Assertions.fail();
    }

    @Test
    void updateDataset_priorVersionNotFound() {
        Assertions.fail();
    }

    @Test
    void updateDataset_tagUpdateInvalid() {
        Assertions.fail();
    }

    @Test
    void updateDataset_tagUpdateReserved() {
        Assertions.fail();
    }

    @Test
    void updateDataset_tagUpdateAttrNotFound() {
        Assertions.fail();
    }

    @Test
    void updateDataset_schemaOmitted() {
        Assertions.fail();
    }

    @Test
    void updateDataset_schemaIdInvalid() {
        Assertions.fail();
    }

    @Test
    void updateDataset_schemaIdNotASchema() {
        Assertions.fail();
    }

    @Test
    void updateDataset_schemaIdNotFound() {
        Assertions.fail();
    }

    @Test
    void updateDataset_schemaInvalid() {
        Assertions.fail();
    }

    @Test
    void updateDataset_schemaEmpty() {
        Assertions.fail();
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
    void updateDataset_formatBlank() {
        Assertions.fail();
    }

    @Test
    void updateDataset_formatInvalid() {
        Assertions.fail();
    }

    @Test
    void updateDataset_formatNotAvailable() {
        Assertions.fail();
    }

    @Test
    void updateDataset_formatDoesNotMatchData() {
        Assertions.fail();
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

        // Create an object to use as the prior, so errors will not come because the prior does not exist
        var priorResult = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, priorResult);
        var priorId = resultOf(priorResult);

        var request = readRequest(priorId);
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

        var originalData = DataApiTestHelpers.decodeCsv(response0.getSchema(), List.of(BASIC_CSV_CONTENT));
        var responseData = DataApiTestHelpers.decodeCsv(response0.getSchema(), List.of(content));

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
    void readDataset_ok_metadata() {
        Assertions.fail();
    }

    @Test
    void readDataset_ok_externalSchema() {
        Assertions.fail();
    }

    @Test
    void readDataset_ok_latestVersion() {
        Assertions.fail();
    }

    @Test
    void readDataset_ok_explicitVersion() {
        Assertions.fail();
    }

    @Test
    void readDataset_ok_versionAsOf() {
        Assertions.fail();
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

        return DataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(dataSelector)
                .setFormat("text/csv")
                .build();
    }
}
