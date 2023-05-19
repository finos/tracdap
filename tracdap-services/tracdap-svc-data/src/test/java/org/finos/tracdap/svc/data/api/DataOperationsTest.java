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

package org.finos.tracdap.svc.data.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.data.ExecutionContext;
import org.finos.tracdap.common.async.Futures;
import org.finos.tracdap.common.data.IExecutionContext;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.test.data.SampleData;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import org.finos.tracdap.test.helpers.PlatformTest;
import org.finos.tracdap.test.helpers.StorageTestHelpers;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import static org.finos.tracdap.common.metadata.MetadataUtil.selectorFor;
import static org.finos.tracdap.common.metadata.MetadataUtil.selectorForLatest;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.resultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.finos.tracdap.common.util.ResourceHelpers.loadResourceAsByteString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


abstract class DataOperationsTest {

    // Functional test cases for dataset operations in the data API
    // (createDataset, updateDataset, readDataset)

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TRAC_CONFIG_ENV_VAR = "TRAC_CONFIG_FILE";
    public static final String TEST_TENANT = "ACME_CORP";
    public static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    protected static EventLoopGroup elg;
    protected IExecutionContext execContext;
    protected TracMetadataApiGrpc.TracMetadataApiFutureStub metaClient;
    protected TracDataApiGrpc.TracDataApiStub dataClient;


    // Include this test case as a unit test
    static class UnitTest extends DataOperationsTest {

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
                .runDbDeploy(true)
                .addTenant(TEST_TENANT)
                .startMeta()
                .startData()
                .build();

        @BeforeAll
        static void setupClass() {
            elg = new NioEventLoopGroup(2);
        }

        @BeforeEach
        void setup() {
            execContext = new ExecutionContext(elg.next());
            metaClient = platform.metaClientFuture();
            dataClient = platform.dataClient();
        }
    }

    // Include this test case for integration against different storage backends
    @Tag("integration")
    @Tag("int-storage")
    static class IntegrationTest extends DataOperationsTest {

        private static final String TRAC_CONFIG_ENV_FILE = System.getenv(TRAC_CONFIG_ENV_VAR);

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_ENV_FILE)
                .runDbDeploy(true)
                .addTenant(TEST_TENANT)
                .startMeta()
                .startData()
                .build();

        @BeforeAll
        static void setupClass() {
            elg = new NioEventLoopGroup(2);
        }

        @BeforeEach
        void setup() {
            execContext = new ExecutionContext(elg.next());
            metaClient = platform.metaClientFuture();
            dataClient = platform.dataClient();
        }

        @AfterAll
        static void tearDownClass() throws Exception {

            var plugins = new PluginManager();
            plugins.initConfigPlugins();
            plugins.initRegularPlugins();

            var config = new ConfigManager(
                    platform.platformConfigUrl(),
                    platform.tracDir(),
                    plugins);

            StorageTestHelpers.deleteStoragePrefix(config, plugins, elg);
        }

    }

    // Reuse sample data from the test lib

    static final SchemaDefinition BASIC_SCHEMA = SampleData.BASIC_TABLE_SCHEMA;
    static final ByteString BASIC_CSV_CONTENT = loadResourceAsByteString(SampleData.BASIC_CSV_DATA_RESOURCE);

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

    static final SchemaDefinition BASIC_SCHEMA_V2 = SampleData.BASIC_TABLE_SCHEMA_V2;
    static final ByteString BASIC_CSV_CONTENT_V2 = loadResourceAsByteString(SampleData.BASIC_CSV_DATA_RESOURCE_V2);

    static final List<TagUpdate> BASIC_TAG_UPDATES_V2 = List.of(
            TagUpdate.newBuilder()
                    .setAttrName("description")
                    .setOperation(TagOperation.REPLACE_ATTR)
                    .setValue(MetadataCodec.encodeValue("Describes what is in the V2 dataset"))
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

        // Use the Data API to read and check the data
        // API tests cannot inspect the storage directly

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

    @Test
    void createDataset_ok_metadata() throws Exception {

        // Basic create call
        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var dataId = resultOf(createDataset);

        // Retrieve metadata

        var metaRequest = metaReadRequest(dataId);
        var metaResponse = Futures.javaFuture(metaClient.readObject(metaRequest));
        waitFor(TEST_TIMEOUT, metaResponse);
        var tag = resultOf(metaResponse);

        // Explicitly set attrs

        Assertions.assertTrue(tag.containsAttrs("dataset_name"));
        Assertions.assertTrue(tag.containsAttrs("description"));
        var appDatasetAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("dataset_name"));
        var descriptionAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("description"));
        Assertions.assertEquals("test_dataset", appDatasetAttr);
        Assertions.assertEquals("Describes what this dataset does in the app", descriptionAttr);

        // Check core attributes of the data definition

        var def = tag.getDefinition();
        Assertions.assertEquals(ObjectType.DATA, def.getObjectType());

        var dataDef = def.getData();
        Assertions.assertEquals(BASIC_SCHEMA, dataDef.getSchema());
        Assertions.assertEquals(1, dataDef.getPartsCount());

        // Storage ID should always point to a storage object and refer to the latest object/tag version
        // This is because storage can evolve independently of logical files/data (e.g. due to retention policy)

        Assertions.assertEquals(ObjectType.STORAGE, dataDef.getStorageId().getObjectType());
        Assertions.assertTrue(dataDef.getStorageId().getLatestObject());
        Assertions.assertTrue(dataDef.getStorageId().getLatestTag());
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

        // Use the Data API to read and check the data
        // API tests cannot inspect the storage directly

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


        // Check the metadata as well

        var metaRequest = metaReadRequest(dataId);
        var metaResponse = Futures.javaFuture(metaClient.readObject(metaRequest));
        waitFor(TEST_TIMEOUT, metaResponse);
        var tag = resultOf(metaResponse);

        // Explicitly set attrs

        Assertions.assertTrue(tag.containsAttrs("dataset_name"));
        Assertions.assertTrue(tag.containsAttrs("description"));
        var appDatasetAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("dataset_name"));
        var descriptionAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("description"));
        Assertions.assertEquals("test_dataset", appDatasetAttr);
        Assertions.assertEquals("Describes what this dataset does in the app", descriptionAttr);

        // Check core attributes of the data definition

        var def = tag.getDefinition();
        Assertions.assertEquals(ObjectType.DATA, def.getObjectType());

        var storedDataDef = def.getData();
        Assertions.assertEquals(selectorFor(schemaId), storedDataDef.getSchemaId());
        Assertions.assertEquals(1, storedDataDef.getPartsCount());

        // Storage ID should always point to a storage object and refer to the latest object/tag version
        // This is because storage can evolve independently of logical files/data (e.g. due to retention policy)

        Assertions.assertEquals(ObjectType.STORAGE, storedDataDef.getStorageId().getObjectType());
        Assertions.assertTrue(storedDataDef.getStorageId().getLatestObject());
        Assertions.assertTrue(storedDataDef.getStorageId().getLatestTag());
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

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder().clearSchemaSpecifier().build();
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
    void createDataset_schemaIdForLatestObject() throws Exception {

        // Using "latestObject" in the schema selector is not allowed
        // As this would result in a dataset version with a schema that changed over time

        // Create external schema
        var schemaRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder().setObjectType(ObjectType.SCHEMA)
                        .setSchema(BASIC_SCHEMA)).build();

        var createSchema = Futures.javaFuture(metaClient.createObject(schemaRequest));
        waitFor(TEST_TIMEOUT, createSchema);
        var schemaId = resultOf(createSchema);

        // Create data using "latestObject" of schema ID

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(selectorForLatest(schemaId))
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
                .setObjectVersion(1)
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
    void createDataset_schemaMismatch_fieldType() {

        // Schema mismatch will only be detected in the data codec when it tries to interpret data
        // This should be reported as DATA_LOSS

        var badSchema = BASIC_SCHEMA.toBuilder()
                .setTable(BASIC_SCHEMA.getTable().toBuilder()
                .removeFields(1)
                .addFields(1, BASIC_SCHEMA.getTable().getFields(1).toBuilder()
                .setFieldType(BasicType.DATETIME)))
                .build();

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchema(badSchema)
                .build();

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, createDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(createDataset));
        assertEquals(Status.Code.DATA_LOSS, error.getStatus().getCode());
    }

    @Test
    void createDataset_schemaMismatch_extraField() {

        // Extra field in the data that is not described in the schema

        // Schema mismatch will only be detected in the data codec when it tries to interpret data
        // This should be reported as DATA_LOSS

        var badSchema = BASIC_SCHEMA.toBuilder()
                .setTable(BASIC_SCHEMA.getTable().toBuilder()
                .removeFields(BASIC_SCHEMA.getTable().getFieldsCount() - 1))
                .build();

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchema(badSchema)
                .build();

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, createDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(createDataset));
        assertEquals(Status.Code.DATA_LOSS, error.getStatus().getCode());
    }

    @Test
    void createDataset_schemaMismatch_missingField() {

        // Field described in the schema but not present in the data

        // Schema mismatch will only be detected in the data codec when it tries to interpret data
        // This should be reported as DATA_LOSS

        var badSchema = BASIC_SCHEMA.toBuilder()
                .setTable(BASIC_SCHEMA.getTable().toBuilder()
                .addFields(FieldSchema.newBuilder()
                .setFieldName("an_extra_field")
                .setFieldOrder(BASIC_SCHEMA.getTable().getFieldsCount())
                .setFieldType(BasicType.STRING)))
                .build();

        var request = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchema(badSchema)
                .build();

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, request);
        waitFor(TEST_TIMEOUT, createDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(createDataset));
        assertEquals(Status.Code.DATA_LOSS, error.getStatus().getCode());
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

        // Use the Data API to read and check the data
        // API tests cannot inspect the storage directly

        var v2Request = readRequest(v2Id);
        var readDataset = DataApiTestHelpers.serverStreaming(dataClient::readDataset, v2Request, execContext);
        waitFor(TEST_TIMEOUT, readDataset);
        var responseList = resultOf(readDataset);

        // First response message should contain metadata only, with an empty buffer
        var response0 = responseList.get(0);
        Assertions.assertEquals(ByteString.EMPTY, response0.getContent());

        // The remainder of the list should contain the file content
        var content = responseList.stream().skip(1)
                .map(DataReadResponse::getContent)
                .reduce(ByteString.EMPTY, ByteString::concat);

        var originalData = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA_V2, List.of(BASIC_CSV_CONTENT_V2));
        var responseData = DataApiTestHelpers.decodeCsv(response0.getSchema(), List.of(content));

        assertDataEqual(originalData, responseData);
    }

    @Test
    void updateDataset_ok_metadata() throws Exception {

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

        // Retrieve V2 metadata

        var metaRequest = metaReadRequest(v2Id);
        var metaResponse = Futures.javaFuture(metaClient.readObject(metaRequest));
        waitFor(TEST_TIMEOUT, metaResponse);
        var tag = resultOf(metaResponse);

        // Explicitly set attrs

        Assertions.assertTrue(tag.containsAttrs("dataset_name"));
        Assertions.assertTrue(tag.containsAttrs("description"));
        var appDatasetAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("dataset_name"));
        var descriptionAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("description"));
        Assertions.assertEquals("test_dataset", appDatasetAttr);
        Assertions.assertEquals("Describes what is in the V2 dataset", descriptionAttr);

        // Check core attributes of the data definition

        var def = tag.getDefinition();
        Assertions.assertEquals(ObjectType.DATA, def.getObjectType());

        var dataDef = def.getData();
        Assertions.assertEquals(BASIC_SCHEMA_V2, dataDef.getSchema());
        Assertions.assertEquals(1, dataDef.getPartsCount());

        // Storage ID should always point to a storage object and refer to the latest object/tag version
        // This is because storage can evolve independently of logical files/data (e.g. due to retention policy)

        Assertions.assertEquals(ObjectType.STORAGE, dataDef.getStorageId().getObjectType());
        Assertions.assertTrue(dataDef.getStorageId().getLatestObject());
        Assertions.assertTrue(dataDef.getStorageId().getLatestTag());
    }

    @Test
    void updateDataset_ok_externalSchema() throws Exception {

        // Dataset is updated using an updated version of an external schema

        // Create V1 schema

        var v1SchemaReq = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setSchema(BASIC_SCHEMA)).build();

        var v1CreateSchema = Futures.javaFuture(metaClient.createObject(v1SchemaReq));
        waitFor(TEST_TIMEOUT, v1CreateSchema);
        var v1SchemaId = resultOf(v1CreateSchema);

        // Create V1 dataset using V1 external schema

        var v1Req = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(selectorFor(v1SchemaId))
                .build();
        var v1Create = DataApiTestHelpers.clientStreaming(dataClient::createDataset, v1Req);
        waitFor(TEST_TIMEOUT, v1Create);
        var v1Id = resultOf(v1Create);

        // Create V2 schema

        var v2SchemaReq = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setPriorVersion(selectorFor(v1SchemaId))
                .setDefinition(ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setSchema(BASIC_SCHEMA_V2))
                .build();

        var v2CreateSchema = Futures.javaFuture(metaClient.updateObject(v2SchemaReq));
        waitFor(TEST_TIMEOUT, v2CreateSchema);
        var v2SchemaId = resultOf(v2CreateSchema);

        // Create V2 dataset using V2 external schema

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchemaId(selectorFor(v2SchemaId))
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var v2Id = resultOf(updateDataset);

        // Now check the V2 data

        // Use the Data API to read and check the data
        // API tests cannot inspect the storage directly

        var v2Request = readRequest(v2Id);
        var readDataset = DataApiTestHelpers.serverStreaming(dataClient::readDataset, v2Request, execContext);
        waitFor(TEST_TIMEOUT, readDataset);
        var responseList = resultOf(readDataset);

        // First response message should contain metadata only, with an empty buffer
        var response0 = responseList.get(0);
        Assertions.assertEquals(ByteString.EMPTY, response0.getContent());

        // The remainder of the list should contain the file content
        var content = responseList.stream().skip(1)
                .map(DataReadResponse::getContent)
                .reduce(ByteString.EMPTY, ByteString::concat);

        var originalData = DataApiTestHelpers.decodeCsv(BASIC_SCHEMA_V2, List.of(BASIC_CSV_CONTENT_V2));
        var responseData = DataApiTestHelpers.decodeCsv(response0.getSchema(), List.of(content));

        assertDataEqual(originalData, responseData);

        // Check the metadata as well

        var metaRequest = metaReadRequest(v2Id);
        var metaResponse = Futures.javaFuture(metaClient.readObject(metaRequest));
        waitFor(TEST_TIMEOUT, metaResponse);
        var tag = resultOf(metaResponse);

        // Explicitly set attrs

        Assertions.assertTrue(tag.containsAttrs("dataset_name"));
        Assertions.assertTrue(tag.containsAttrs("description"));
        var appDatasetAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("dataset_name"));
        var descriptionAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("description"));
        Assertions.assertEquals("test_dataset", appDatasetAttr);
        Assertions.assertEquals("Describes what is in the V2 dataset", descriptionAttr);

        // Check core attributes of the data definition

        var def = tag.getDefinition();
        Assertions.assertEquals(ObjectType.DATA, def.getObjectType());

        var storedDataDef = def.getData();
        Assertions.assertEquals(selectorFor(v2SchemaId), storedDataDef.getSchemaId());
        Assertions.assertEquals(1, storedDataDef.getPartsCount());

        // Storage ID should always point to a storage object and refer to the latest object/tag version
        // This is because storage can evolve independently of logical files/data (e.g. due to retention policy)

        Assertions.assertEquals(ObjectType.STORAGE, storedDataDef.getStorageId().getObjectType());
        Assertions.assertTrue(storedDataDef.getStorageId().getLatestObject());
        Assertions.assertTrue(storedDataDef.getStorageId().getLatestTag());
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

    @Test
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
                .clearSchemaSpecifier()
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
    void updateDataset_schemaIdForLatestObject() throws Exception {

        // Using "latestObject" in the schema selector is not allowed
        // As this would result in a dataset version with a schema that changed over time

        // Create V1 schema

        var schemaReq = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setSchema(BASIC_SCHEMA)).build();

        var createSchema = Futures.javaFuture(metaClient.createObject(schemaReq));
        waitFor(TEST_TIMEOUT, createSchema);
        var schemaId = resultOf(createSchema);

        // Create V1 dataset using V1 external schema

        var v1Req = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(selectorFor(schemaId))
                .build();

        var v1Create = DataApiTestHelpers.clientStreaming(dataClient::createDataset, v1Req);
        waitFor(TEST_TIMEOUT, v1Create);
        var v1Id = resultOf(v1Create);

        // Create V2 dataset using a selector for the latest version of the schema

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchemaId(selectorForLatest(schemaId))
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
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

        // Set a non-existent schema version and try to use that for the V2 update

        var unknownSchemaId = selectorFor(schemaId).toBuilder()
                .setObjectVersion(2);

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchemaId(unknownSchemaId)
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
    void updateDataset_schemaIncompatible_forExternal() throws Exception {

        // For datasets using an external schema, the schema object ID cannot change between dataset versions
        // Even if the alternate schema ID is for a compatible schema, this is not (currently) allowed
        // Reported as a failed precondition

        // Create V1 schema

        var schemaReq = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setSchema(BASIC_SCHEMA)).build();

        var createSchema = Futures.javaFuture(metaClient.createObject(schemaReq));
        waitFor(TEST_TIMEOUT, createSchema);
        var schemaId = resultOf(createSchema);

        // Create V1 dataset using V1 external schema

        var v1Req = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(selectorFor(schemaId))
                .build();

        var v1Create = DataApiTestHelpers.clientStreaming(dataClient::createDataset, v1Req);
        waitFor(TEST_TIMEOUT, v1Create);
        var v1Id = resultOf(v1Create);

        // Now create an alternate schema with a new object ID

        var altSchemaReq = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setSchema(BASIC_SCHEMA_V2)).build();

        var altCreateSchema = Futures.javaFuture(metaClient.createObject(altSchemaReq));
        waitFor(TEST_TIMEOUT, altCreateSchema);
        var altSchemaId = resultOf(altCreateSchema);

        // Create V2 dataset using V2 external schema

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchemaId(selectorFor(altSchemaId))
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaIncompatible_forEmbedded() throws Exception {

        // Incompatible schema change in an embedded schema
        // Reported as failed precondition

        // Create V1 dataset

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        // Create V2 dataset, schema has one field removed

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchema(BASIC_SCHEMA.toBuilder()
                .setTable(BASIC_SCHEMA.getTable().toBuilder()
                .removeFields(BASIC_SCHEMA.getTable().getFieldsCount() - 1)))
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());

        // Create V2 dataset, schema has one field type changed

        var request2 = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchema(BASIC_SCHEMA.toBuilder()
                .setTable(BASIC_SCHEMA.getTable().toBuilder()
                .removeFields(BASIC_SCHEMA.getTable().getFieldsCount() - 1)
                .addFields(BASIC_SCHEMA.getTable()
                        .getFields(BASIC_SCHEMA.getTable().getFieldsCount() -1)
                        .toBuilder()
                        .setFieldType(BasicType.STRING))))
                .build();

        var updateDataset2 = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request2);
        waitFor(TEST_TIMEOUT, updateDataset2);
        var error2 = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset2));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, error2.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaIncompatible_externalToEmbedded() throws Exception {

        // Switching from external to embedded schema between versions is not currently supported
        // Even if the schemas are compatible

        // Create V1 schema

        var schemaReq = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setSchema(BASIC_SCHEMA)).build();

        var createSchema = Futures.javaFuture(metaClient.createObject(schemaReq));
        waitFor(TEST_TIMEOUT, createSchema);
        var schemaId = resultOf(createSchema);

        // Create V1 dataset using V1 external schema

        var v1Req = BASIC_CREATE_DATASET_REQUEST.toBuilder()
                .setSchemaId(selectorFor(schemaId))
                .build();

        var v1Create = DataApiTestHelpers.clientStreaming(dataClient::createDataset, v1Req);
        waitFor(TEST_TIMEOUT, v1Create);
        var v1Id = resultOf(v1Create);

        // Create V2 using a compatible embedded schema

        var request2 = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .build();

        var updateDataset2 = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request2);
        waitFor(TEST_TIMEOUT, updateDataset2);
        var error2 = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset2));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, error2.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaIncompatible_embeddedToExternal() throws Exception {

        // Switching from external to embedded schema between versions is not currently supported
        // Even if the schemas are compatible

        // Create V1 dataset with embedded schema

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        // Create compativle external schema object

        var schemaReq = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.SCHEMA)
                .setDefinition(ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setSchema(BASIC_SCHEMA_V2))
                .build();

        var createSchema = Futures.javaFuture(metaClient.createObject(schemaReq));
        waitFor(TEST_TIMEOUT, createSchema);
        var schemaId = resultOf(createSchema);

        // Create V2 dataset using the external schema

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchemaId(selectorFor(schemaId))
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error2 = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, error2.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaMismatch_fieldType() throws Exception {

        // Schema mismatch will only be detected in the data codec when it tries to interpret data
        // This should be reported as DATA_LOSS

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // Make the last field, which is a string, be an integer in the schema
        // Codec should fail to parse an integer value
        var badSchema = BASIC_SCHEMA_V2.toBuilder()
                .setTable(BASIC_SCHEMA_V2.getTable().toBuilder()
                .removeFields(7)
                .addFields(7, BASIC_SCHEMA_V2.getTable().getFields(7).toBuilder()
                .setFieldType(BasicType.INTEGER)))
                .build();

        var v2Request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchema(badSchema)
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, v2Request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        assertEquals(Status.Code.DATA_LOSS, error.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaMismatch_extraField() throws Exception {

        // Extra field in the data that is not described in the schema

        // Schema mismatch will only be detected in the data codec when it tries to interpret data
        // This should be reported as DATA_LOSS

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // Remove the last (new) field from the updated schema
        // Removing a field that exists in V1 would fail version validation upfront
        var badSchema = BASIC_SCHEMA_V2.toBuilder()
                .setTable(BASIC_SCHEMA_V2.getTable().toBuilder()
                .removeFields(7))
                .build();

        var v2Request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchema(badSchema)
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, v2Request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        assertEquals(Status.Code.DATA_LOSS, error.getStatus().getCode());
    }

    @Test
    void updateDataset_schemaMismatch_missingField() throws Exception {

        // Field described in the schema but not present in the data

        // Schema mismatch will only be detected in the data codec when it tries to interpret data
        // This should be reported as DATA_LOSS

        var createV1 = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createV1);
        var v1Id = resultOf(createV1);

        // Remove the last (new) field from the updated schema
        // Removing a field that exists in V1 would fail version validation upfront
        var badSchema = BASIC_SCHEMA_V2.toBuilder()
                .setTable(BASIC_SCHEMA_V2.getTable().toBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("an_extra_field")
                        .setFieldOrder(BASIC_SCHEMA_V2.getTable().getFieldsCount())
                        .setFieldType(BasicType.STRING)))
                .build();

        var v2Request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .setSchema(badSchema)
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, v2Request);
        waitFor(TEST_TIMEOUT, updateDataset);
        var error = Assertions.assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        assertEquals(Status.Code.DATA_LOSS, error.getStatus().getCode());
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
    void updateDataset_noContent() throws Exception {

        // Create V1 dataset

        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var v1Id = resultOf(createDataset);

        // Create V2 dataset

        var request = BASIC_UPDATE_DATASET_REQUEST.toBuilder()
                .setPriorVersion(selectorFor(v1Id))
                .clearContent()
                .build();

        var updateDataset = DataApiTestHelpers.clientStreaming(dataClient::updateDataset, request);
        waitFor(TEST_TIMEOUT, updateDataset);

        var error = assertThrows(StatusRuntimeException.class, () -> resultOf(updateDataset));
        assertEquals(Status.Code.DATA_LOSS, error.getStatus().getCode());
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

        // By default, offset = 0 and limit = length of first response column

        assertDataEqualForRange(originalData, responseData, 0, originalData.get(0).size());
    }

    private void assertDataEqualForRange(
            List<Vector<Object>> originalData, List<Vector<Object>> responseData,
            long offset, long limit) {

        Assertions.assertEquals(originalData.size(), responseData.size());

        for (var col = 0; col < originalData.size(); col++) {
            Assertions.assertEquals(limit, responseData.get(col).size());
            for (var row = 0; row < limit; row++) {

                var originalVal = originalData.get(col).get(row + (int) offset);
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
    void readDataset_ok_limitAndOffset() throws Exception {

        // Create an object to read
        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, BASIC_CREATE_DATASET_REQUEST);
        waitFor(TEST_TIMEOUT, createDataset);
        var dataId = resultOf(createDataset);

        var readRequest0 = readRequest(dataId)
                .toBuilder()
                .setLimit(2)
                .build();

        var readDataset = DataApiTestHelpers.serverStreaming(dataClient::readDataset, readRequest0, execContext);
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

        assertDataEqualForRange(originalData, responseData, 0, 2);

        var readRequest1 = readRequest0
                .toBuilder()
                .setOffset(2)
                .build();

        var readDataset1 = DataApiTestHelpers.serverStreaming(dataClient::readDataset, readRequest1, execContext);
        waitFor(TEST_TIMEOUT, readDataset1);
        var responseList1 = resultOf(readDataset1);

        // First response message should contain metadata only, with an empty buffer
        var response1 = responseList1.get(0);
        Assertions.assertEquals(ByteString.EMPTY, response1.getContent());

        // The remainder of the list should contain the file content
        var content1 = responseList1.stream().skip(1)
                .map(DataReadResponse::getContent)
                .reduce(ByteString.EMPTY, ByteString::concat);

        var responseData1 = DataApiTestHelpers.decodeCsv(response1.getSchema(), List.of(content1));

        assertDataEqualForRange(originalData, responseData1, 2, 2);
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
                .clearObjectCriteria())
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
                .clearTagCriteria())
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
