/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.async.Flows;
import org.finos.tracdap.common.data.IExecutionContext;
import org.finos.tracdap.common.util.ResourceHelpers;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.data.DataApiTestHelpers;
import org.finos.tracdap.test.data.SampleData;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.finos.tracdap.common.metadata.MetadataUtil.selectorFor;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.resultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;


abstract class DataRoundTripTest {

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TRAC_TENANTS_UNIT = "config/trac-unit-tenants.yaml";
    public static final String TRAC_CONFIG_ENV_VAR = "TRAC_CONFIG_FILE";
    public static final String TRAC_TENANTS_ENV_VAR = "TRAC_TENANTS_FILE";
    public static final String TEST_TENANT = "ACME_CORP";

    protected static EventLoopGroup elg;
    protected static IExecutionContext execContext;
    protected static TracDataApiGrpc.TracDataApiStub dataClient;

    // Include this test case as a unit test
    static class UnitTest extends DataRoundTripTest {

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT, List.of(TRAC_TENANTS_UNIT))
                .runDbDeploy(true)
                .addTenant(TEST_TENANT)
                .startService(TracMetadataService.class)
                .startService(TracDataService.class)
                .startService(TracAdminService.class)
                .build();

        @BeforeAll
        static void setupClass() {
            elg = new NioEventLoopGroup(2);
            dataClient = platform.dataClient();
        }

        @BeforeEach
        void setup() {
            execContext = new DataContext(elg.next(), new RootAllocator());
        }
    }

    // Include this test case for integration against different storage backends
    @Tag("integration")
    @Tag("int-storage")
    @Tag("all-platforms")
    static class IntegrationTest extends DataRoundTripTest {

        private static final String TRAC_CONFIG_FILE = System.getenv(TRAC_CONFIG_ENV_VAR);
        private static final String TRAC_TENANTS_FILE = System.getenv(TRAC_TENANTS_ENV_VAR);

        @RegisterExtension
        public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_FILE, List.of(TRAC_TENANTS_FILE))
                .runDbDeploy(true)
                .addTenant(TEST_TENANT)
                .manageDataPrefix(true)
                .startService(TracAdminService.class)
                .startService(TracMetadataService.class)
                .startService(TracDataService.class)
                .build();

        @BeforeAll
        static void setupClass() {
            elg = new NioEventLoopGroup(2);
            dataClient = platform.dataClient();
        }

        @AfterAll
        static void tearDownClass() {
            elg.shutdownGracefully();
        }

        @BeforeEach
        void setup() {
            execContext = new DataContext(elg.next(), new RootAllocator());
        }
    }


    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(20);

    private static final String BASIC_CSV_DATA = SampleData.BASIC_CSV_DATA_RESOURCE;
    private static final String BASIC_JSON_DATA = SampleData.BASIC_JSON_DATA_RESOURCE;
    private static final String STRUCT_JSON_DATA = SampleData.STRUCT_JSON_DATA_RESOURCE;

    private static final String LARGE_CSV_DATA = "/large_csv_data_100000.csv";
    private static final String LARGE_JSON_DATA = "/large_json_data_100000.json";
    private static final long LARGE_DATA_ROW_COUNT = 100000;

    private static final byte[] BASIC_CSV_CONTENT = ResourceHelpers.loadResourceAsBytes(BASIC_CSV_DATA);

    private static final List<Vector<Object>> BASIC_TEST_DATA = DataApiTestHelpers.decodeCsv(
            SampleData.BASIC_TABLE_SCHEMA,
            List.of(ByteString.copyFrom(BASIC_CSV_CONTENT)));


    @Test
    void roundTrip_arrowStream() throws Exception {

        // Create a single batch of Arrow data

        var allocator = new RootAllocator();
        var root = SampleData.generateBasicData(allocator);

        // Use a writer to encode the batch as a stream of chunks (arrow record batches, including the schema)

        var writeChannel = new ChunkChannel();

        // Keep the writer open until after the test is complete
        // Closing the writer will close the VSR, which releases the underlying memory
        try (var writer = new ArrowStreamWriter(root.getFrontBuffer(), root.getDictionaries(), writeChannel)) {

            writer.start();
            writer.writeBatch();
            writer.end();

            var mimeType = "application/vnd.apache.arrow.stream";
            roundTripTest(writeChannel.getChunks(), mimeType, mimeType, DataApiTestHelpers::decodeArrowStream, true);
            roundTripTest(writeChannel.getChunks(), mimeType, mimeType, DataApiTestHelpers::decodeArrowStream, false);
        }
    }

    @Test
    void roundTrip_arrowFile() throws Exception {

        // Create a single batch of Arrow data

        var allocator = new RootAllocator();
        var root = SampleData.generateBasicData(allocator);

        // Use a writer to encode the batch as a stream of chunks (arrow record batches, including the schema)

        var writeChannel = new ChunkChannel();

        // Keep the writer open until after the test is complete
        // Closing the writer will close the VSR, which releases the underlying memory
        try (var writer = new ArrowFileWriter(root.getFrontBuffer(), root.getDictionaries(), writeChannel)) {

            writer.start();
            writer.writeBatch();
            writer.end();

            var mimeType = "application/vnd.apache.arrow.file";
            roundTripTest(writeChannel.getChunks(), mimeType, mimeType, DataApiTestHelpers::decodeArrowFile, true);
            roundTripTest(writeChannel.getChunks(), mimeType, mimeType, DataApiTestHelpers::decodeArrowFile, false);
        }
    }

    @Test
    void roundTrip_csv() throws Exception {

        try (var testDataStream = getClass().getResourceAsStream(BASIC_CSV_DATA)) {

            if (testDataStream == null)
                throw new RuntimeException("Test data not found");

            var testDataBytes = testDataStream.readAllBytes();
            var testData = List.of(ByteString.copyFrom(testDataBytes));

            var mimeType = "text/csv";
            roundTripTest(testData, mimeType, mimeType, DataApiTestHelpers::decodeCsv, true);
            roundTripTest(testData, mimeType, mimeType, DataApiTestHelpers::decodeCsv, false);
        }
    }

    @Test
    void roundTrip_csvLarge() throws Exception {

        try (var testDataStream = getClass().getResourceAsStream(LARGE_CSV_DATA)) {

            if (testDataStream == null)
                throw new RuntimeException("Test data not found");

            var testDataBytes = testDataStream.readAllBytes();
            var testData = new ArrayList<ByteString>();
            var mb2 = 2 * 1024 * 1024;

            for (var offset = 0; offset < testDataBytes.length; offset += mb2) {

                var sliceSize = Math.min(mb2, testDataBytes.length - offset);
                var slice = ByteString.copyFrom(testDataBytes, offset, sliceSize);
                testData.add(slice);
            }

            var mimeType = "text/csv";


            var requestParams = DataWriteRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSchema(SampleData.BASIC_TABLE_SCHEMA)
                    .setFormat(mimeType)
                    .build();

            var createDatasetRequest = dataWriteRequest(requestParams, testData, false);
            var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, createDatasetRequest);

            waitFor(TEST_TIMEOUT, createDataset);
            var objHeader = resultOf(createDataset);

            var dataRequest = DataReadRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSelector(selectorFor(objHeader))
                    .setFormat(mimeType)
                    .build();

            var readResponse = Flows.<DataReadResponse>hub(execContext.eventLoopExecutor());
            var readResponse0 = Flows.first(readResponse);
            var readByteStream = Flows.map(readResponse, DataReadResponse::getContent);
            var readBytes = Flows.fold(readByteStream, ByteString::concat, ByteString.EMPTY);

            DataApiTestHelpers.serverStreaming(dataClient::readDataset, dataRequest, readResponse);

            waitFor(Duration.ofMinutes(20), readResponse0, readBytes);
            var roundTripResponse = resultOf(readResponse0);
            var roundTripSchema = roundTripResponse.getSchema();
            var roundTripBytes = resultOf(readBytes);

            var roundTripData = DataApiTestHelpers.decodeCsv(roundTripSchema, List.of(roundTripBytes));
            var integerField = roundTripData.get(1);
            var stringField = roundTripData.get(4);

            // Check the right number of rows came through the round tripe
            Assertions.assertEquals(LARGE_DATA_ROW_COUNT, integerField.size());

            // Just testing the int and string fields - exhaustive encoding checks in -lib-data
            for (var i = 0; i < integerField.size(); i++) {
                Assertions.assertEquals((long) i % 10000 + 1, integerField.get(i));
                Assertions.assertEquals("hello " + ((long) i % 10000 + 1), stringField.get(i));
            }
        }
    }

    @Test
    void roundTrip_csvRandomChunks() throws Exception {

        try (var testDataStream = getClass().getResourceAsStream(LARGE_CSV_DATA)) {

            if (testDataStream == null)
                throw new RuntimeException("Test data not found");

            var testDataBytes = testDataStream.readAllBytes();
            var testData = new ArrayList<ByteString>();

            var offset = 0;
            var random = new Random(Instant.now().getEpochSecond());

            while (offset < testDataBytes.length) {

                var randomSliceSize = random.nextInt(1, 512 * 1024);
                var sliceSize = Math.min(randomSliceSize, testDataBytes.length - offset);
                var slice = ByteString.copyFrom(testDataBytes, offset, sliceSize);
                testData.add(slice);

                offset += sliceSize;
            }

            var mimeType = "text/csv";


            var requestParams = DataWriteRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSchema(SampleData.BASIC_TABLE_SCHEMA)
                    .setFormat(mimeType)
                    .build();

            var createDatasetRequest = dataWriteRequest(requestParams, testData, false);
            var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, createDatasetRequest);

            waitFor(TEST_TIMEOUT, createDataset);
            var objHeader = resultOf(createDataset);

            var dataRequest = DataReadRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSelector(selectorFor(objHeader))
                    .setFormat(mimeType)
                    .build();

            var readResponse = Flows.<DataReadResponse>hub(execContext.eventLoopExecutor());
            var readResponse0 = Flows.first(readResponse);
            var readByteStream = Flows.map(readResponse, DataReadResponse::getContent);
            var readBytes = Flows.fold(readByteStream, ByteString::concat, ByteString.EMPTY);

            DataApiTestHelpers.serverStreaming(dataClient::readDataset, dataRequest, readResponse);

            waitFor(Duration.ofMinutes(20), readResponse0, readBytes);
            var roundTripResponse = resultOf(readResponse0);
            var roundTripSchema = roundTripResponse.getSchema();
            var roundTripBytes = resultOf(readBytes);

            var roundTripData = DataApiTestHelpers.decodeCsv(roundTripSchema, List.of(roundTripBytes));
            var integerField = roundTripData.get(1);
            var stringField = roundTripData.get(4);

            // Check the right number of rows came through the round tripe
            Assertions.assertEquals(LARGE_DATA_ROW_COUNT, integerField.size());

            // Just testing the int and string fields - exhaustive encoding checks in -lib-data
            for (var i = 0; i < integerField.size(); i++) {
                Assertions.assertEquals((long) i % 10000 + 1, integerField.get(i));
                Assertions.assertEquals("hello " + ((long) i % 10000 + 1), stringField.get(i));
            }
        }
    }

    @Test
    void roundTrip_json() throws Exception {

        try (var testDataStream = getClass().getResourceAsStream(BASIC_JSON_DATA)) {

            if (testDataStream == null)
                throw new RuntimeException("Test data not found");

            var testDataBytes = testDataStream.readAllBytes();
            var testData = List.of(ByteString.copyFrom(testDataBytes));

            var mimeType = "application/json";
            roundTripTest(testData, mimeType, mimeType, DataApiTestHelpers::decodeJson, true);
            roundTripTest(testData, mimeType, mimeType, DataApiTestHelpers::decodeJson, false);
        }
    }

    @Test
    void roundTrip_jsonLarge() throws Exception {

        try (var testDataStream = getClass().getResourceAsStream(LARGE_JSON_DATA)) {

            if (testDataStream == null)
                throw new RuntimeException("Test data not found");

            var testDataBytes = testDataStream.readAllBytes();
            var testData = new ArrayList<ByteString>();
            var mb2 = 2 * 1024 * 1024;

            for (var offset = 0; offset < testDataBytes.length; offset += mb2) {

                var sliceSize = Math.min(mb2, testDataBytes.length - offset);
                var slice = ByteString.copyFrom(testDataBytes, offset, sliceSize);
                testData.add(slice);
            }

            var mimeType = "application/json";


            var requestParams = DataWriteRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSchema(SampleData.BASIC_TABLE_SCHEMA)
                    .setFormat(mimeType)
                    .build();

            var createDatasetRequest = dataWriteRequest(requestParams, testData, false);
            var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, createDatasetRequest);

            waitFor(TEST_TIMEOUT, createDataset);
            var objHeader = resultOf(createDataset);

            var dataRequest = DataReadRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSelector(selectorFor(objHeader))
                    .setFormat(mimeType)
                    .build();

            var readResponse = Flows.<DataReadResponse>hub(execContext.eventLoopExecutor());
            var readResponse0 = Flows.first(readResponse);
            var readByteStream = Flows.map(readResponse, DataReadResponse::getContent);
            var readBytes = Flows.fold(readByteStream, ByteString::concat, ByteString.EMPTY);

            DataApiTestHelpers.serverStreaming(dataClient::readDataset, dataRequest, readResponse);

            waitFor(Duration.ofMinutes(20), readResponse0, readBytes);
            var roundTripResponse = resultOf(readResponse0);
            var roundTripSchema = roundTripResponse.getSchema();
            var roundTripBytes = resultOf(readBytes);

            var roundTripData = DataApiTestHelpers.decodeJson(roundTripSchema, List.of(roundTripBytes));
            var integerField = roundTripData.get(1);
            var stringField = roundTripData.get(4);

            // Check the right number of rows came through the round tripe
            Assertions.assertEquals(LARGE_DATA_ROW_COUNT, integerField.size());

            // Just testing the int and string fields - exhaustive encoding checks in -lib-data
            for (var i = 0; i < integerField.size(); i++) {
                Assertions.assertEquals((long) i % 10000 + 1, integerField.get(i));
                Assertions.assertEquals("hello " + ((long) i % 10000 + 1), stringField.get(i));
            }
        }
    }

    @Test
    void roundTrip_jsonRandomChunks() throws Exception {

        try (var testDataStream = getClass().getResourceAsStream(LARGE_JSON_DATA)) {

            if (testDataStream == null)
                throw new RuntimeException("Test data not found");

            var testDataBytes = testDataStream.readAllBytes();
            var testData = new ArrayList<ByteString>();

            var offset = 0;
            var random = new Random(Instant.now().getEpochSecond());

            while (offset < testDataBytes.length) {

                var randomSliceSize = random.nextInt(1, 512 * 1024);
                var sliceSize = Math.min(randomSliceSize, testDataBytes.length - offset);
                var slice = ByteString.copyFrom(testDataBytes, offset, sliceSize);
                testData.add(slice);

                offset += sliceSize;
            }

            var mimeType = "application/json";


            var requestParams = DataWriteRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSchema(SampleData.BASIC_TABLE_SCHEMA)
                    .setFormat(mimeType)
                    .build();

            var createDatasetRequest = dataWriteRequest(requestParams, testData, false);
            var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, createDatasetRequest);

            waitFor(TEST_TIMEOUT, createDataset);
            var objHeader = resultOf(createDataset);

            var dataRequest = DataReadRequest.newBuilder()
                    .setTenant(TEST_TENANT)
                    .setSelector(selectorFor(objHeader))
                    .setFormat(mimeType)
                    .build();

            var readResponse = Flows.<DataReadResponse>hub(execContext.eventLoopExecutor());
            var readResponse0 = Flows.first(readResponse);
            var readByteStream = Flows.map(readResponse, DataReadResponse::getContent);
            var readBytes = Flows.fold(readByteStream, ByteString::concat, ByteString.EMPTY);

            DataApiTestHelpers.serverStreaming(dataClient::readDataset, dataRequest, readResponse);

            waitFor(Duration.ofMinutes(20), readResponse0, readBytes);
            var roundTripResponse = resultOf(readResponse0);
            var roundTripSchema = roundTripResponse.getSchema();
            var roundTripBytes = resultOf(readBytes);

            var roundTripData = DataApiTestHelpers.decodeJson(roundTripSchema, List.of(roundTripBytes));
            var integerField = roundTripData.get(1);
            var stringField = roundTripData.get(4);

            // Check the right number of rows came through the round tripe
            Assertions.assertEquals(LARGE_DATA_ROW_COUNT, integerField.size());

            // Just testing the int and string fields - exhaustive encoding checks in -lib-data
            for (var i = 0; i < integerField.size(); i++) {
                Assertions.assertEquals((long) i % 10000 + 1, integerField.get(i));
                Assertions.assertEquals("hello " + ((long) i % 10000 + 1), stringField.get(i));
            }
        }
    }

    @Test
    void roundTrip_jsonStruct() throws Exception {

        try (var testDataStream = getClass().getResourceAsStream(STRUCT_JSON_DATA)) {

            if (testDataStream == null)
                throw new RuntimeException("Test data not found");

            var testDataBytes = testDataStream.readAllBytes();
            var testData = List.of(ByteString.copyFrom(testDataBytes));

            var jsonComparison = (BiConsumer<byte[], byte[]>) (expected, actual) -> {

                try {

                    var mapper = new ObjectMapper();
                    var expectedTree = mapper.readTree(new String(expected, StandardCharsets.UTF_8));
                    var roundTripTree = mapper.readTree(new String(actual, StandardCharsets.UTF_8));

                    Assertions.assertEquals(expectedTree, roundTripTree);
                }
                catch (Exception e) {

                    Assertions.fail(e);
                }
            };

            var mimeType = "application/json";
            roundTripComparison(testData, mimeType, mimeType, jsonComparison, true);
            roundTripComparison(testData, mimeType, mimeType, jsonComparison, false);
        }
    }

    private void roundTripTest(
            List<ByteString> content, String writeFormat, String readFormat,
            BiFunction<SchemaDefinition, List<ByteString>, List<Vector<Object>>> decodeFunc,
            boolean dataInChunkZero) throws Exception {

        var requestParams = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(SampleData.BASIC_TABLE_SCHEMA)
                .setFormat(writeFormat)
                .build();

        var createDatasetRequest = dataWriteRequest(requestParams, content, dataInChunkZero);
        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, createDatasetRequest);

        waitFor(TEST_TIMEOUT, createDataset);
        var objHeader = resultOf(createDataset);

        var dataRequest = DataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selectorFor(objHeader))
                .setFormat(readFormat)
                .build();

        var readResponse = Flows.<DataReadResponse>hub(execContext.eventLoopExecutor());
        var readResponse0 = Flows.first(readResponse);
        var readByteStream = Flows.map(readResponse, DataReadResponse::getContent);
        var readBytes = Flows.fold(readByteStream, ByteString::concat, ByteString.EMPTY);

        DataApiTestHelpers.serverStreaming(dataClient::readDataset, dataRequest, readResponse);

        waitFor(Duration.ofMinutes(20), readResponse0, readBytes);
        var roundTripResponse = resultOf(readResponse0);
        var roundTripSchema = roundTripResponse.getSchema();
        var roundTripBytes = resultOf(readBytes);

        var roundTripData = decodeFunc.apply(roundTripSchema, List.of(roundTripBytes));

        Assertions.assertEquals(SampleData.BASIC_TABLE_SCHEMA, roundTripSchema);

        for (int col = 0; col < roundTripSchema.getTable().getFieldsCount(); col++) {

            for (var row = 0; row < DataRoundTripTest.BASIC_TEST_DATA.size(); row++) {

                var expectedVal = DataRoundTripTest.BASIC_TEST_DATA.get(col).get(row);
                var roundTripVal = roundTripData.get(col).get(row);

                // Allow comparing big decimals with different scales
                if (expectedVal instanceof BigDecimal)
                    roundTripVal = ((BigDecimal) roundTripVal).setScale(((BigDecimal) expectedVal).scale(), RoundingMode.UNNECESSARY);

                Assertions.assertEquals(expectedVal, roundTripVal);
            }
        }
    }

    private void roundTripComparison(
            List<ByteString> content, String writeFormat, String readFormat,
            BiConsumer<byte[], byte[]> comparison,
            boolean dataInChunkZero) throws Exception {

        var requestParams = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(SampleData.BASIC_STRUCT_SCHEMA)
                .setFormat(writeFormat)
                .build();

        var createDatasetRequest = dataWriteRequest(requestParams, content, dataInChunkZero);
        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, createDatasetRequest);

        waitFor(TEST_TIMEOUT, createDataset);
        var objHeader = resultOf(createDataset);

        var dataRequest = DataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selectorFor(objHeader))
                .setFormat(readFormat)
                .build();

        var readResponse = Flows.<DataReadResponse>hub(execContext.eventLoopExecutor());
        var readResponse0 = Flows.first(readResponse);
        var readByteStream = Flows.map(readResponse, DataReadResponse::getContent);
        var readBytes = Flows.fold(readByteStream, ByteString::concat, ByteString.EMPTY);

        DataApiTestHelpers.serverStreaming(dataClient::readDataset, dataRequest, readResponse);

        waitFor(Duration.ofMinutes(20), readResponse0, readBytes);
        var roundTripBytes = resultOf(readBytes);

        var originalBytes = ByteString.empty();
        for (var chunk : content) {
            originalBytes = originalBytes.concat(chunk);
        }

        comparison.accept(originalBytes.toByteArray(), roundTripBytes.toByteArray());
    }

    private Flow.Publisher<DataWriteRequest> dataWriteRequest(
            DataWriteRequest requestParams,
            List<ByteString> content,
            boolean dataInChunkZero) {

        var chunkZeroBytes = dataInChunkZero
                ? content.get(0)
                : ByteString.EMPTY;

        var requestZero = requestParams.toBuilder()
                .setContent(chunkZeroBytes)
                .build();

        var remainingContent = dataInChunkZero
                ? content.subList(1, content.size())
                : content;

        var requestStream = remainingContent.stream().map(bytes ->
                DataWriteRequest.newBuilder()
                .setContent(bytes)
                .build());

        return Flows.publish(Streams.concat(
                Stream.of(requestZero),
                requestStream));
    }

    private static class ChunkChannel implements WritableByteChannel {

        private final List<ByteString> chunks = new ArrayList<>();
        private boolean isOpen = true;

        public List<ByteString> getChunks() {
            return chunks;
        }

        @Override
        public int write(ByteBuffer chunk) {

            var copied = ByteString.copyFrom(chunk);
            chunks.add(copied);
            return copied.size();
        }

        @Override public boolean isOpen() { return isOpen; }
        @Override public void close() { isOpen = false; }
    }
}
