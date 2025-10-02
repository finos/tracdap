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

package org.finos.tracdap.svc.orch.jobs;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.data.ArrowVsrContext;
import org.finos.tracdap.common.data.SchemaMapping;
import org.finos.tracdap.common.data.util.Bytes;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.data.util.ByteOutputChannel;
import org.finos.tracdap.common.data.util.ByteSeekableChannel;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.metadata.ImportModelJob;
import org.finos.tracdap.metadata.RunModelJob;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.data.DataComparison;
import org.finos.tracdap.test.data.MemoryTestHelpers;
import org.finos.tracdap.test.data.SampleData;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import com.google.protobuf.ByteString;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Tag("integration")
@Tag("int-e2e")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DataRoundTripTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";

    // The data frameworks to test with a round trip
    private static final String[] DATA_FRAMEWORKS = {"pandas", "polars"};

    // Pandas / NumPy native dates and timestamps are encoded as 64-bit nanoseconds around the Unix epoch
    private static final LocalDateTime MIN_PANDAS_TIMESTAMP = LocalDateTime
            .ofEpochSecond(0, 0, ZoneOffset.UTC)
            .minusNanos(Long.MAX_VALUE);

    private static final LocalDateTime MAX_PANDAS_TIMESTAMP = LocalDateTime
            .ofEpochSecond(0, 0, ZoneOffset.UTC)
            .plusNanos(Long.MAX_VALUE);

    protected static PlatformTest platform;

    public static class CsvFormatTest extends DataRoundTripTest {
        @RegisterExtension private static final
        PlatformTest platformTest = DataRoundTripTest.createPlatformTest("CSV");
        static { platform = platformTest; }
    }

    public static class ArrowFormatTest extends DataRoundTripTest {
        @RegisterExtension private static final
        PlatformTest platformTest = DataRoundTripTest.createPlatformTest("ARROW_FILE");
        static { platform = platformTest; }
    }

    private static final Logger log = LoggerFactory.getLogger(DataRoundTripTest.class);

    private static PlatformTest createPlatformTest(String storageFormat) {

        return PlatformTest.forConfig(E2E_CONFIG, List.of(E2E_TENANTS))
                .runDbDeploy(true)
                .runCacheDeploy(true)
                .addTenant(TEST_TENANT)
                .storageFormat(storageFormat)
                .prepareLocalExecutor(true)
                .startService(TracMetadataService.class)
                .startService(TracDataService.class)
                .startService(TracOrchestratorService.class)
                .startService(TracAdminService.class)
                .build();
    }

    private static Stream<String> dataFrameworks() {
        return Stream.of(DATA_FRAMEWORKS);
    }

    static BufferAllocator ALLOCATOR;
    static TagHeader modelId;

    @BeforeAll
    static void setUp() throws Exception {

        ALLOCATOR = MemoryTestHelpers.testAllocator(false);
        importModel();
    }

    @AfterAll
    static void cleanUp() {

        // Any release / double free bugs in the test code will be reported as failures
        ALLOCATOR.close();
    }

    static void importModel() throws Exception {

        log.info("Running IMPORT_MODEL job...");

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var modelVersion = GitHelpers.getCurrentCommit();

        var importModel = ImportModelJob.newBuilder()
                .setLanguage("python")
                .setRepository("TRAC_LOCAL_REPO")
                .setPath("tracdap-services/tracdap-svc-orch/src/test/resources")
                .setEntryPoint("data_round_trip.DataRoundTripModel")
                .setVersion(modelVersion)
                .addModelAttrs(TagUpdate.newBuilder()
                        .setAttrName("data_round_trip_model")
                        .setValue(MetadataCodec.encodeValue(true)))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.IMPORT_MODEL)
                        .setImportModel(importModel))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName("round_trip_job")
                        .setValue(MetadataCodec.encodeValue("round_trip:import_model")))
                .build();

        var jobStatus = Helpers.runJob(orchClient, jobRequest);
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        var modelSearch = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName("trac_create_job")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(MetadataCodec.encodeValue(jobKey)))))
                .build();

        var modelSearchResult = metaClient.search(modelSearch);

        Assertions.assertEquals(1, modelSearchResult.getSearchResultCount());

        modelId = modelSearchResult.getSearchResult(0).getHeader();
    }

    @ParameterizedTest
    @MethodSource("dataFrameworks")
    @Execution(ExecutionMode.CONCURRENT)
    void basicData(String dataFramework) throws Exception {

        var data = SampleData.generateBasicData(ALLOCATOR);

        doRoundTrip(data, "basicData", dataFramework);
    }

    @ParameterizedTest
    @MethodSource("dataFrameworks")
    @Execution(ExecutionMode.CONCURRENT)
    void nullFirstRow(String dataFramework) throws Exception {

        var data = SampleData.generateBasicData(ALLOCATOR);

        // This is a quick way of nulling a value in an Arrow vector
        // Unsetting the first bit of the validity buffer makes the first value null
        for (var col = 0; col < data.getVsr().getSchema().getFields().size(); ++col) {

            var vector = data.getVsr().getVector(col);
            var validityMask0 = vector.getValidityBuffer().getByte(0);
            validityMask0 = (byte) (validityMask0 & (byte) 0xfe);

            vector.getValidityBuffer().setByte(0, validityMask0);
            Assertions.assertNull(vector.getObject(0));
        }

        doRoundTrip(data, "nullDataItems", dataFramework);
    }

    @ParameterizedTest
    @MethodSource("dataFrameworks")
    @Execution(ExecutionMode.CONCURRENT)
    void nullEntireTable(String dataFramework) throws Exception {

        var data = SampleData.generateBasicData(ALLOCATOR);

        // This is a quick way of nulling an entire vector, by setting the validity buffer to zero
        for (var col = 0; col < data.getVsr().getSchema().getFields().size(); ++col) {

            var vector = data.getVsr().getVector(0);
            vector.getValidityBuffer().setZero(0, vector.getValidityBuffer().capacity());
            Assertions.assertEquals(vector.getValueCount(), vector.getNullCount());
        }

        doRoundTrip(data, "nullDataItems", dataFramework);
    }

    @ParameterizedTest
    @MethodSource("dataFrameworks")
    @Execution(ExecutionMode.CONCURRENT)
    void emptyTable(String dataFramework) throws Exception {

        var schema = SchemaMapping.tracToArrow(SampleData.BASIC_TABLE_SCHEMA);
        var data = SampleData.convertData(schema, List.of(), ALLOCATOR);

        doRoundTrip(data, "emptyTable", dataFramework);
    }

    @ParameterizedTest
    @MethodSource("dataFrameworks")
    @Execution(ExecutionMode.CONCURRENT)
    void edgeCaseIntegers(String dataFramework) throws Exception {

        List<Object> edgeCases = List.of(0, Long.MIN_VALUE, Long.MAX_VALUE);

        doEdgeCaseTest("integer_field", edgeCases, dataFramework);
    }

    @ParameterizedTest
    @MethodSource("dataFrameworks")
    @Execution(ExecutionMode.CONCURRENT)
    void edgeCaseFloats(String dataFramework) throws Exception {

        // In Java, Double.NaN == Double.NaN is true, so NaN can be checked as a regular float edge case

        List<Object>  edgeCases = List.of(
                0.0,
                Double.MIN_VALUE, Double.MAX_VALUE,
                Double.MIN_NORMAL, -Double.MIN_NORMAL,
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Double.NaN);

        doEdgeCaseTest("float_field", edgeCases, dataFramework);
    }

    @ParameterizedTest
    @MethodSource("dataFrameworks")
    @Execution(ExecutionMode.CONCURRENT)
    void edgeCaseDecimals(String dataFramework) throws Exception {

        var d0 = BigDecimal.ZERO;
        var d1 = BigDecimal.TEN.pow(25);
        var d2 = BigDecimal.TEN.pow(25).negate();
        var d3 = new BigDecimal("0.000000000001");
        var d4 = new BigDecimal("-0.000000000001");

        Assertions.assertNotEquals(BigDecimal.ZERO, d3);
        Assertions.assertNotEquals(BigDecimal.ZERO, d4);

        List<Object>  edgeCases = Stream.of(d0, d1, d2, d3, d4)
                .map(d -> d.setScale(12, RoundingMode.UNNECESSARY))
                .collect(Collectors.toList());

        doEdgeCaseTest("decimal_field", edgeCases, dataFramework);
    }

    @ParameterizedTest
    @MethodSource("dataFrameworks")
    @Execution(ExecutionMode.CONCURRENT)
    void edgeCaseStrings(String dataFramework) throws Exception {

        List<Object>  edgeCases = List.of(
                "", " ", "  ", "\t", "\r\n", "  \r\n   ",
                "a, b\",", "'@@'", "[\"\"%^&", "£££", "#@",
                "Olá Mundo", "你好，世界", "Привет, мир", "नमस्ते दुनिया",
                "𝜌 = ∑ 𝑃𝜓 | 𝜓 ⟩ ⟨ 𝜓 |");

        doEdgeCaseTest("string_field", edgeCases, dataFramework);
    }

    @ParameterizedTest
    @MethodSource("dataFrameworks")
    @Execution(ExecutionMode.CONCURRENT)
    void edgeCaseDates(String dataFramework) throws Exception {

        List<Object> edgeCases = List.of(
                LocalDate.EPOCH,
                LocalDate.of(2000, 1, 1),
                LocalDate.of(2038, 1, 20),

                // Round-trip model is using Pandas-native timestamps by default
                MIN_PANDAS_TIMESTAMP.toLocalDate().plusDays(1),
                MAX_PANDAS_TIMESTAMP.toLocalDate()
        );

        doEdgeCaseTest("date_field", edgeCases, dataFramework);
    }

    @ParameterizedTest
    @MethodSource("dataFrameworks")
    @Execution(ExecutionMode.CONCURRENT)
    void edgeCaseDateTimes(String dataFramework) throws Exception {

        List<Object> edgeCases = List.of(
                LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0),
                LocalDateTime.of(2038, 1, 19, 3, 14, 8),

                // Fractional seconds before and after the epoch
                // Test fractions for both positive and negative encoded values
                LocalDateTime.of(1972, 1, 1, 0, 0, 0, 500000000),
                LocalDateTime.of(1968, 1, 1, 23, 59, 59, 500000000),

                // Round-trip model is using Pandas-native timestamps by default
                MIN_PANDAS_TIMESTAMP.plusSeconds(1),
                MAX_PANDAS_TIMESTAMP
        );

        doEdgeCaseTest("datetime_field", edgeCases, dataFramework);
    }

    void doEdgeCaseTest(String fieldName, List<Object> edgeCases, String dataFramework) throws Exception {

        var javaData = new ArrayList<Map<String, Object>>();

        for (int row = 0; row < edgeCases.size(); row++) {

            var record = new HashMap<String, Object>();

            for (var field : SampleData.BASIC_TABLE_SCHEMA.getTable().getFieldsList()) {

                if (field.getFieldName().equals(fieldName)) {
                    record.put(field.getFieldName(), edgeCases.get(row));
                }
                else {
                    var javaValue = SampleData.generateJavaPrimitive(field.getFieldType(), field.getCategorical(), row, null);
                    record.put(field.getFieldName(), javaValue);
                }
            }

            javaData.add(record);
        }

        var schema = SchemaMapping.tracToArrow(SampleData.BASIC_TABLE_SCHEMA);
        var data = SampleData.convertData(schema, javaData,  ALLOCATOR);

        doRoundTrip(data, "edgeCase:" + fieldName, dataFramework);
    }

    void doRoundTrip(ArrowVsrContext inputData, String testName, String dataFramework) throws Exception {

        ArrowVsrContext outputData = null;

        try {

            var inputDataId = saveInputData(inputData, testName);
            var outputDataId = runModel(inputDataId, testName, dataFramework);
            outputData = loadOutputData(outputDataId);

            DataComparison.compareSchemas(inputData.getSchema(), outputData.getSchema());
            DataComparison.compareBatches(inputData, outputData);
        }
        finally {

            inputData.close();

            if (outputData != null) {
                outputData.close();
            }
        }
    }

    TagHeader saveInputData(ArrowVsrContext inputData, String testName) throws Exception {

        var buf = new ArrayList<ArrowBuf>();

        try (var channel = new ByteOutputChannel(ALLOCATOR, buf::add);
             var writer = new ArrowFileWriter(inputData.getVsr(), inputData.getDictionaries(), channel)) {

            writer.start();
            writer.writeBatch();
            writer.end();
        }

        var bytes = Bytes.copyFromBuffer(buf);

        buf.forEach(ArrowBuf::close);
        buf.clear();

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(SampleData.BASIC_TABLE_SCHEMA)
                .setFormat("application/vnd.apache.arrow.file")
                .setContent(ByteString.copyFrom(bytes))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("round_trip_dataset")
                        .setValue(MetadataCodec.encodeValue(testName + ":input")))
                .build();

        var dataClient = platform.dataClientBlocking();


        return dataClient.createSmallDataset(writeRequest);
    }

    TagHeader runModel(TagHeader inputDataId, String testName, String dataFramework) {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var runModel = RunModelJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(modelId))
                .putParameters("data_framework", MetadataCodec.encodeValue(dataFramework))
                .putInputs("round_trip_input", MetadataUtil.selectorFor(inputDataId))
                .addOutputAttrs(TagUpdate.newBuilder()
                .setAttrName("round_trip_dataset")
                .setValue(MetadataCodec.encodeValue(testName + ":output")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setRunModel(runModel))
                .addJobAttrs(TagUpdate.newBuilder()
                .setAttrName("round_trip_job")
                .setValue(MetadataCodec.encodeValue(testName + ":run_model")))
                .build();

        var jobStatus = Helpers.runJob(orchClient, jobRequest);
        var jobKey = MetadataUtil.objectKey(jobStatus.getJobId());

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        var dataSearch = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName("trac_create_job")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(MetadataCodec.encodeValue(jobKey)))))
                .build();

        var dataSearchResult = metaClient.search(dataSearch);

        Assertions.assertEquals(1, dataSearchResult.getSearchResultCount());

        return dataSearchResult.getSearchResult(0).getHeader();
    }

    ArrowVsrContext loadOutputData(TagHeader outputDataId) throws Exception {

        var readRequest = DataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(outputDataId))
                .setFormat("application/vnd.apache.arrow.file")
                .build();

        var dataClient = platform.dataClientBlocking();
        var dataResponse = dataClient.readSmallDataset(readRequest);

        var allocator = new RootAllocator();
        var arrowBuf = allocator.buffer(dataResponse.getContent().size());
        arrowBuf.setBytes(0, dataResponse.getContent().asReadOnlyByteBuffer());
        arrowBuf.writerIndex(dataResponse.getContent().size());

        var reader = new ArrowFileReader(new ByteSeekableChannel(List.of(arrowBuf)), allocator);
        reader.loadNextBatch();

        var root = ArrowVsrContext.forSource(reader.getVectorSchemaRoot(), reader, allocator, reader);
        var arrowSchema = SchemaMapping.tracToArrow(dataResponse.getSchema());

        DataComparison.compareSchemas(arrowSchema, root.getSchema());

        return root;
    }
}
