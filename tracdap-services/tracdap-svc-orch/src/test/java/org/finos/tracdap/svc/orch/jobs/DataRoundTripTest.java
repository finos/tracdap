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

package org.finos.tracdap.svc.orch.jobs;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.data.ArrowSchema;
import org.finos.tracdap.common.data.util.Bytes;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.data.util.ByteOutputChannel;
import org.finos.tracdap.common.data.util.ByteSeekableChannel;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.metadata.ImportModelJob;
import org.finos.tracdap.metadata.RunModelJob;
import org.finos.tracdap.test.data.SampleData;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Schema;
import com.google.protobuf.ByteString;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
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

import static org.finos.tracdap.svc.orch.jobs.Helpers.runJob;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DataRoundTripTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";

    // Pandas / NumPy native dates and timestamps are encoded as 64-bit nanoseconds around the Unix epoch
    private static final LocalDateTime MIN_PANDAS_TIMESTAMP = LocalDateTime
            .ofEpochSecond(0, 0, ZoneOffset.UTC)
            .minusNanos(Long.MAX_VALUE);

    private static final LocalDateTime MAX_PANDAS_TIMESTAMP = LocalDateTime
            .ofEpochSecond(0, 0, ZoneOffset.UTC)
            .plusNanos(Long.MAX_VALUE);

    // Python native min/max dates and timestamps are for the years 1 and 9999 CE
    private static final LocalDateTime MIN_PYTHON_TIMESTAMP = LocalDateTime.of(1, 1, 1, 0, 0, 0);
    private static final LocalDateTime MAX_PYTHON_TIMESTAMP = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000);


    protected abstract String storageFormat();

    public static class CsvFormatTest extends DataRoundTripTest {
        protected String storageFormat() { return "CSV"; }
    }

    public static class ArrowFormatTest extends DataRoundTripTest {
        protected String storageFormat() { return "ARROW_FILE"; }
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    @RegisterExtension
    private final PlatformTest platform = PlatformTest.forConfig(E2E_CONFIG)
            .addTenant(TEST_TENANT)
            .storageFormat(storageFormat())
            .startAll()
            .build();

    static BufferAllocator ALLOCATOR;
    static TagHeader modelId;

    @BeforeAll
    static void setUp() {

        ALLOCATOR = new RootAllocator();
    }

    @AfterAll
    static void cleanUp() {

        ALLOCATOR.close();
    }

    @Test @Order(1)
    void importModel() throws Exception {

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

        var jobStatus = runJob(orchClient, jobRequest);
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

    @Test
    void basicData() throws Exception {

        var schema = ArrowSchema.tracToArrow(SampleData.BASIC_TABLE_SCHEMA);
        var data = SampleData.generateBasicData(ALLOCATOR);

        doRoundTrip(schema, data, "basicData");
    }

    @Test
    void nullFirstRow() throws Exception {

        var schema = ArrowSchema.tracToArrow(SampleData.BASIC_TABLE_SCHEMA);
        var data = SampleData.generateBasicData(ALLOCATOR);

        // This is a quick way of nulling a value in an Arrow vector
        // Unsetting the first bit of the validity buffer makes the first value null
        for (var col = 0; col < data.getSchema().getFields().size(); ++col) {

            var vector = data.getVector(col);
            var validityMask0 = vector.getValidityBuffer().getByte(0);
            validityMask0 = (byte) (validityMask0 & (byte) 0xfe);

            vector.getValidityBuffer().setByte(0, validityMask0);
            Assertions.assertNull(vector.getObject(0));
        }

        doRoundTrip(schema, data, "nullDataItems");
    }

    @Test
    void nullEntireTable() throws Exception {

        var schema = ArrowSchema.tracToArrow(SampleData.BASIC_TABLE_SCHEMA);
        var data = SampleData.generateBasicData(ALLOCATOR);

        // This is a quick way of nulling an entire vector, by setting the validity buffer to zero
        for (var col = 0; col < data.getSchema().getFields().size(); ++col) {

            var vector = data.getVector(0);
            vector.getValidityBuffer().setZero(0, vector.getValidityBuffer().capacity());
            Assertions.assertEquals(vector.getValueCount(), vector.getNullCount());
        }

        doRoundTrip(schema, data, "nullDataItems");
    }

    @Test
    void emptyTable() throws Exception {

        var schema = ArrowSchema.tracToArrow(SampleData.BASIC_TABLE_SCHEMA);
        var data = SampleData.convertData(schema, Map.of(), 0, ALLOCATOR);

        doRoundTrip(schema, data, "emptyTable");
    }

    @Test
    void edgeCaseIntegers() throws Exception {

        List<Object> edgeCases = List.of(0, Long.MIN_VALUE, Long.MAX_VALUE);

        doEdgeCaseTest("integer_field", edgeCases);
    }

    @Test
    void edgeCaseFloats() throws Exception {

        // In Java, Double.NaN == Double.NaN is true, so NaN can be checked as a regular float edge case

        List<Object>  edgeCases = List.of(
                0.0,
                Double.MIN_VALUE, Double.MAX_VALUE,
                Double.MIN_NORMAL, -Double.MIN_NORMAL,
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Double.NaN);

        doEdgeCaseTest("float_field", edgeCases);
    }

    @Test
    void edgeCaseDecimals() throws Exception {

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

        doEdgeCaseTest("decimal_field", edgeCases);
    }

    @Test
    void edgeCaseStrings() throws Exception {

        List<Object>  edgeCases = List.of(
                "", " ", "  ", "\t", "\r\n", "  \r\n   ",
                "a, b\",", "'@@'", "[\"\"%^&", "£££", "#@",
                "Olá Mundo", "你好，世界", "Привет, мир", "नमस्ते दुनिया",
                "𝜌 = ∑ 𝑃𝜓 | 𝜓 ⟩ ⟨ 𝜓 |");

        doEdgeCaseTest("string_field", edgeCases);
    }

    @Test
    void edgeCaseDates() throws Exception {

        List<Object> edgeCases = List.of(
                LocalDate.EPOCH,
                LocalDate.of(2000, 1, 1),
                LocalDate.of(2038, 1, 20),

                // Round-trip model is using Pandas-native timestamps by default
                MIN_PANDAS_TIMESTAMP.toLocalDate().plusDays(1),
                MAX_PANDAS_TIMESTAMP.toLocalDate()
        );

        doEdgeCaseTest("date_field", edgeCases);
    }

    @Test
    void edgeCaseDateTimes() throws Exception {

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

        doEdgeCaseTest("datetime_field", edgeCases);
    }

    void doEdgeCaseTest(String fieldName, List<Object> edgeCases) throws Exception {

        var javaData = new HashMap<String, List<Object>>();

        for (var field : SampleData.BASIC_TABLE_SCHEMA.getTable().getFieldsList()) {

            if (field.getFieldName().equals(fieldName)) {
                javaData.put(field.getFieldName(), edgeCases);
            }
            else {
                var javaValues = SampleData.generateJavaValues(field.getFieldType(), edgeCases.size());
                javaData.put(field.getFieldName(), javaValues);
            }
        }

        var schema = ArrowSchema.tracToArrow(SampleData.BASIC_TABLE_SCHEMA);
        var data = SampleData.convertData(schema, javaData, edgeCases.size(), ALLOCATOR);

        doRoundTrip(schema, data, "edgeCase:" + fieldName);
    }

    void doRoundTrip(Schema schema, VectorSchemaRoot inputData, String testName) throws Exception {

        VectorSchemaRoot outputData = null;

        try {

            var inputDataId = saveInputData(schema, inputData, testName);
            var outputDataId = runModel(inputDataId, testName);
            outputData = loadOutputData(outputDataId);

            Assertions.assertEquals(inputData.getSchema(), outputData.getSchema());
            Assertions.assertEquals(inputData.getRowCount(), outputData.getRowCount());

            for (var field : outputData.getSchema().getFields()) {

                for (var i = 0; i < outputData.getRowCount(); i++) {

                    var original = inputData.getVector(field).getObject(i);
                    var rt = outputData.getVector(field).getObject(i);

                    Assertions.assertEquals(
                            original, rt,
                            String.format("Difference in column [%s] row [%d]", field.getName(), i));
                }
            }
        }
        finally {

            inputData.close();

            if (outputData != null) {
                outputData.close();
            }
        }
    }

    TagHeader saveInputData(Schema schema, VectorSchemaRoot data, String testName) throws Exception {

        var buf = new ArrayList<ArrowBuf>();

        try (var channel = new ByteOutputChannel(ALLOCATOR, buf::add);
             var writer = new ArrowFileWriter(data, null,  channel)) {

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

    TagHeader runModel(TagHeader inputDataId, String testName) {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var runModel = RunModelJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(modelId))
                .putParameters("use_spark", MetadataCodec.encodeValue(false))
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

        var jobStatus = runJob(orchClient, jobRequest);
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

    VectorSchemaRoot loadOutputData(TagHeader outputDataId) throws Exception {

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

        var root = reader.getVectorSchemaRoot();

        var arrowSchema = ArrowSchema.tracToArrow(dataResponse.getSchema());
        Assertions.assertEquals(arrowSchema, root.getSchema());

        return root;
    }
}
