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

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.codec.arrow.ArrowFileCodec;
import org.finos.tracdap.common.codec.arrow.ArrowSchema;
import org.finos.tracdap.common.concurrent.Flows;
import org.finos.tracdap.common.data.DataBlock;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.metadata.ImportModelJob;
import org.finos.tracdap.metadata.RunModelJob;
import org.finos.tracdap.test.data.SampleData;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.finos.tracdap.svc.orch.jobs.Helpers.runJob;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.resultOf;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DataRoundTripTest {

    protected abstract String storageFormat();

    public static class CsvFormatTest extends DataRoundTripTest {
        protected String storageFormat() { return "CSV"; }
    }

    public static class ArrowFormatTest extends DataRoundTripTest {
        protected String storageFormat() { return "ARROW_FILE"; }
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";

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
                .setRepository("UNIT_TEST_REPO")
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
    void nullDataItems() throws Exception {

        var schema = ArrowSchema.tracToArrow(SampleData.BASIC_TABLE_SCHEMA);
        var data = SampleData.generateBasicData(ALLOCATOR);

        // This is a quick way of masking a value in an Arrow vector
        // Zeroing byte zero of the validity buffer will set the first 8 values to null (validity is a bit map)
        for (var col = 0; col < data.getSchema().getFields().size(); ++col) {

            var vector = data.getVector(col);
            vector.getValidityBuffer().setZero(0, 1);
            Assertions.assertNull(vector.getObject(0));
        }

        doRoundTrip(schema, data, "nullDataItems");
    }

    @Test
    void emptyTable() throws Exception {

        var schema = ArrowSchema.tracToArrow(SampleData.BASIC_TABLE_SCHEMA);
        var data = SampleData.generateDataFor(schema, Map.of(), 0, ALLOCATOR);

        doRoundTrip(schema, data, "emptyTable");
    }

    @Test
    void edgeCaseIntegers() throws Exception {

        List<Object> edgeCases = List.of(0, Long.MIN_VALUE, Long.MAX_VALUE);

        doEdgeCaseTest("integer_field", edgeCases);
    }

    @Test
    void edgeCaseFloats() throws Exception {

        List<Object>  edgeCases = List.of(
                0,
                Double.MIN_VALUE, Double.MAX_VALUE,
                Double.MIN_NORMAL, -Double.MIN_NORMAL,
                Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);

        doEdgeCaseTest("float_field", edgeCases);
    }

    void doEdgeCaseTest(String fieldName, List<Object> edgeCases) throws Exception {

        var edgeCaseMap = Map.of(fieldName, edgeCases);

        var schema = ArrowSchema.tracToArrow(SampleData.BASIC_TABLE_SCHEMA);
        var data = SampleData.generateDataFor(schema, edgeCaseMap, edgeCases.size(), ALLOCATOR);

        doRoundTrip(schema, data, "edgeCaseIntegers");
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

        var unloader = new VectorUnloader(data);
        var batch = unloader.getRecordBatch();
        var schemaBlock = DataBlock.forSchema(schema);
        var dataBlock = DataBlock.forRecords(batch);
        var blockStream = Flows.publish(Stream.of(schemaBlock, dataBlock));

        var codec = new ArrowFileCodec();
        var encoder = codec.getEncoder(ALLOCATOR, SampleData.BASIC_TABLE_SCHEMA, Map.of());

        blockStream.subscribe(encoder);
        var opaqueData_ = Flows.fold(encoder, (bs, b) -> bs.addComponent(true, b), ByteBufAllocator.DEFAULT.compositeBuffer());
        var opaqueData = resultOf(opaqueData_);

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(SampleData.BASIC_TABLE_SCHEMA)
                .setFormat("application/vnd.apache.arrow.file")
                .setContent(ByteString.copyFrom(opaqueData.nioBuffer()))
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
        var dataBuf = Unpooled.wrappedBuffer(dataResponse.getContent().asReadOnlyByteBuffer());
        var dataStream = Flows.publish(Stream.of(dataBuf));

        var codec = new ArrowFileCodec();
        var allocator = new RootAllocator();
        var decoder = codec.getDecoder(allocator, SampleData.BASIC_TABLE_SCHEMA, Map.of());

        dataStream.subscribe(decoder);
        var blocks = resultOf(Flows.toList(decoder));

        Assertions.assertTrue(blocks.size() == 1 || blocks.size() == 2);
        Assertions.assertNotNull(blocks.get(0).arrowSchema);

        if (blocks.size() == 1) {
            var fields = blocks.get(0).arrowSchema.getFields();
            var emptyVectors = fields.stream()
                    .map(f -> f.createVector(ALLOCATOR))
                    .collect(Collectors.toList());
            return new VectorSchemaRoot(fields, emptyVectors, 0);
        }

        var arrowSchema = blocks.get(0).arrowSchema;
        var arrowRecords = blocks.get(1).arrowRecords;
        var root = VectorSchemaRoot.create(arrowSchema, allocator);
        var loader = new VectorLoader(root);
        loader.load(arrowRecords);

        return root;
    }
}
