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
import org.finos.tracdap.test.data.SampleDataFormats;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Stream;

import static org.finos.tracdap.svc.orch.jobs.Helpers.runJob;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.resultOf;
import static org.finos.tracdap.test.data.SampleDataFormats.generateBasicData;


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
    static VectorSchemaRoot originalData;

    static TagHeader modelId;
    static TagHeader inputDataId;
    static TagHeader outputDataId;

    @BeforeAll
    static void setUp() {

        ALLOCATOR = new RootAllocator();
    }

    @AfterAll
    static void cleanUp() {

        if (originalData != null)
            originalData.close();

        ALLOCATOR.close();
    }

    @Test
    @Order(1)
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
    @Order(2)
    void saveInputData() throws Exception {

        var arrowSchema = ArrowSchema.tracToArrow(SampleDataFormats.BASIC_TABLE_SCHEMA);
        var root = generateBasicData(ALLOCATOR);

        var unloader = new VectorUnloader(root);
        var batch = unloader.getRecordBatch();
        var schemaBlock = DataBlock.forSchema(arrowSchema);
        var dataBlock = DataBlock.forRecords(batch);
        var blockStream = Flows.publish(Stream.of(schemaBlock, dataBlock));

        var codec = new ArrowFileCodec();
        var encoder = codec.getEncoder(ALLOCATOR, SampleDataFormats.BASIC_TABLE_SCHEMA, Map.of());

        blockStream.subscribe(encoder);
        var opaqueData_ = Flows.fold(encoder, (bs, b) -> bs.addComponent(true, b), ByteBufAllocator.DEFAULT.compositeBuffer());
        var opaqueData = resultOf(opaqueData_);

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(SampleDataFormats.BASIC_TABLE_SCHEMA)
                .setFormat("application/vnd.apache.arrow.file")
                .setContent(ByteString.copyFrom(opaqueData.nioBuffer()))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("round_trip_dataset")
                        .setValue(MetadataCodec.encodeValue("round_trip:input")))
                .build();

        var dataClient = platform.dataClientBlocking();
        inputDataId = dataClient.createSmallDataset(writeRequest);

        originalData = root;
    }

    @Test @Order(3)
    void runModel() {

        var metaClient = platform.metaClientBlocking();
        var orchClient = platform.orchClientBlocking();

        var runModel = RunModelJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(modelId))
                .putParameters("use_spark", MetadataCodec.encodeValue(false))
                .putInputs("round_trip_input", MetadataUtil.selectorFor(inputDataId))
                .addOutputAttrs(TagUpdate.newBuilder()
                .setAttrName("round_trip_dataset")
                .setValue(MetadataCodec.encodeValue("round_trip:output")))
                .build();

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setRunModel(runModel))
                .addJobAttrs(TagUpdate.newBuilder()
                .setAttrName("round_trip_job")
                .setValue(MetadataCodec.encodeValue("round_trip:run_model")))
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

        outputDataId = dataSearchResult.getSearchResult(0).getHeader();
    }

    @Test
    @Order(4)
    void checkOutputData() throws Exception {

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
        var decoder = codec.getDecoder(allocator, SampleDataFormats.BASIC_TABLE_SCHEMA, Map.of());

        dataStream.subscribe(decoder);
        var blocks = resultOf(Flows.toList(decoder));

        Assertions.assertEquals(2, blocks.size());

        var arrowSchema = blocks.get(0).arrowSchema;
        var arrowRecords = blocks.get(1).arrowRecords;
        var root = VectorSchemaRoot.create(arrowSchema, allocator);
        var loader = new VectorLoader(root);
        loader.load(arrowRecords);

        Assertions.assertEquals(originalData.getSchema(), root.getSchema());
        Assertions.assertEquals(originalData.getRowCount(), root.getRowCount());

        for (var field : root.getSchema().getFields()) {

            for (var i = 0; i < root.getRowCount(); i++) {

                var original = originalData.getVector(field).getObject(i);
                var rt = root.getVector(field).getObject(i);

                Assertions.assertEquals(
                        original, rt,
                        String.format("Difference in column [%s] row [%d]", field.getName(), i));
            }
        }

        root.close();
    }
}
