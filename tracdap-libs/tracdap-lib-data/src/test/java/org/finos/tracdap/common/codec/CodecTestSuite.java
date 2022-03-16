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

package org.finos.tracdap.common.codec;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.finos.tracdap.common.codec.arrow.ArrowFileCodec;
import org.finos.tracdap.common.codec.arrow.ArrowSchema;
import org.finos.tracdap.common.codec.arrow.ArrowStreamCodec;
import org.finos.tracdap.common.codec.csv.CsvCodec;
import org.finos.tracdap.common.codec.json.JsonCodec;
import org.finos.tracdap.common.concurrent.Flows;
import org.finos.tracdap.common.data.DataBlock;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.metadata.SchemaDefinition;
import org.finos.tracdap.test.data.SampleDataFormats;
import org.finos.tracdap.test.helpers.TestResourceHelpers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIf;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.resultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.finos.tracdap.test.data.SampleDataFormats.generateBasicData;


public abstract class CodecTestSuite {

    // Concrete test cases for codecs included in CORE_DATA

    static class ArrowStreamTest extends CodecTestSuite { @BeforeAll static void setup() {
        codec = new ArrowStreamCodec();
        basicData = null;
    } }

    static class ArrowFileTest extends CodecTestSuite { @BeforeAll static void setup() {
        codec = new ArrowFileCodec();
        basicData = null;
    } }

    static class CSVTest extends CodecTestSuite { @BeforeAll static void setup() {
        codec = new CsvCodec();
        basicData = SampleDataFormats.BASIC_CSV_DATA_RESOURCE;
    } }

    static class JSONTest extends CodecTestSuite { @BeforeAll static void setup() {
        codec = new JsonCodec();
        basicData = SampleDataFormats.BASIC_JSON_DATA_RESOURCE;
    } }

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    static ICodec codec;
    static String basicData;

    private boolean basicDataAvailable() {
        return basicData != null;
    }

    @Test
    void roundTrip_basic() throws Exception {

        var allocator = new RootAllocator();
        var arrowSchema = ArrowSchema.tracToArrow(SampleDataFormats.BASIC_TABLE_SCHEMA);
        var root = generateBasicData(allocator);

        roundTrip_impl(SampleDataFormats.BASIC_TABLE_SCHEMA, arrowSchema, root, allocator);
    }

    @Test
    void roundTrip_nulls() throws Exception {

        var allocator = new RootAllocator();
        var arrowSchema = ArrowSchema.tracToArrow(SampleDataFormats.BASIC_TABLE_SCHEMA);
        var root = generateBasicData(allocator);

        // With the basic test data, we'll get one null value for each data type

        var limit = Math.min(root.getRowCount(), root.getFieldVectors().size());

        for (var i = 0; i < limit; i++) {

            var vector = root.getVector(i);

            if (BaseFixedWidthVector.class.isAssignableFrom(vector.getClass())) {

                var fixedVector = (BaseFixedWidthVector) vector;
                fixedVector.setNull(i);
            }
            else if (BaseVariableWidthVector.class.isAssignableFrom(vector.getClass())) {

                var variableVector = (BaseVariableWidthVector) vector;
                variableVector.setNull(i);
            }
            else {

                throw new EUnexpected();
            }

        }

        roundTrip_impl(SampleDataFormats.BASIC_TABLE_SCHEMA, arrowSchema, root, allocator);
    }

    @Test
    void roundTrip_edgeCaseStrings() throws Exception {

        var allocator = new RootAllocator();

        var fieldType = new FieldType(true, ArrowType.Utf8.INSTANCE, null);
        var field = new Field("string_field", fieldType, null);
        var arrowSchema = new Schema(List.of(field));
        var tracSchema = ArrowSchema.arrowToTrac(arrowSchema);

        var stringVec = new VarCharVector("string_field", allocator);
        stringVec.allocateNew(10);

        stringVec.set(0, "hello".getBytes(StandardCharsets.UTF_8));
        stringVec.setNull(1);
        stringVec.set(2, "".getBytes(StandardCharsets.UTF_8));
        stringVec.set(3, " ".getBytes(StandardCharsets.UTF_8));
        stringVec.set(4, "\r\n\t".getBytes(StandardCharsets.UTF_8));
        stringVec.set(5, "\\\"/\\//\\".getBytes(StandardCharsets.UTF_8));
        stringVec.set(6, " hello\nworld ".getBytes(StandardCharsets.UTF_8));
        stringVec.set(7, "你好世界".getBytes(StandardCharsets.UTF_8));
        stringVec.set(8, "مرحبا بالعالم".getBytes(StandardCharsets.UTF_8));
        stringVec.set(9, "\0".getBytes(StandardCharsets.UTF_8));

        var root = new VectorSchemaRoot(List.of(field), List.of(stringVec));
        root.setRowCount(10);

        roundTrip_impl(tracSchema, arrowSchema, root, allocator);
    }


    void roundTrip_impl(
            SchemaDefinition tracSchema, Schema arrowSchema,
            VectorSchemaRoot root, RootAllocator allocator) throws Exception {

        var unloader = new VectorUnloader(root);
        var batch = unloader.getRecordBatch();
        var schemaBlock = DataBlock.forSchema(arrowSchema);
        var dataBlock = DataBlock.forRecords(batch);
        var blockStream = Flows.publish(Stream.of(schemaBlock, dataBlock));

        var encoder = codec.getEncoder(allocator, tracSchema, Map.of());
        var decoder = codec.getDecoder(allocator, tracSchema, Map.of());

        blockStream.subscribe(encoder);
        encoder.subscribe(decoder);

        var roundTrip = Flows.fold(decoder, (bs, b) -> {bs.add(b); return bs;}, new ArrayList<DataBlock>());
        waitFor(TEST_TIMEOUT, roundTrip);
        var rtBlocks = resultOf(roundTrip);

        Assertions.assertEquals(2, rtBlocks.size());
        Assertions.assertEquals(arrowSchema, rtBlocks.get(0).arrowSchema);

        compareBatchToRoot(
                arrowSchema, rtBlocks.get(0).arrowSchema,
                root, rtBlocks.get(1).arrowRecords, allocator);

        var rtBatch = rtBlocks.get(1).arrowRecords;
        rtBatch.close();
        root.close();
        batch.close();
    }

    @Test
    @EnabledIf(value = "basicDataAvailable", disabledReason = "Pre-saved test data not available for this format")
    void decode_basic() throws Exception {

        var allocator = new RootAllocator();
        var arrowSchema = ArrowSchema.tracToArrow(SampleDataFormats.BASIC_TABLE_SCHEMA);
        var root = generateBasicData(allocator);

        var testData = TestResourceHelpers.loadResourceAsBytes(basicData);
        var testDataBuf = Unpooled.wrappedBuffer(testData);
        var testDataStream = Flows.publish(List.of(testDataBuf));

        var decoder = codec.getDecoder(allocator, SampleDataFormats.BASIC_TABLE_SCHEMA, Map.of());
        testDataStream.subscribe(decoder);

        var roundTrip = Flows.fold(decoder, (bs, b) -> {bs.add(b); return bs;}, new ArrayList<DataBlock>());
        waitFor(TEST_TIMEOUT, roundTrip);
        var rtBlocks = resultOf(roundTrip);

        Assertions.assertEquals(2, rtBlocks.size());
        Assertions.assertEquals(arrowSchema, rtBlocks.get(0).arrowSchema);

        compareBatchToRoot(
                arrowSchema, rtBlocks.get(0).arrowSchema,
                root, rtBlocks.get(1).arrowRecords, allocator);

        var rtBatch = rtBlocks.get(1).arrowRecords;
        rtBatch.close();
        root.close();
    }

    @Test
    void decode_empty() {

        var allocator = new RootAllocator();

        // An empty stream (i.e. with no buffers)

        var noBufStream = Flows.publish(List.<ByteBuf>of());
        var noBufDecoder = codec.getDecoder(allocator, SampleDataFormats.BASIC_TABLE_SCHEMA, Map.of());
        noBufStream.subscribe(noBufDecoder);

        var noBufResult = Flows.fold(noBufDecoder, (bs, b) -> {bs.add(b); return bs;}, new ArrayList<DataBlock>());
        waitFor(TEST_TIMEOUT, noBufResult);

        Assertions.assertThrows(EDataCorruption.class, () -> resultOf(noBufResult));

        // A stream containing a series of empty buffers

        var emptyBuffers = List.of(
                new EmptyByteBuf(ByteBufAllocator.DEFAULT),
                new EmptyByteBuf(ByteBufAllocator.DEFAULT),
                new EmptyByteBuf(ByteBufAllocator.DEFAULT));

        var emptyBufStream = Flows.publish(emptyBuffers);
        var emptyBufDecoder = codec.getDecoder(allocator, SampleDataFormats.BASIC_TABLE_SCHEMA, Map.of());
        emptyBufStream.subscribe(emptyBufDecoder);

        var emptyBufResult = Flows.fold(emptyBufDecoder, (bs, b) -> {bs.add(b); return bs;}, new ArrayList<DataBlock>());
        waitFor(TEST_TIMEOUT, emptyBufResult);

        Assertions.assertThrows(EDataCorruption.class, () -> resultOf(emptyBufResult));
    }

    @Test
    void decode_garbled() {

        // Send a stream of random bytes - 3 chunks worth

        var testData = List.of(
                new byte[10000],
                new byte[10000],
                new byte[10000]);

        var random = new Random();
        testData.forEach(random::nextBytes);

        var testDataBuf = testData.stream().map(Unpooled::wrappedBuffer).collect(Collectors.toList());
        var testDataStream = Flows.publish(testDataBuf);

        // Run the garbage data through the decoder

        var allocator = new RootAllocator();
        var decoder = codec.getDecoder(allocator, SampleDataFormats.BASIC_TABLE_SCHEMA, Map.of());
        testDataStream.subscribe(decoder);

        // Decoder should report EDataCorruption

        var decodeResult = Flows.fold(decoder, (bs, b) -> {bs.add(b); return bs;}, new ArrayList<DataBlock>());
        waitFor(TEST_TIMEOUT, decodeResult);

        Assertions.assertThrows(EDataCorruption.class, () -> resultOf(decodeResult));
    }


    private void compareBatchToRoot(
            Schema expectedSchema, Schema rtSchema,
            VectorSchemaRoot root, ArrowRecordBatch rtBatch, BufferAllocator allocator) {

        Assertions.assertEquals(expectedSchema, rtSchema);

        var rtFields = rtSchema.getFields();
        var rtVectors = rtFields.stream().map(f -> f.createVector(allocator)).collect(Collectors.toList());

        try (var rtRoot = new VectorSchemaRoot(rtFields, rtVectors)) {

            var rtLoader = new VectorLoader(rtRoot);
            rtLoader.load(rtBatch);

            Assertions.assertEquals(root.getRowCount(), rtRoot.getRowCount());

            for (var j = 0; j < root.getFieldVectors().size(); j++) {

                var vec = root.getVector(j);
                var rtVec = rtRoot.getVector(j);

                for (int i = 0; i < 10; i++)
                    Assertions.assertEquals(vec.getObject(i), rtVec.getObject(i), "Mismatch on row " + i);
            }
        }
    }
}
