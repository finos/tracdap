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

package com.accenture.trac.common.codec;

import com.accenture.trac.common.codec.arrow.ArrowFileCodec;
import com.accenture.trac.common.codec.arrow.ArrowSchema;
import com.accenture.trac.common.codec.arrow.ArrowStreamCodec;
import com.accenture.trac.common.codec.csv.CsvCodec;
import com.accenture.trac.common.codec.json.JsonCodec;
import com.accenture.trac.common.concurrent.Flows;
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.test.data.SampleDataFormats;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.resultOf;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.waitFor;
import static com.accenture.trac.test.data.SampleDataFormats.generateBasicData;


public abstract class CodecRoundTripTest {

    // Concrete test cases for codecs included in CORE_DATA

    static class ArrowStream extends CodecRoundTripTest { @BeforeEach void setup() {
        codec = new ArrowStreamCodec();
        basicData = null;
    } }

    static class ArrowFile extends CodecRoundTripTest { @BeforeEach void setup() {
        codec = new ArrowFileCodec();
        basicData = null;
    } }

    static class CSV extends CodecRoundTripTest { @BeforeEach void setup() {
        codec = new CsvCodec();
        basicData = "/sample_data_formats/csv_basic.csv";
    } }

    static class JSON extends CodecRoundTripTest { @BeforeAll
    static void setup() {
        codec = new JsonCodec();
        basicData = "/sample_data_formats/json_basic.json";
    } }

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    static ICodec codec;
    static String basicData;

    private boolean basicDataAvailable() {
        return basicData != null;
    }

    @Test
    void basic_roundTrip() throws Exception {

        var allocator = new RootAllocator();
        var arrowSchema = ArrowSchema.tracToArrow(SampleDataFormats.BASIC_TABLE_SCHEMA);
        var root = generateBasicData(allocator);

        var unloader = new VectorUnloader(root);
        var batch = unloader.getRecordBatch();
        var schemaBlock = DataBlock.forSchema(arrowSchema);
        var dataBlock = DataBlock.forRecords(batch);
        var blockStream = Flows.publish(Stream.of(schemaBlock, dataBlock));

        var encoder = codec.getEncoder(allocator, SampleDataFormats.BASIC_TABLE_SCHEMA, Map.of());
        var decoder = codec.getDecoder(allocator, SampleDataFormats.BASIC_TABLE_SCHEMA, Map.of());

        blockStream.subscribe(encoder);
        encoder.subscribe(decoder);

        var roundTrip = Flows.fold(decoder, (bs, b) -> {bs.add(b); return bs;}, new ArrayList<DataBlock>());
        waitFor(TEST_TIMEOUT, roundTrip);
        var rtBlocks = resultOf(roundTrip);

        Assertions.assertEquals(2, rtBlocks.size());
        Assertions.assertEquals(arrowSchema, rtBlocks.get(0).arrowSchema);

        compareBatchToRoot(root, rtBlocks.get(0).arrowSchema, rtBlocks.get(1).arrowRecords, allocator);

        var rtBatch = rtBlocks.get(1).arrowRecords;
        rtBatch.close();
        root.close();
        batch.close();
    }

    @Test
    @EnabledIf(value = "basicDataAvailable", disabledReason = "Pre-saved test data not available for this format")
    void basic_decode() throws Exception {

        var allocator = new RootAllocator();
        var arrowSchema = ArrowSchema.tracToArrow(SampleDataFormats.BASIC_TABLE_SCHEMA);
        var root = generateBasicData(allocator);

        var testData = loadResource(basicData);
        var testDataBuf = Unpooled.wrappedBuffer(testData);
        var testDataStream = Flows.publish(List.of(testDataBuf));

        var decoder = codec.getDecoder(allocator, SampleDataFormats.BASIC_TABLE_SCHEMA, Map.of());
        testDataStream.subscribe(decoder);

        var roundTrip = Flows.fold(decoder, (bs, b) -> {bs.add(b); return bs;}, new ArrayList<DataBlock>());
        waitFor(TEST_TIMEOUT, roundTrip);
        var rtBlocks = resultOf(roundTrip);

        Assertions.assertEquals(2, rtBlocks.size());
        Assertions.assertEquals(arrowSchema, rtBlocks.get(0).arrowSchema);

        compareBatchToRoot(root, rtBlocks.get(0).arrowSchema, rtBlocks.get(1).arrowRecords, allocator);

        var rtBatch = rtBlocks.get(1).arrowRecords;
        rtBatch.close();
        root.close();
    }

    private void compareBatchToRoot(VectorSchemaRoot root, Schema rtSchema, ArrowRecordBatch rtBatch, BufferAllocator allocator) {


        var arrowSchema = ArrowSchema.tracToArrow(SampleDataFormats.BASIC_TABLE_SCHEMA);
        Assertions.assertEquals(arrowSchema, rtSchema);

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
                    Assertions.assertEquals(vec.getObject(i), rtVec.getObject(i));
            }
        }
    }

    private byte[] loadResource(String resourcePath) {

        try (var stream = getClass().getResourceAsStream(resourcePath)) {

            if (stream == null)
                throw new IOException("Failed to read resource: " + resourcePath);

            return stream.readAllBytes();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
