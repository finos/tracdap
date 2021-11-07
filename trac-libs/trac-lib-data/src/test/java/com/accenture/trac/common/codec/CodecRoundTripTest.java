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

import com.accenture.trac.common.codec.arrow.ArrowSchema;
import com.accenture.trac.common.codec.arrow.ArrowStreamCodec;
import com.accenture.trac.common.codec.csv.CsvCodec;
import com.accenture.trac.common.concurrent.Flows;
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.metadata.*;
import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.NettyAllocationManager;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.resultOf;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.waitFor;

public abstract class CodecRoundTripTest {

    static class ArrowStream extends CodecRoundTripTest {
        @BeforeEach void setup() { codec = new ArrowStreamCodec(); }
    }

    static class CSV extends CodecRoundTripTest {
        @BeforeEach void setup() { codec = new CsvCodec(); }
    }

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    private static final SchemaDefinition BASIC_SCHEMA = SchemaDefinition.newBuilder()
            .setSchemaType(SchemaType.TABLE)
            .setTable(TableSchema.newBuilder()
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("string_field")
                    .setFieldOrder(0)
                    .setFieldType(BasicType.STRING)
                    .setLabel("A string field"))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("int_field")
                    .setFieldOrder(0)
                    .setFieldType(BasicType.INTEGER)
                    .setLabel("An integer field")))
            .build();

    ICodec codec;

    @Test
    void roundTrip_basic() throws Exception {

        var stringValues = IntStream.rangeClosed(0, 9)
                .mapToObj(i -> String.format("string_%d", i))
                .toArray();

        var intValues = IntStream.rangeClosed(0, 9)
                .toArray();

        var arrowSchema = ArrowSchema.tracToArrow(BASIC_SCHEMA);

        var allocatorConfig = RootAllocator.configBuilder()
                .allocationManagerFactory(NettyAllocationManager.FACTORY)
                .build();

        var allocator = new RootAllocator(allocatorConfig);

        var stringVec = new VarCharVector(arrowSchema.getFields().get(0), allocator);
        stringVec.setInitialCapacity(10);
        stringVec.allocateNew();

        var intVec = new BigIntVector(arrowSchema.getFields().get(1), allocator);
        intVec.setInitialCapacity(10);
        intVec.allocateNew();

        for (int i = 0; i < 10; i++) {
            stringVec.set(i, stringValues[i].toString().getBytes(StandardCharsets.UTF_8));
            intVec.set(i, intValues[i]);
        }

        var root = new VectorSchemaRoot(arrowSchema.getFields(), List.of(stringVec, intVec));
        root.setRowCount(10);

        var unloader = new VectorUnloader(root);
        var batch = unloader.getRecordBatch();
        var schemaBlock = DataBlock.forSchema(arrowSchema);
        var dataBlock = DataBlock.forRecords(batch);
        var blockStream = Flows.publish(Stream.of(schemaBlock, dataBlock));

        var encoder = codec.getEncoder(allocator, BASIC_SCHEMA, Map.of());
        var decoder = codec.getDecoder(allocator, BASIC_SCHEMA, Map.of());

        blockStream.subscribe(encoder);
        encoder.subscribe(decoder);

        var roundTrip = Flows.fold(decoder, (bs, b) -> {bs.add(b); return bs;}, new ArrayList<DataBlock>());
        waitFor(TEST_TIMEOUT, roundTrip);
        var rtBlocks = resultOf(roundTrip);

        Assertions.assertEquals(2, rtBlocks.size());
        Assertions.assertNotNull(rtBlocks.get(0).arrowSchema);
        Assertions.assertNotNull(rtBlocks.get(1).arrowRecords);

        var rtSchema = rtBlocks.get(0).arrowSchema;
        var rtBatch = rtBlocks.get(1).arrowRecords;

        Assertions.assertEquals(arrowSchema, rtSchema);

        var rtFields = rtSchema.getFields();
        var rtStringVec = new VarCharVector(rtFields.get(0), allocator);
        var rtIntVec = new BigIntVector(rtFields.get(1), allocator);
        var rtRoot = new VectorSchemaRoot(rtFields, List.of(rtStringVec, rtIntVec));

        var rtLoader = new VectorLoader(rtRoot);
        rtLoader.load(rtBatch);

        Assertions.assertEquals(10, rtRoot.getRowCount());

        for (int i = 0; i < 10; i++) {

            Assertions.assertEquals(stringValues[i], rtStringVec.getObject(i).toString());
            Assertions.assertEquals(intValues[i], rtIntVec.get(i));
        }

        rtRoot.close();
        rtBatch.close();
        root.close();
        batch.close();
    }
}
