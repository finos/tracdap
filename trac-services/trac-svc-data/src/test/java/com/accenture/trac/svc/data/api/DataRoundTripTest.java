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

import com.accenture.trac.api.*;
import com.accenture.trac.common.concurrent.Flows;
import com.accenture.trac.common.concurrent.Futures;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.metadata.*;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.accenture.trac.common.metadata.MetadataUtil.selectorFor;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.resultOf;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.waitFor;


class DataRoundTripTest extends DataApiTestBase {

    private static final String BASIC_CSV_DATA = "/basic_csv_data.csv";
    private static final String BASIC_JSON_DATA = "/basic_json_data.json";

    private static class TestDataContainer {

        List<String> fieldNames;
        List<BasicType> fieldTypes;
        List<Vector<Object>> values;
    }

    private static final SchemaDefinition BASIC_TEST_SCHEMA;
    private static final TestDataContainer BASIC_TEST_DATA;

    static {

        BASIC_TEST_SCHEMA = SchemaDefinition.newBuilder()
                .setSchemaType(SchemaType.TABLE)
                .setTable(TableSchema.newBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("string_field")
                        .setFieldOrder(0)
                        .setFieldType(BasicType.STRING))
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("int_field")
                        .setFieldOrder(1)
                        .setFieldType(BasicType.INTEGER)))
                .build();

        BASIC_TEST_DATA = new TestDataContainer();
        BASIC_TEST_DATA.fieldNames = List.of("string_field", "int_field");
        BASIC_TEST_DATA.fieldTypes = List.of(BasicType.STRING, BasicType.INTEGER);
        BASIC_TEST_DATA.values = List.of(new Vector<>(), new Vector<>());

        for (int i = 0; i < 10; i++) {
            BASIC_TEST_DATA.values.get(0).add("string_" + i);
            BASIC_TEST_DATA.values.get(1).add((long) i);
        }
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

    @Test
    void roundTrip_arrowBasic() throws Exception {

        // Create a single batch of Arrow data

        var allocator = new RootAllocator();
        var varcharVector = new VarCharVector("string_field", allocator);
        var intVector = new IntVector("int_field", allocator);

        var nRows = 10;

        var originalVarchar = new String[nRows];
        var originalInt = new int[nRows];

        varcharVector.allocateNew(nRows);
        intVector.allocateNew(nRows);

        for (int i = 0; i < nRows; i++) {

            originalVarchar[i] = String.format("string_%d", i);
            originalInt[i] = i;

            varcharVector.set(i, originalVarchar[i].getBytes(StandardCharsets.UTF_8));
            intVector.set(i, originalInt[i]);
        }

        varcharVector.setValueCount(nRows);
        intVector.setValueCount(nRows);

        var fields = List.of(varcharVector.getField(), intVector.getField());
        var vectors = List.<FieldVector>of(varcharVector, intVector);
        var batch = new VectorSchemaRoot(fields, vectors);

        // Use a writer to encode the batch as a stream of chunks (arrow record batches, including the schema)

        var writeChannel = new ChunkChannel();

        try (var writer = new ArrowStreamWriter(batch, null, writeChannel)) {

            writer.start();
            writer.writeBatch();
            writer.end();
        }

        roundTripTest(writeChannel.getChunks(), "ARROW", "ARROW", this::decodeArrow, BASIC_TEST_DATA, true);
        roundTripTest(writeChannel.getChunks(), "ARROW", "ARROW", this::decodeArrow, BASIC_TEST_DATA, false);
    }

    @Test
    void roundTrip_csv() throws Exception {

        var testDataStream = getClass().getResourceAsStream(BASIC_CSV_DATA);

        if (testDataStream == null)
            throw new RuntimeException("Test data not found");

        var testDataBytes = testDataStream.readAllBytes();
        var testData = List.of(ByteString.copyFrom(testDataBytes));

        roundTripTest(testData, "CSV", "CSV", this::decodeCsv, BASIC_TEST_DATA, true);
        roundTripTest(testData, "CSV", "CSV", this::decodeCsv, BASIC_TEST_DATA, false);
    }

    @Test
    void roundTrip_json() throws Exception {

        var testDataStream = getClass().getResourceAsStream(BASIC_JSON_DATA);

        if (testDataStream == null)
            throw new RuntimeException("Test data not found");

        var testDataBytes = testDataStream.readAllBytes();
        var testData = List.of(ByteString.copyFrom(testDataBytes));

        roundTripTest(testData, "JSON", "JSON", this::decodeJson, BASIC_TEST_DATA, true);
        roundTripTest(testData, "JSON", "JSON", this::decodeJson, BASIC_TEST_DATA, false);
    }

    private void roundTripTest(
            List<ByteString> content, String writeFormat, String readFormat,
            BiFunction<SchemaDefinition, List<ByteString>, TestDataContainer> decodeFunc,
            TestDataContainer expectedResult, boolean dataInChunkZero) throws Exception {

        var requestParams = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(BASIC_TEST_SCHEMA)
                .setFormat(writeFormat)
                .build();

        var createDatasetRequest = dataWriteRequest(requestParams, content, dataInChunkZero);
        var createDataset = DataApiTestHelpers.clientStreaming(dataClient::createDataset, createDatasetRequest);

        waitFor(TEST_TIMEOUT, createDataset);
        var objHeader = resultOf(createDataset);

        // Fetch metadata for the data and storage objects that should be created

        var dataDef = fetchDefinition(selectorFor(objHeader), ObjectDefinition::getData);
        var storageDef = fetchDefinition(dataDef.getStorageId(), ObjectDefinition::getStorage);

        // TODO: Check definitions

        var dataRequest = DataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selectorFor(objHeader))
                .setFormat(readFormat)
                .build();

        var readResponse = Flows.<DataReadResponse>hub(execContext);
        var readResponse0 = Flows.first(readResponse);
        var readByteStream = Flows.map(readResponse, DataReadResponse::getContent);
        var readBytes = Flows.fold(readByteStream, ByteString::concat, ByteString.EMPTY);

        DataApiTestHelpers.serverStreaming(dataClient::readDataset, dataRequest, readResponse);

        waitFor(Duration.ofMinutes(20), readResponse0, readBytes);
        var roundTripResponse = resultOf(readResponse0);
        var roundTripSchema = roundTripResponse.getSchema();
        var roundTripBytes = resultOf(readBytes);

        var roundTripData = decodeFunc.apply(roundTripSchema, List.of(roundTripBytes));

        Assertions.assertEquals(BASIC_TEST_SCHEMA, roundTripSchema);
        Assertions.assertEquals(expectedResult.values, roundTripData.values);
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

    private <TDef>
    TDef fetchDefinition(
            TagSelector selector,
            Function<ObjectDefinition, TDef> defTypeFunc)
            throws Exception {

        var tagGrpc = metaClient.readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selector)
                .build());

        var tag = Futures.javaFuture(tagGrpc);

        waitFor(TEST_TIMEOUT, tag);

        var objDef = resultOf(tag).getDefinition();

        return defTypeFunc.apply(objDef);
    }



    private TestDataContainer decodeCsv(SchemaDefinition schema, List<ByteString> data) {

        var result = new TestDataContainer();

        result.fieldNames = schema.getTable()
                .getFieldsList().stream()
                .map(FieldSchema::getFieldName)
                .collect(Collectors.toList());

        result.fieldTypes = schema.getTable()
                .getFieldsList().stream()
                .map(FieldSchema::getFieldType)
                .collect(Collectors.toList());

        result.values = IntStream
                .range(0, result.fieldNames.size())
                .mapToObj(x -> new Vector<>())
                .collect(Collectors.toList());

        var allData = data.stream().reduce(ByteString.EMPTY, ByteString::concat).toString(StandardCharsets.UTF_8);

        try (var reader = new BufferedReader(new StringReader(allData))) {

            reader.readLine();  // skip header

            var csvReader = CsvMapper.builder().build()
                    .readerForArrayOf(String.class)
                    .with(CsvParser.Feature.WRAP_AS_ARRAY);

            try (var itr = csvReader.readValues(reader)) {

                int nCols = result.fieldNames.size();
                int row = 0;

                while (itr.hasNextValue()) {

                    var csvValues = (Object[]) itr.nextValue();

                    for (int col = 0; col < nCols; col++) {

                        var fieldType = result.fieldTypes.get(col);
                        var csvValue = csvValues[col];
                        var vector = result.values.get(col);

                        var objValue = decodeJavaObject(fieldType, csvValue);

                        vector.add(row, objValue);
                    }

                    row++;
                }
            }

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    private TestDataContainer decodeJson(SchemaDefinition schema, List<ByteString> data) {
        return null;
    }

    private TestDataContainer decodeArrow(SchemaDefinition schema, List<ByteString> data) {

        var allocator = new RootAllocator();  // TODO: Pass in an allocator

        var allData = data.stream().reduce(ByteString.EMPTY, ByteString::concat);

        try (var stream = new ByteArrayInputStream(allData.toByteArray());
             var reader = new ArrowStreamReader(stream, allocator)) {

            var rtBatch = reader.getVectorSchemaRoot();
            var rtSchema = rtBatch.getSchema();
            var nCols = rtSchema.getFields().size();

            var result = new TestDataContainer();
            result.fieldNames = rtSchema.getFields().stream().map(Field::getName).collect(Collectors.toList());
            result.fieldTypes = null;  // new BasicType[rtSchema.getFields().size()];
            result.values = new ArrayList<>(nCols);

            for(var j = 0; j < nCols; j++)
                result.values.set(j, new Vector<>());

            while (reader.loadNextBatch()) {

                for (int j = 0; j < nCols; j++) {

                    var resultCol = result.values.get(j);
                    var arrowCol = rtBatch.getVector(j);

                    for (int i = 0; i < rtBatch.getRowCount(); i++)
                        resultCol.add(arrowCol.getObject(i));
                }
            }

            return result;
        }
        catch (Exception e) {

            if (e instanceof RuntimeException)
                throw (RuntimeException) e;
            else
                throw new RuntimeException(e);
        }
    }

    private Object decodeJavaObject(BasicType fieldType, Object rawObject) {

        switch (fieldType) {

            case BOOLEAN:

                if (rawObject instanceof Boolean)
                    return rawObject;

                throw new EUnexpected();

            case INTEGER:

                if (rawObject instanceof Long) return rawObject;
                if (rawObject instanceof Integer) return (long) (int) rawObject;
                if (rawObject instanceof Short) return (long) (short) rawObject;
                if (rawObject instanceof Byte) return (long) (byte) rawObject;

                if (rawObject instanceof String)
                    return Long.parseLong(rawObject.toString());

                throw new EUnexpected();

            case FLOAT:

                if (rawObject instanceof Double) return rawObject;
                if (rawObject instanceof Float) return rawObject;

                if (rawObject instanceof String)
                    return Double.parseDouble(rawObject.toString());

                throw new EUnexpected();

            case STRING:

                return rawObject.toString();

            default:

                throw new EUnexpected();
        }

    }
}
