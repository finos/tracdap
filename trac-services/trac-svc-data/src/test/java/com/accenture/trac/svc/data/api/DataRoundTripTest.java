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
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.metadata.*;

import com.accenture.trac.test.data.SampleDataFormats;
import com.accenture.trac.test.helpers.TestResourceHelpers;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
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

    private static final String BASIC_CSV_DATA = SampleDataFormats.BASIC_CSV_DATA_RESOURCE;
    private static final String BASIC_JSON_DATA = SampleDataFormats.BASIC_JSON_DATA_RESOURCE;

    static final byte[] BASIC_CSV_CONTENT = TestResourceHelpers.loadResourceAsBytes(BASIC_CSV_DATA);

    private static class TestDataContainer {

        List<String> fieldNames;
        List<BasicType> fieldTypes;
        List<Vector<Object>> values;
    }

    private final TestDataContainer BASIC_TEST_DATA = decodeCsv(
            SampleDataFormats.BASIC_TABLE_SCHEMA,
            List.of(ByteString.copyFrom(BASIC_CSV_CONTENT)));


    @Test
    void roundTrip_arrowStream() throws Exception {

        // Create a single batch of Arrow data

        var allocator = new RootAllocator();
        var root = SampleDataFormats.generateBasicData(allocator);

        // Use a writer to encode the batch as a stream of chunks (arrow record batches, including the schema)

        var writeChannel = new ChunkChannel();

        try (var writer = new ArrowStreamWriter(root, null, writeChannel)) {

            writer.start();
            writer.writeBatch();
            writer.end();
        }

        var mimeType = "application/vnd.apache.arrow.stream";
        roundTripTest(writeChannel.getChunks(), mimeType, mimeType, this::decodeArrow, BASIC_TEST_DATA, true);
        roundTripTest(writeChannel.getChunks(), mimeType, mimeType, this::decodeArrow, BASIC_TEST_DATA, false);
    }

    @Test
    void roundTrip_csv() throws Exception {

        var testDataStream = getClass().getResourceAsStream(BASIC_CSV_DATA);

        if (testDataStream == null)
            throw new RuntimeException("Test data not found");

        var testDataBytes = testDataStream.readAllBytes();
        var testData = List.of(ByteString.copyFrom(testDataBytes));

        var mimeType = "text/csv";
        roundTripTest(testData, mimeType, mimeType, this::decodeCsv, BASIC_TEST_DATA, true);
        roundTripTest(testData, mimeType, mimeType, this::decodeCsv, BASIC_TEST_DATA, false);
    }

    @Test
    void roundTrip_json() throws Exception {

        var testDataStream = getClass().getResourceAsStream(BASIC_JSON_DATA);

        if (testDataStream == null)
            throw new RuntimeException("Test data not found");

        var testDataBytes = testDataStream.readAllBytes();
        var testData = List.of(ByteString.copyFrom(testDataBytes));

        var mimeType = "text/json";
        roundTripTest(testData, mimeType, mimeType, this::decodeJson, BASIC_TEST_DATA, true);
        roundTripTest(testData, mimeType, mimeType, this::decodeJson, BASIC_TEST_DATA, false);
    }

    private void roundTripTest(
            List<ByteString> content, String writeFormat, String readFormat,
            BiFunction<SchemaDefinition, List<ByteString>, TestDataContainer> decodeFunc,
            TestDataContainer expectedResult, boolean dataInChunkZero) throws Exception {

        var requestParams = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(SampleDataFormats.BASIC_TABLE_SCHEMA)
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

        Assertions.assertEquals(SampleDataFormats.BASIC_TABLE_SCHEMA, roundTripSchema);

        for (int i = 0; i < roundTripSchema.getTable().getFieldsCount(); i++) {

            for (var row = 0; row < expectedResult.values.size(); row++) {

                var expectedVal = expectedResult.values.get(i).get(row);
                var roundTripVal = roundTripData.values.get(i).get(row);

                // Allow comparing big decimals with different scales
                if (expectedVal instanceof BigDecimal)
                    roundTripVal = ((BigDecimal) roundTripVal).setScale(((BigDecimal) expectedVal).scale(), RoundingMode.UNNECESSARY);

                Assertions.assertEquals(expectedVal, roundTripVal);
            }
        }
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

        var result = schemaResult(schema);

        var allData = data.stream().reduce(ByteString.EMPTY, ByteString::concat).toString(StandardCharsets.UTF_8);

        try (var reader = new BufferedReader(new StringReader(allData))) {

            reader.readLine();  // skip header

            var csvMapper = CsvMapper.builder()
                    .enable(CsvParser.Feature.TRIM_SPACES)
                    .build();

            var csvReader = csvMapper
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

    private TestDataContainer decodeJson(SchemaDefinition schema, List<ByteString> rawData) {

        var result = schemaResult(schema);

        var fieldMap = new HashMap<String, Integer>();
        for (int col = 0; col < schema.getTable().getFieldsCount(); col++)
            fieldMap.put(schema.getTable().getFields(col).getFieldName(), col);

        var allData = rawData.stream().reduce(ByteString.EMPTY, ByteString::concat);
        var allDataStr = allData.toString(StandardCharsets.UTF_8);

        var genericTableType = new TypeReference<List<Map<String, Object>>>(){};

        try (var reader = new BufferedReader(new StringReader(allDataStr))) {

            var mapper = new ObjectMapper();
            var jsonData = mapper.readValue(reader, genericTableType);

            int row = 0;

            for (var jsonRow : jsonData) {

                for (var jsonField : jsonRow.entrySet()) {

                    var fieldName = jsonField.getKey();
                    var fieldIndex = fieldMap.get(fieldName);
                    var fieldType = result.fieldTypes.get(fieldIndex);

                    var jsonValue = jsonField.getValue();
                    var objValue = decodeJavaObject(fieldType, jsonValue);

                    result.values.get(fieldIndex).add(row, objValue);
                }

                row++;
            }

            return result;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private TestDataContainer schemaResult(SchemaDefinition schema) {

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

        return result;
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
                result.values.add(j, new Vector<>());

            while (reader.loadNextBatch()) {

                for (int j = 0; j < nCols; j++) {

                    var resultCol = result.values.get(j);
                    var arrowCol = rtBatch.getVector(j);

                    for (int i = 0; i < rtBatch.getRowCount(); i++) {
                        var arrowValue = arrowCol.getObject(i);
                        if (arrowValue instanceof Text)
                            resultCol.add(arrowValue.toString());
                        else if (arrowCol.getMinorType() == Types.MinorType.DATEDAY)
                            resultCol.add(LocalDate.ofEpochDay((int) arrowValue));
                        else
                            resultCol.add(arrowValue);
                    }
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

    private static Object decodeJavaObject(BasicType fieldType, Object rawObject) {

        switch (fieldType) {

            case BOOLEAN:

                if (rawObject instanceof Boolean)
                    return rawObject;

                if (rawObject instanceof String)
                    return Boolean.valueOf(rawObject.toString());

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

            case DECIMAL:

                if (rawObject instanceof BigDecimal) return rawObject;

                if (rawObject instanceof String)
                    return new BigDecimal(rawObject.toString());

                break;

            case STRING:

                return rawObject.toString();

            case DATE:

                if (rawObject instanceof LocalDate) return rawObject;

                if (rawObject instanceof String)
                    return LocalDate.parse(rawObject.toString());

                break;

            case DATETIME:

                if (rawObject instanceof LocalDateTime) return rawObject;

                if (rawObject instanceof String)
                    return LocalDateTime.parse(rawObject.toString(), MetadataCodec.ISO_DATETIME_NO_ZONE_FORMAT);

                break;

            default:

                System.out.println(fieldType);
                System.out.println(rawObject.getClass());

                throw new EUnexpected();
        }



        System.out.println(fieldType);
        System.out.println(rawObject.getClass());

        throw new EUnexpected();

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
