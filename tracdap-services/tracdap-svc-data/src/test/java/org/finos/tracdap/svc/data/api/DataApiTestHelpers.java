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

package org.finos.tracdap.svc.data.api;

import org.finos.tracdap.api.FileReadRequest;
import org.finos.tracdap.common.concurrent.IExecutionContext;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.concurrent.Flows;
import org.finos.tracdap.common.data.util.ByteSeekableChannel;
import org.finos.tracdap.test.grpc.GrpcTestStreams;
import org.finos.tracdap.metadata.BasicType;
import org.finos.tracdap.metadata.SchemaDefinition;
import org.finos.tracdap.metadata.TagHeader;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.Unpooled;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.Text;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;


class DataApiTestHelpers {

    static <TReq, TResp>
    void serverStreaming(
            BiConsumer<TReq, StreamObserver<TResp>> grpcMethod,
            TReq request, Flow.Subscriber<TResp> response) {

        var responseGrpc = GrpcTestStreams.clientResponseStream(response);
        grpcMethod.accept(request, responseGrpc);
    }

    static<TReq, TResp>
    CompletionStage<List<TResp>> serverStreaming(
            BiConsumer<TReq, StreamObserver<TResp>> grpcMethod,
            TReq request, IExecutionContext execCtx){

        var responseStream = Flows.<TResp>hub(execCtx);

        // Collect response messages into a list for direct inspection
        var collectList = Flows.fold(responseStream,
                (bs, b) -> {bs.add(b); return bs;},
                (List<TResp>) new ArrayList<TResp>());

        var responseGrpc = GrpcTestStreams.clientResponseStream(responseStream);
        grpcMethod.accept(request, responseGrpc);

        return collectList;
    }

    static <TReq, TResp>
    CompletionStage<Void> serverStreamingDiscard(
            BiConsumer<TReq, StreamObserver<TResp>> grpcMethod,
            TReq request, IExecutionContext execCtx) {

        // Server streaming response uses ByteString for binary data
        // ByteString does not need an explicit release

        var msgStream = Flows.<TResp>hub(execCtx);
        var discard = Flows.fold(msgStream, (acc, msg) -> acc, (Void) null);

        var grpcStream = GrpcTestStreams.clientResponseStream(msgStream);
        grpcMethod.accept(request, grpcStream);

        return discard;
    }

    static <TReq, TResp>
    CompletableFuture<TResp> clientStreaming(
            Function<StreamObserver<TResp>, StreamObserver<TReq>> grpcMethod,
            Flow.Publisher<TReq> requestPublisher) {

        var response = new CompletableFuture<TResp>();

        var responseGrpc = GrpcTestStreams.clientResponseHandler(response);
        var requestGrpc = grpcMethod.apply(responseGrpc);
        var requestSubscriber = GrpcTestStreams.clientRequestStream(requestGrpc);

        requestPublisher.subscribe(requestSubscriber);

        return response;
    }

    static <TReq, TResp>
    CompletableFuture<TResp> clientStreaming(
            Function<StreamObserver<TResp>, StreamObserver<TReq>> grpcMethod,
            TReq request) {

        return clientStreaming(grpcMethod, Flows.publish(Stream.of(request)));
    }

    static FileReadRequest readRequest(String tenant, TagHeader fileId) {

        var fileSelector = MetadataUtil.selectorFor(fileId);

        return FileReadRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(fileSelector)
                .build();
    }

    public static List<Vector<Object>> decodeArrowStream(SchemaDefinition schema, List<ByteString> data) {

        var allData = data.stream().reduce(ByteString.EMPTY, ByteString::concat);

        // This allocator is for decode only, data will not be fed back into Arrow framework

        try (var allocator = new RootAllocator();
             var stream = new ByteArrayInputStream(allData.toByteArray());
             var reader = new ArrowStreamReader(stream, allocator);
             var rtBatch = reader.getVectorSchemaRoot()) {

            var rtSchema = rtBatch.getSchema();
            var nCols = rtSchema.getFields().size();

            var result = new ArrayList<Vector<Object>>(nCols);
            for (var j = 0; j < nCols; j++)
                result.add(j, new Vector<>());

            while (reader.loadNextBatch()) {

                for (int j = 0; j < nCols; j++) {

                    var resultCol = result.get(j);
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

    public static List<Vector<Object>> decodeArrowFile(SchemaDefinition schema, List<ByteString> data) {

        var allData = data.stream().reduce(ByteString.EMPTY, ByteString::concat);

        try (var stream = new ByteSeekableChannel(Unpooled.wrappedBuffer(allData.toByteArray()))) {

            return decodeArrowFile(schema, stream);
        }
        catch (Exception e) {

            if (e instanceof RuntimeException)
                throw (RuntimeException) e;
            else
                throw new RuntimeException(e);
        }
    }

    public static List<Vector<Object>> decodeArrowFile(SchemaDefinition schema, SeekableByteChannel channel) {

        // This allocator is for decode only, data will not be fed back into Arrow framework

        try (var allocator = new RootAllocator();
             var reader = new ArrowFileReader(channel, allocator);
             var rtBatch = reader.getVectorSchemaRoot()) {

            var rtSchema = rtBatch.getSchema();
            var nCols = rtSchema.getFields().size();

            var result = new ArrayList<Vector<Object>>(nCols);
            for (var j = 0; j < nCols; j++)
                result.add(j, new Vector<>());

            while (reader.loadNextBatch()) {

                for (int j = 0; j < nCols; j++) {

                    var resultCol = result.get(j);
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

    public static List<Vector<Object>> decodeCsv(SchemaDefinition schema, List<ByteString> data) {

        var nCols = schema.getTable().getFieldsList().size();
        var result = new ArrayList<Vector<Object>>(nCols);
        for (var i = 0; i < nCols; i++)
            result.add(i, new Vector<>());

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

                int row = 0;

                while (itr.hasNextValue()) {

                    var csvValues = (Object[]) itr.nextValue();

                    for (int col = 0; col < nCols; col++) {

                        var fieldType = schema.getTable().getFields(col).getFieldType();
                        var csvValue = csvValues[col];
                        var vector = result.get(col);

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

    public static List<Vector<Object>> decodeJson(SchemaDefinition schema, List<ByteString> rawData) {

        var nCols = schema.getTable().getFieldsList().size();
        var result = new ArrayList<Vector<Object>>(nCols);
        for (var i = 0; i < nCols; i++)
            result.add(i, new Vector<>());

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
                    var fieldType = schema.getTable().getFields(fieldIndex).getFieldType();

                    var jsonValue = jsonField.getValue();
                    var objValue = decodeJavaObject(fieldType, jsonValue);

                    result.get(fieldIndex).add(row, objValue);
                }

                row++;
            }

            return result;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object decodeJavaObject(BasicType fieldType, Object rawObject) {

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
                    return LocalDateTime.parse(rawObject.toString(), MetadataCodec.ISO_DATETIME_INPUT_NO_ZONE_FORMAT);

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
}
