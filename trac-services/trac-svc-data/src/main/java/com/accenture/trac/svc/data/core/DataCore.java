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

package com.accenture.trac.svc.data.core;

import com.accenture.trac.api.DataWriteRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.arrow.flatbuf.RecordBatch;

import java.util.concurrent.Flow;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataCore {


    public interface DataShape<IR> {

    }

    public class TableShape implements DataShape<RecordBatch> {

    }


    public abstract class FormatReader<TBytes, IR> implements Flow.Processor<TBytes, IR> {

        @Override
        public void subscribe(Flow.Subscriber<? super IR> subscriber) {

        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {

        }

        @Override
        public void onNext(TBytes content) {

        }

        @Override
        public void onComplete() {

        }

        @Override
        public void onError(Throwable throwable) {

        }
    }

    public abstract class FormatWriter<TBytes, IR> implements Flow.Processor<IR, TBytes> {

        @Override
        public void subscribe(Flow.Subscriber<? super TBytes> subscriber) {

        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {

        }

        @Override
        public void onNext(IR batch) {

        }

        @Override
        public void onComplete() {

        }

        @Override
        public void onError(Throwable throwable) {

        }
    }

    public interface FileStorage {

    }











    public interface FormatReader<DataShape, IR> {

        Stream<IR> prepare();

        void prepareSchema();

        void acceptContent(ByteString content);

        void onError();

        void onComplete();
    }


    public class CsvFormatReader implements FormatReader<TableShape, RecordBatch> {

        @Override
        public Stream<RecordBatch> prepare() {


            Stream.of(new RecordBatch()).

            return null;
        }

        @Override
        public void prepareSchema() {

        }

        @Override
        public void acceptContent(ByteString content) {

        }

        @Override
        public void onError() {

        }

        @Override
        public void onComplete() {

        }
    }



    public class DataUploadHandler implements StreamObserver<DataWriteRequest> {

        void setup() {



        }

        @Override
        public void onNext(DataWriteRequest value) {


            var backingBuffer = value.getContent().asReadOnlyByteBufferList();
            var byteBuffers = backingBuffer.stream().map(Unpooled::wrappedBuffer).collect(Collectors.toList());

            var allocator = byteBuffers.get(0).alloc();
            var byteBuf = allocator.compositeBuffer().addComponents(byteBuffers);

        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {

        }
    }
}
