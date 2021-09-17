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

package com.accenture.trac.svc.data.service;


import com.accenture.trac.api.MetadataWriteRequest;
import com.accenture.trac.api.TrustedMetadataApiGrpc;
import com.accenture.trac.common.util.Futures;
import com.accenture.trac.metadata.*;

import io.netty.buffer.ByteBuf;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.concurrent.Flow;
import java.util.function.Function;
import java.util.stream.Stream;


public class DataWriteService {
/*

    private final TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi;

    public DataWriteService() {

    }

    public void createData(

            // Optional<TagSelector>

            Flow.Publisher<ByteBuf> content
    ) {

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setObjectType(ObjectType.DATA)
                .build();

        var objectHeader = Futures.javaFuture(metaApi.preallocateId(preallocateRequest));

        var dataDef = objectHeader.thenApply(this::newDataDefinition);


    }

    public Flow.Processor<ByteBuf, Object> createFile() {

        var preallocateRequest = MetadataWriteRequest.newBuilder()
                .setObjectType(ObjectType.DATA)
                .build();

        var objectHeader = Futures.javaFuture(metaApi.preallocateId(preallocateRequest));

        var fileDef = objectHeader.thenApply(this::newFileDefinition);


    }



    public Flow.Publisher<ByteBuf> readFile() {

    }


    private DataDefinition newDataDefinition(TagHeader header) {

        return DataDefinition.newBuilder()
                .build();
    }

    private FileDefinition newFileDefinition(TagHeader header) {

        return FileDefinition.newBuilder()
                .build();
    }

    private <IR> Flow.Publisher<IR> toIR(Stream<ByteBuf> content) {

    }

    private <IR> Flow.Publisher<ByteBuf> fromIR(Stream<IR> ir) {

    }


    class DataContext<IR> {



    }


    class FileDataContext extends DataContext<ByteBuf> {


    }


    class TableDataContext extends DataContext<VectorSchemaRoot> {

    }


    // private IFileStorage.





*/



}
