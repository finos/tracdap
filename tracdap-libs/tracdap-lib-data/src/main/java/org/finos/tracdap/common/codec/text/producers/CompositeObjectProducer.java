/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.codec.text.producers;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;


public class CompositeObjectProducer extends BaseCompositeProducer {

    private final List<String> fieldNames;

    public CompositeObjectProducer(List<IJsonProducer<?>> delegates) {

        super(delegates);

        fieldNames = delegates.stream()
                .map(IJsonProducer::getVector)
                .map(ValueVector::getField)
                .map(Field::getName)
                .collect(Collectors.toList());
    }

    public void produceElement(JsonGenerator generator) throws IOException {

        generator.writeStartObject();

        for (int i = 0; i < fieldNames.size(); i++) {

            generator.writeFieldName(fieldNames.get(i));
            delegates.get(i).produceElement(generator);
        }

        generator.writeEndObject();
    }
}
