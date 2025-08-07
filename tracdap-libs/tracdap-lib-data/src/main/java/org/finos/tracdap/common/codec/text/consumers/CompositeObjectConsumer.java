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

package org.finos.tracdap.common.codec.text.consumers;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.finos.tracdap.common.exception.EDataCorruption;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class CompositeObjectConsumer {

    private final List<IJsonConsumer<?>> delegates;

    private final boolean useFieldNames;
    private final Map<String, Integer> fieldNameMap;
    private final boolean[] consumedFields;

    JsonToken token;
    private boolean active = false;
    private boolean delegateActive = false;
    private boolean gotValue = false;
    private int currentFieldIndex;

    public CompositeObjectConsumer(List<IJsonConsumer<?>> delegates, boolean isCaseSensitive) {

        this.delegates = delegates;

        useFieldNames = true;
        fieldNameMap = buildFieldNameMap(delegates, isCaseSensitive);

        consumedFields = new boolean[delegates.size()];
        currentFieldIndex = -1;
    }

    private static Map<String, Integer> buildFieldNameMap(List<IJsonConsumer<?>> delegates, boolean isCaseSensitive) {

        var casedMap = IntStream.range(0, delegates.size())
                .boxed()
                .collect(Collectors.toMap(
                        i -> delegates.get(i).getVector().getName(),
                        i -> i));

        if (isCaseSensitive)
            return casedMap;

        else {

            var uncasedMap = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
            uncasedMap.putAll(casedMap);

            return uncasedMap;
        }
    }

    public boolean consumeElement(JsonParser parser) throws IOException {

        if (!active) {

            token = parser.currentToken();
            active = true;

            if (token != JsonToken.START_OBJECT)
                throw new EDataCorruption("Unexpected token: " + parser.currentToken() + parser.currentLocation());
        }

        if (!delegateActive) {
            token = parser.nextValue();
        }

        while (token != null && token != JsonToken.NOT_AVAILABLE) {

            if (delegateActive) {

                var delegate = delegates.get(currentFieldIndex);

                if(delegate.consumeElement(parser)) {
                    consumedFields[currentFieldIndex] = true;
                    delegateActive = false;
                }
                else {
                    return false;
                }
            }
            else if (token.isScalarValue() || token.isStructStart() || token == JsonToken.START_ARRAY) {

                var fieldName = parser.currentName();
                var fieldIndex = fieldNameMap.get(fieldName);

                if (fieldIndex == null)
                    throw new EDataCorruption("Unknown field name: " + fieldName);

                currentFieldIndex = fieldIndex;

                var delegate = delegates.get(currentFieldIndex);

                if(delegate.consumeElement(parser)) {
                    consumedFields[currentFieldIndex] = true;
                }
                else {
                    delegateActive = true;
                    return false;
                }
            }
            else if (token == JsonToken.END_OBJECT) {
                gotValue = true;
                break;
            }
            else {
                throw new EDataCorruption("Unexpected token: " + token);
            }

            token = parser.nextValue();
        }

        if (!gotValue) {
            return false;
        }

        checkConsumedFields(parser);

        active = false;
        gotValue = false;
        currentFieldIndex = -1;
        resetConsumedFields();

        return true;
    }

    IJsonConsumer<?> getFieldDelegate(JsonParser parser) throws IOException {

        if (useFieldNames) {

            var fieldName = parser.currentName();
            var fieldIndex = fieldNameMap.get(fieldName);

            if (fieldIndex != null)
                return delegates.get(fieldIndex);
        }
        else {

            if (currentFieldIndex >= 0 && currentFieldIndex < delegates.size())
                return delegates.get(currentFieldIndex);
        }

        return null;
    }

    private void resetConsumedFields() {

        Arrays.fill(consumedFields, false);
    }

    private void checkConsumedFields(JsonParser parser) throws IOException {

        for (int i = 0; i < consumedFields.length; i++) {
            if (!consumedFields[i]) {

                var delegate = delegates.get(i);
                var field = delegate.getVector().getField();

                if (field.isNullable()) {
                    delegate.setNull();
                }
                else {
                    var msg = String.format("Invalid JSON table: Missing required field [%s]", field.getName());
                    throw new JsonParseException(parser, msg, parser.currentLocation());
                }
            }
        }
    }

    public void resetVectors(List<FieldVector> vectors) {

        if (active || delegateActive)
            throw new IllegalStateException("JSON consumer reset mid-value");

        for (int i = 0; i < vectors.size(); i++) {

            @SuppressWarnings("unchecked")
            var delegate = (IJsonConsumer<FieldVector>) delegates.get(i);
            var vector = vectors.get(i);

            resetDelegateVector(delegate, vector);
        }
    }

    private <TVector extends ValueVector> void resetDelegateVector(IJsonConsumer<TVector> delegate, TVector vector) {

        if (vector.getMinorType() != delegate.getVector().getMinorType())
            throw new IllegalArgumentException();

        delegate.resetVector(vector);
    }
}
