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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.finos.tracdap.common.codec.text.producers.IJsonProducer;
import org.finos.tracdap.common.exception.EDataCorruption;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JsonMapConsumer extends BaseJsonConsumer<MapVector> {

    private VarCharVector keyVector;
    private final IJsonConsumer<?> valueDelegate;

    private boolean gotFirstToken = false;
    private boolean gotLastToken = false;
    private boolean midValue = false;
    private int nItems = 0;
    private int totalItems = 0;

    public JsonMapConsumer(MapVector vector, VarCharVector keyVector, IJsonConsumer<?> valueDelegate) {
        super(vector);
        this.keyVector = keyVector;
        this.valueDelegate = valueDelegate;
    }

    @Override
    public boolean consumeElement(JsonParser parser) throws IOException {

        if (!gotFirstToken) {

            if (parser.currentToken() != JsonToken.START_OBJECT)
                throw new EDataCorruption("Unexpected token: " + parser.currentToken());

            vector.startNewValue(currentIndex);

            gotFirstToken = true;
            nItems = 0;

            parser.nextValue();
        }

        for (var token = parser.currentToken(); token != null && token != JsonToken.NOT_AVAILABLE; token = parser.nextValue()) {

            if (gotLastToken)
                throw new EDataCorruption("Unexpected token: " + token);

            if (midValue || token.isScalarValue() || token.isStructStart() || token == JsonToken.START_ARRAY) {

                if (!midValue) {
                    ensureInnerVectorCapacity(totalItems + 1);
                    var key = parser.currentName();
                    keyVector.setSafe(totalItems, key.getBytes(StandardCharsets.UTF_8));
                }

                if(valueDelegate.consumeElement(parser)) {
                    ((StructVector)vector.getDataVector()).setIndexDefined(totalItems);
                    midValue =false;
                    nItems++;
                    totalItems++;
                }
                else {
                    midValue = true;
                    return false;
                }
            }

            else if (token == JsonToken.END_OBJECT) {
                gotLastToken = true;
                break;
            }

            else {
                throw new EDataCorruption("Unexpected token: " + token);
            }
        }

        if (gotLastToken) {

            vector.endValue(currentIndex, nItems);
            currentIndex++;

            gotFirstToken = false;
            gotLastToken = false;
            midValue = false;
            nItems = 0;

            return true;
        }
        else {

            return false;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void resetVector(MapVector vector) {

        var entryVector = (StructVector) vector.getDataVector();
        var keyVector = (VarCharVector) entryVector.getChildrenFromFields().get(0);
        var valueVector = entryVector.getChildrenFromFields().get(1);

        this.keyVector = keyVector;
        ((IJsonProducer<FieldVector>) valueDelegate).resetVector(valueVector);

        super.resetVector(vector);
    }

    void ensureInnerVectorCapacity(long targetCapacity) {
        StructVector innerVector = (StructVector) vector.getDataVector();
        for (FieldVector v : innerVector.getChildrenFromFields()) {
            while (v.getValueCapacity() < targetCapacity) {
                v.reAlloc();
            }
        }
    }
}
