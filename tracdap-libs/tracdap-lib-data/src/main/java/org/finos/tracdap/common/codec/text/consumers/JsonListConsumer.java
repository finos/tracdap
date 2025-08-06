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

import org.finos.tracdap.common.exception.EDataCorruption;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;

import java.io.IOException;


public class JsonListConsumer extends BaseJsonConsumer<ListVector> {

    private final IJsonConsumer<?> delegate;

    private boolean gotFirstToken = false;
    private boolean gotLastToken = false;
    private boolean midValue = false;
    private int nItems = 0;
    private int totalItems = 0;

    public JsonListConsumer(ListVector vector, IJsonConsumer<?> delegate) {
        super(vector);
        this.delegate = delegate;
    }

    @Override
    public boolean consumeElement(JsonParser parser) throws IOException {

        if (!gotFirstToken) {

            if (parser.currentToken() != JsonToken.START_ARRAY)
                throw new EDataCorruption("Unexpected token: " + parser.currentToken());

            vector.startNewValue(currentIndex);

            gotFirstToken = true;
            nItems = 0;

            parser.nextToken();
        }

        for (var token = parser.currentToken(); token != null && token != JsonToken.NOT_AVAILABLE; token = parser.nextToken()) {

            if (gotLastToken)
                throw new EDataCorruption("Unexpected token: " + token);

            if (midValue || token.isScalarValue() || token.isStructStart() || token == JsonToken.START_ARRAY) {

                if (!midValue)
                    ensureInnerVectorCapacity(totalItems + 1);

                if(delegate.consumeElement(parser)) {
                    midValue =false;
                    nItems++;
                    totalItems++;
                }
                else {
                    midValue = true;
                    return false;
                }
            }

            else if (token == JsonToken.END_ARRAY) {
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
    public void resetVector(ListVector vector) {
        ((IJsonConsumer<FieldVector>) delegate).resetVector(vector.getDataVector());
        super.resetVector(vector);
    }

    void ensureInnerVectorCapacity(long targetCapacity) {
        while (vector.getDataVector().getValueCapacity() < targetCapacity) {
            vector.getDataVector().reAlloc();
        }
    }
}
