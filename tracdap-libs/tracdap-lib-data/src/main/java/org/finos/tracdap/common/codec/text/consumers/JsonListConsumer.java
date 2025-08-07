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

    JsonToken token;
    private boolean active = false;
    private boolean delegateActive = false;
    private boolean gotValue = false;

    private int nItems = 0;
    private int totalItems = 0;

    public JsonListConsumer(ListVector vector, IJsonConsumer<?> delegate) {
        super(vector);
        this.delegate = delegate;
    }

    @Override
    public boolean consumeElement(JsonParser parser) throws IOException {

        if (!active) {

            token = parser.currentToken();
            active = true;

            if (token != JsonToken.START_ARRAY)
                throw new EDataCorruption("Unexpected token: " + parser.currentToken());

            vector.startNewValue(currentIndex);
        }

        if (!delegateActive) {
            token = parser.nextToken();
        }

        while (token != null && token != JsonToken.NOT_AVAILABLE) {

            if (delegateActive) {

                if (delegate.consumeElement(parser)) {
                    delegateActive = false;
                    nItems++;
                    totalItems++;
                }
                else {
                    return false;
                }
            }
            else if (token.isScalarValue() || token.isStructStart() || token == JsonToken.START_ARRAY) {

                ensureInnerVectorCapacity(totalItems + 1);

                if (delegate.consumeElement(parser)) {
                    nItems++;
                    totalItems++;
                }
                else {
                    delegateActive = true;
                    return false;
                }
            }
            else if (token == JsonToken.END_ARRAY) {
                gotValue = true;
                break;
            }
            else {
                throw new EDataCorruption("Unexpected token: " + token);
            }

            token = parser.nextToken();
        }

        if (!gotValue) {
            return false;
        }

        vector.endValue(currentIndex, nItems);
        currentIndex++;

        active = false;
        gotValue = false;
        nItems = 0;

        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void resetVector(ListVector vector) {

        if (active || delegateActive)
            throw new IllegalStateException("JSON consumer reset mid-value");

        nItems = 0;
        totalItems = 0;

        ((IJsonConsumer<FieldVector>) delegate).resetVector(vector.getDataVector());
        super.resetVector(vector);
    }

    void ensureInnerVectorCapacity(long targetCapacity) {
        while (vector.getDataVector().getValueCapacity() < targetCapacity) {
            vector.getDataVector().reAlloc();
        }
    }
}
