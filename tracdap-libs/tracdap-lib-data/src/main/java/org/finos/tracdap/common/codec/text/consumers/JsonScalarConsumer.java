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
import org.finos.tracdap.common.exception.EUnexpected;

import java.io.IOException;


public class JsonScalarConsumer<TVector extends FieldVector> extends BaseJsonConsumer<TVector> {

    private final IJsonConsumer<TVector> delegate;

    public JsonScalarConsumer(IJsonConsumer<TVector> delegate) {
        super(delegate.getVector());
        this.delegate = delegate;
    }

    @Override
    public boolean consumeElement(JsonParser parser) throws IOException {

        if (parser.currentToken() == null || parser.currentToken() == JsonToken.NOT_AVAILABLE)
            return false;

        if (parser.currentToken() == JsonToken.VALUE_NULL) {

            if (vector.getField().isNullable()) {
                delegate.setNull();
                return true;
            }
            else {
                throw new JsonParseException(parser, "Invalid input: (null not allowed)");
            }
        }

        return delegate.consumeElement(parser);
    }

    @Override
    public void setNull() {
        delegate.setNull();
        currentIndex++;
    }

    @Override
    public void resetVector(TVector vector) {
        delegate.resetVector(vector);
        super.resetVector(vector);
    }
}
