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
import org.apache.arrow.vector.Float2Vector;

import java.io.IOException;

public class JsonFloat2Producer extends BaseJsonProducer<Float2Vector> {

    public JsonFloat2Producer(Float2Vector vector) {
        super(vector);
    }

    @Override
    public void produceElement(JsonGenerator generator) throws IOException {

        float value = vector.getValueAsFloat(currentIndex++);

        if (Double.isNaN(value))
            JsonFormatting.quoteNanAsString(generator, JsonFormatting.STANDARD_NAN);
        else if (Double.isInfinite(value)) {
            if (value > 0)
                JsonFormatting.quoteNanAsString(generator, JsonFormatting.STANDARD_POSITIVE_INFINITY);
            else
                JsonFormatting.quoteNanAsString(generator, JsonFormatting.STANDARD_NEGATIVE_INFINITY);
        }
        else
            generator.writeNumber(value);
    }
}
