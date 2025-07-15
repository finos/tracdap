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
import org.apache.arrow.vector.UInt4Vector;
import org.finos.tracdap.common.exception.EDataCorruption;

import java.io.IOException;


public class JsonUInt4Consumer extends BaseJsonConsumer<UInt4Vector> {

    public JsonUInt4Consumer(UInt4Vector vector) {
        super(vector);
    }

    @Override
    public boolean consumeElement(JsonParser parser) throws IOException {

        long value = parser.getLongValue();

        if (value < 0 || value > 0xFFFFFFFFL) {
            throw new EDataCorruption("Value out of range for UINT4: " + value);
        }

        vector.set(currentIndex++, (int) value);
        return true;
    }
}
