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
import org.apache.arrow.vector.DateDayVector;

import java.io.IOException;
import java.time.LocalDate;


public class JsonDateDayConsumer extends BaseJsonConsumer<DateDayVector> {

    public JsonDateDayConsumer(DateDayVector vector) {
        super(vector);
    }

    @Override
    public boolean consumeElement(JsonParser parser) throws IOException {

        LocalDate dateVal = JsonParsing.parseLocalDate(parser);
        long unixEpochDay = dateVal.toEpochDay();

        if (unixEpochDay < Integer.MIN_VALUE || unixEpochDay > Integer.MAX_VALUE)
            throw new IllegalArgumentException();  // todo

        vector.set(currentIndex++, (int) unixEpochDay);

        return true;
    }
}
