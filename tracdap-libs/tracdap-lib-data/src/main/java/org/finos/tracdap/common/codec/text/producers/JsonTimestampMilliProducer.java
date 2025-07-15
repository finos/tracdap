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
import org.apache.arrow.vector.TimeStampMilliVector;
import org.finos.tracdap.common.metadata.MetadataCodec;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class JsonTimestampMilliProducer extends BaseJsonProducer<TimeStampMilliVector> {

    public JsonTimestampMilliProducer(TimeStampMilliVector vector) {
        super(vector);
    }

    @Override
    public void produceElement(JsonGenerator generator) throws IOException {

        long epochMillis = vector.get(currentIndex);
        long epochSeconds = epochMillis / 1000;
        int nanos = (int) (epochMillis % 1000) * 1000000;

        if (epochSeconds < 0 && nanos != 0) {
            --epochSeconds;
            nanos = nanos + 1000000000;
        }

        LocalDateTime localDatetimeVal = LocalDateTime.ofEpochSecond(epochSeconds, nanos, ZoneOffset.UTC);
        OffsetDateTime offsetDatetimeVal = localDatetimeVal.atOffset(ZoneOffset.UTC);
        String datetimeStr = MetadataCodec.ISO_DATETIME_NO_ZONE_FORMAT.format(offsetDatetimeVal);

        generator.writeString(datetimeStr);

        currentIndex++;
    }
}
