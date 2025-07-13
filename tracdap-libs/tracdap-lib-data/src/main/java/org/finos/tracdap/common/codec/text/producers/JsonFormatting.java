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
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;

import java.io.IOException;
import java.util.List;

public class JsonFormatting {

    // Standard NaN / infinity values are what gets quoted when encoding data
    // For consistency, these values match the output of the Apache Arrow CSV implementation
    static final String STANDARD_NAN = "nan";
    static final String STANDARD_POSITIVE_INFINITY = "inf";
    static final String STANDARD_NEGATIVE_INFINITY = "-inf";

    // Values that are recognised as NaN / infinity during decoding
    static final List<String> NAN_VALUES = List.of("nan", "na");
    static final List<String> INFINITY_VALUES = List.of("inf", "infinity");

    static void quoteNanAsString(JsonGenerator generator, String nanValue) throws IOException {

        // Special handling for output of NaN values (NaN, +Infinity, -Infinity)

        // In JSON, NaN values are not valid numbers so need to be quoted as strings

        // In CSV NaN also looks like a string
        // The Apache Arrow CSV implementation only works with quoted strings for NaN
        // So, switch on quoting for NaN fields to match the Arrow implementation
        // This allows the runtime to use the Arrow (C++) CSV parser instead of the lenient (Python) fallback

        CsvGenerator csvGenerator = generator instanceof CsvGenerator
                ? (CsvGenerator) generator
                : null;

        boolean switchQuoting = ! CsvGenerator.Feature
                .ALWAYS_QUOTE_STRINGS
                .enabledIn(generator.getFormatFeatures());

        if (csvGenerator != null && switchQuoting) {
            csvGenerator.enable(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS);
        }

        generator.writeString(nanValue);

        if (csvGenerator != null && switchQuoting) {
            csvGenerator.disable(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS);
        }
    }
}
