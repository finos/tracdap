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

package org.finos.tracdap.test.data;

import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.finos.tracdap.common.data.ArrowVsrContext;
import org.finos.tracdap.common.data.ArrowVsrSchema;

import org.junit.jupiter.api.Assertions;

import java.math.BigDecimal;


public class DataComparison {

    public static void compareSchemas(ArrowVsrSchema expected, ArrowVsrSchema actual) {

        if (expected.decoded() == null) {
            Assertions.assertNull(actual.decoded());
            Assertions.assertEquals(expected.physical(), actual.physical());
            return;
        }

        var nCols = expected.physical().getFields().size();

        for (int col = 0; col < nCols; col++) {

            var expectedField = expected.physical().getFields().get(col);
            var actualField = actual.physical().getFields().get(col);

            if (expectedField.getDictionary() == null) {

                var actualConcreteField = actual.decoded().getFields().get(col);

                Assertions.assertEquals(expectedField, actualField);
                Assertions.assertEquals(expectedField, actualConcreteField);
            }
            else {

                Assertions.assertEquals(expectedField.getName(), actualField.getName());
                Assertions.assertEquals(expectedField.getType(), actualField.getType());
                Assertions.assertEquals(expectedField.isNullable(), actualField.getFieldType().isNullable());
                Assertions.assertEquals(expectedField.getChildren(), actualField.getChildren());

                var expectedDictField = expected.decoded().getFields().get(col);
                var actualDictField = actual.decoded().getFields().get(col);

                Assertions.assertEquals(expectedDictField.getFieldType(), actualDictField.getFieldType());
                Assertions.assertEquals(expectedDictField.isNullable(), actualDictField.isNullable());
            }
        }
    }

    public static void compareBatches(ArrowVsrContext originalContext, ArrowVsrContext roundTripContext) {

        // Compare front (presenting) buffers of original and RT data

        var original = originalContext.getFrontBuffer();
        var roundTrip = roundTripContext.getFrontBuffer();

        // Data pipeline cleans up round trip root after the pipeline completes
        // To do this comparison, SingleBatchDataSink should convert root -> java array / maps

        Assertions.assertEquals(original.getRowCount(), roundTrip.getRowCount());

        for (var j = 0; j < original.getFieldVectors().size(); j++) {

            var field = original.getVector(j).getField();
            var originalVec = original.getVector(j);
            var rtVec = roundTrip.getVector(j);

            for (int i = 0; i < original.getRowCount(); i++) {

                var originalVal = getArrowValue(originalVec, i, originalContext.getDictionaries());
                var rtVal = getArrowValue(rtVec, i, roundTripContext.getDictionaries());

                // Decimals must be checked using compareTo, equals() does not handle equal values with different scale
                if (originalVal instanceof BigDecimal)
                    Assertions.assertEquals(0, ((BigDecimal) originalVal).compareTo((BigDecimal) rtVal), String.format("Mismatch in field [%s] row [%d]", field.getName(), i));
                else
                    Assertions.assertEquals(originalVal, rtVal, String.format("Mismatch in field [%s] row [%d]", field.getName(), i));
            }
        }
    }

    public static Object getArrowValue(FieldVector vector, int row, DictionaryProvider dictionaries) {

        if (vector.isNull(row))
            return null;

        if (vector.getField().getDictionary() != null) {
            var encoding = vector.getField().getDictionary();
            var dictionary = dictionaries.lookup(encoding.getId());
            var index = ((BaseIntVector) vector).getValueAsLong(row);
            return dictionary.getVector().getObject((int) index);
        }
        else {
            return vector.getObject(row);
        }
    }
}
