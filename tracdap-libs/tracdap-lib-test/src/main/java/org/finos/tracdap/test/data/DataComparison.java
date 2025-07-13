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
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.finos.tracdap.common.data.ArrowVsrContext;
import org.finos.tracdap.common.data.ArrowVsrSchema;

import org.junit.jupiter.api.Assertions;


public class DataComparison {

    public static void compareSchemas(ArrowVsrSchema expected, ArrowVsrSchema actual) {

        if (expected.logical() == null) {
            Assertions.assertNull(actual.logical());
            Assertions.assertEquals(expected.physical(), actual.physical());
            return;
        }

        var nCols = expected.physical().getFields().size();

        for (int col = 0; col < nCols; col++) {

            var expectedField = expected.physical().getFields().get(col);
            var actualField = actual.physical().getFields().get(col);

            var expectedConcreteField = expected.physical().getFields().get(col);
            var actualConcreteField = actual.physical().getFields().get(col);

            compareFields(expectedField, actualField, expectedConcreteField, actualConcreteField);
        }
    }

    public static void compareFields(Field expectedField, Field actualField, Field expectedConcreteField, Field actualConcreteField) {

        if (expectedField.getChildren() != null && !expectedField.getChildren().isEmpty()) {

            Assertions.assertEquals(expectedField.getName(), actualField.getName());
            Assertions.assertEquals(expectedField.getType(), actualField.getType());

            var expectedChildren = expectedField.getChildren();
            var actualChildren = actualField.getChildren();

            Assertions.assertEquals(expectedChildren.size(), actualChildren.size());

            for (int i = 0; i < expectedChildren.size(); i++) {

                var expectedChild = expectedChildren.get(i);
                var actualChild = actualChildren.get(i);
                var expectedConcreteChild = expectedConcreteField.getChildren().get(i);
                var actualConcreteChild = actualConcreteField.getChildren().get(i);

                compareFields(expectedChild, actualChild, expectedConcreteChild, actualConcreteChild);
            }

            return;
        }

        if (expectedField.getDictionary() == null) {

            Assertions.assertEquals(expectedField, actualField);
            Assertions.assertEquals(expectedField, actualConcreteField);
        }
        else {

            Assertions.assertEquals(expectedField.getName(), actualField.getName());
            Assertions.assertEquals(expectedField.getType(), actualField.getType());
            Assertions.assertEquals(expectedField.isNullable(), actualField.getFieldType().isNullable());
            Assertions.assertEquals(expectedField.getChildren(), actualField.getChildren());
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

            // Create a visitor that will do the comparison
            RangeEqualsVisitor visitor = new RangeEqualsVisitor(originalVec, rtVec);

            Assertions.assertTrue(
                    visitor.rangeEquals(new Range(0, 0, originalVec.getValueCount())),
                    "Vectors not equal for field " + field.getName());
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
