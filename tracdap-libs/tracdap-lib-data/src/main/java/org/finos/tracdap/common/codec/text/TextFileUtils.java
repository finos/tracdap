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

package org.finos.tracdap.common.codec.text;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.finos.tracdap.common.codec.text.consumers.*;
import org.finos.tracdap.common.codec.text.producers.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class TextFileUtils {

    public static IBatchProducer createBatchProducer(
            VectorSchemaRoot batch,
            DictionaryProvider dictionaries,
            boolean singleRecord) {

        var fieldProducers = createProducers(batch.getFieldVectors(), dictionaries);
        var recordProducer = new CompositeObjectProducer(fieldProducers);

        if (singleRecord)
            return new SingleRecordProducer(recordProducer);
        else
            return new BatchProducer(recordProducer);
    }

    public static List<IJsonProducer<?>> createProducers(List<FieldVector> vectors, DictionaryProvider dictionaries) {

        return vectors.stream()
                .map(v -> createProducer(v, v.getField().isNullable(), dictionaries))
                .collect(Collectors.toList());
    }

    public static IJsonProducer<?>
    createProducer(FieldVector vector, boolean nullable, DictionaryProvider dictionaries) {

        // Wrap nullable fields with an outer producer to handle nulls
        if (nullable && vector.getField().isNullable()) {
            var innerProducer = createProducer(vector, /* nullable = */ false, dictionaries);
            return new JsonNullableProducer<>(innerProducer);
        }

        // Wrap dictionary encoded fields with a producer that decodes dictionary values
        // No text format (currently) supports dictionaries, so they are always decoded
        if (vector.getField().getDictionary() != null) {

            if (dictionaries == null) {
                var message = "Field references a dictionary but no dictionaries were provided: %s";
                var error = String.format(message, vector.getField().getName());
                throw new IllegalArgumentException(error);
            }

            var dictionary = dictionaries.lookup(vector.getField().getDictionary().getId());

            if (dictionary == null) {
                var message = "Field references a dictionary that does not exist: %s, dictionary ID = %d";
                var error = String.format(message, vector.getField().getName(), vector.getField().getDictionary().getId());
                throw new IllegalArgumentException(error);
            }

            var innerNullable = vector.getField().isNullable();
            var innerProducer = createProducer(dictionary.getVector(), innerNullable, /* dictionaries = */ null);
            return new JsonDictionaryProducer((BaseIntVector) vector, innerProducer);
        }

        // Create a producer for the specific vector type
        switch (vector.getMinorType()) {

            // Null type

            case NULL:
                return new JsonNullProducer((NullVector) vector);

            // Numeric types

            case BIT:
                return new JsonBitProducer((BitVector) vector);
            case TINYINT:
                return new JsonTinyIntProducer((TinyIntVector) vector);
            case SMALLINT:
                return new JsonSmallIntProducer((SmallIntVector) vector);
            case INT:
                return new JsonIntProducer((IntVector) vector);
            case BIGINT:
                return new JsonBigIntProducer((BigIntVector) vector);
            case UINT1:
                return new JsonUInt1Producer((UInt1Vector) vector);
            case UINT2:
                return new JsonUInt2Producer((UInt2Vector) vector);
            case UINT4:
                return new JsonUInt4Producer((UInt4Vector) vector);
            case UINT8:
                return new JsonUInt8Producer((UInt8Vector) vector);
            case FLOAT2:
                return new JsonFloat2Producer((Float2Vector) vector);
            case FLOAT4:
                return new JsonFloat4Producer((Float4Vector) vector);
            case FLOAT8:
                return new JsonFloat8Producer((Float8Vector) vector);
            case DECIMAL:
                return new JsonDecimalProducer((DecimalVector) vector);
            case DECIMAL256:
                return new JsonDecimal256Producer((Decimal256Vector) vector);

            // Text / binary types

            case VARCHAR:
                return new JsonVarCharProducer((VarCharVector) vector);

            // Temporal types

            case DATEDAY:
                return new JsonDateDayProducer((DateDayVector) vector);
            case TIMESTAMPMILLI:
                return new JsonTimestampMilliProducer((TimeStampMilliVector) vector);

            // Composite types

            case LIST:
                var listVector = (ListVector) vector;
                var itemVector = listVector.getDataVector();
                var itemProducer = createProducer(itemVector, itemVector.getField().isNullable(), dictionaries);
                return new JsonListProducer(listVector, itemProducer);

            case FIXED_SIZE_LIST:
                var fixedListVector = (FixedSizeListVector) vector;
                var fixedItemVector = fixedListVector.getDataVector();
                var fixedItemProducer = createProducer(fixedItemVector, fixedItemVector.getField().isNullable(), dictionaries);
                return new JsonFixedSizeListProducer(fixedListVector, fixedItemProducer);

            case MAP:
                var mapVector = (MapVector) vector;
                var entryVector = (StructVector) mapVector.getDataVector();
                var keyType = entryVector.getChildrenFromFields().get(0).getMinorType();
                if (keyType != Types.MinorType.VARCHAR) {
                    throw new IllegalArgumentException("MAP key type must be VARCHAR for text encoding");
                }
                var keyVector = (VarCharVector) entryVector.getChildrenFromFields().get(0);
                var valueVector = entryVector.getChildrenFromFields().get(1);
                var valueProducer = createProducer(valueVector, valueVector.getField().isNullable(), dictionaries);
                return new JsonMapProducer(mapVector, keyVector, valueProducer);

            case STRUCT:
                var structVector = (StructVector) vector;
                var childVectors = structVector.getChildrenFromFields();
                var childProducers = new ArrayList<IJsonProducer<?>>(childVectors.size());
                for (FieldVector childVector : childVectors) {
                    var childProducer = createProducer(childVector, childVector.getField().isNullable(), dictionaries);
                    childProducers.add(childProducer);
                }
                return new JsonStructProducer(structVector, childProducers);

            // Support for UNION and DENSEUNION is not currently available
            // This is pending fixes in the implementation of the union vectors themselves
            // https://github.com/apache/arrow-java/issues/108

            default:

                // Not all Arrow types are supported for encoding (yet)!
                var error = String.format(
                        "Encoding Arrow type %s is not currently supported",
                        vector.getMinorType().name());

                throw new UnsupportedOperationException(error);
        }
    }

    public static IBatchConsumer createBatchConsumer(
            VectorSchemaRoot root,
            Map<Long, Field> dictionaryFields,
            DictionaryProvider dictionaries,
            List<DictionaryStagingConsumer<?>> staging,
            TextFileConfig config) {

        var fieldConsumers = createConsumers(root.getFieldVectors(), dictionaryFields, dictionaries, staging);
        var recordConsumer = new CompositeObjectConsumer(fieldConsumers, /* caseSensitive = */ true);

        if (config.isSingleRecord())
            return new SingleRecordConsumer(recordConsumer, staging, root);
        else
            return new BatchConsumer(recordConsumer, staging, root, config.getBatchSize());
    }

    public static List<IJsonConsumer<?>> createConsumers(
            List<FieldVector> vectors,
            Map<Long, Field> dictionaryFields,
            DictionaryProvider dictionaries,
            List<DictionaryStagingConsumer<?>> staging) {

        return vectors.stream()
                .map(vector -> createConsumer(vector, dictionaryFields, dictionaries, staging))
                .collect(Collectors.toList());
    }

    public static IJsonConsumer<?>
    createConsumer(
            FieldVector vector,
            Map<Long, Field> dictionaryFields,
            DictionaryProvider dictionaries,
            List<DictionaryStagingConsumer<?>> staging) {

        var encoding = vector.getField().getDictionary();

        // If this field is dictionary encoded, set up a staging pair
        if (encoding != null) {

            if (dictionaryFields == null) {
                var message = String.format(
                        "Missing type information for dictionary-encoded field [%s]",
                        vector.getField().getName());
                throw new IllegalArgumentException(message);
            }

            var dictionaryField = dictionaryFields.get(encoding.getId());

            if (dictionaryField == null) {
                var message = String.format(
                        "Missing type information for dictionary-encoded field [%s]",
                        vector.getField().getName());
                throw new IllegalArgumentException(message);
            }

            Field stagingField;

            // Dictionary vector may have different nullability from the main vector
            if (vector.getField().isNullable() != dictionaryField.isNullable()) {
                var stagingFieldType = new FieldType(vector.getField().isNullable(), dictionaryField.getType(), null);
                stagingField = new Field(dictionaryField.getName(), stagingFieldType, dictionaryField.getChildren());
            }
            else {
                stagingField = dictionaryField;
            }

            var targetVector = (BaseIntVector) vector;
            var dictionary = dictionaries.lookup(encoding.getId());
            var dynamic = false;

            if (dictionary == null || dictionary.getVector().getValueCount() == 0) {
                var dictionaryVector = dictionaryField.createVector(vector.getAllocator());
                dictionaryVector.allocateNew();
                dictionary = new Dictionary(dictionaryVector, encoding);
                dynamic = true;
            }

            var stagingVector = stagingField.createVector(vector.getAllocator());
            stagingVector.setInitialCapacity(vector.getValueCapacity());

            @SuppressWarnings("unchecked")
            var stagingConsumer = (IJsonConsumer<ElementAddressableVector>) createConsumer(stagingVector, null, null, null);
            var dictionaryConsumer = new DictionaryStagingConsumer<>(targetVector, stagingConsumer, dictionary, dynamic);

            staging.add(dictionaryConsumer);

            return dictionaryConsumer;
        }

        // Create a producer for the specific vector type
        switch (vector.getMinorType()) {

            // Null type

            case NULL:
                break;

            // Numeric types

            case BIT:
                return new JsonScalarConsumer<>(new JsonBitConsumer((BitVector) vector));
            case TINYINT:
                return new JsonScalarConsumer<>(new JsonTinyIntConsumer((TinyIntVector) vector));
            case SMALLINT:
                return new JsonScalarConsumer<>(new JsonSmallIntConsumer((SmallIntVector) vector));
            case INT:
                return new JsonScalarConsumer<>(new JsonIntConsumer((IntVector) vector));
            case BIGINT:
                return new JsonScalarConsumer<>(new JsonBigIntConsumer((BigIntVector) vector));
            case UINT1:
                return new JsonScalarConsumer<>(new JsonUInt1Consumer((UInt1Vector) vector));
            case UINT2:
                return new JsonScalarConsumer<>(new JsonUInt2Consumer((UInt2Vector) vector));
            case UINT4:
                return new JsonScalarConsumer<>(new JsonUInt4Consumer((UInt4Vector) vector));
            case UINT8:
                return new JsonScalarConsumer<>(new JsonUInt8Consumer((UInt8Vector) vector));
            case FLOAT2:
                return new JsonScalarConsumer<>(new JsonFloat2Consumer((Float2Vector) vector));
            case FLOAT4:
                return new JsonScalarConsumer<>(new JsonFloat4Consumer((Float4Vector) vector));
            case FLOAT8:
                return new JsonScalarConsumer<>(new JsonFloat8Consumer((Float8Vector) vector));
            case DECIMAL:
                return new JsonScalarConsumer<>(new JsonDecimalConsumer((DecimalVector) vector));
            case DECIMAL256:
                return new JsonScalarConsumer<>(new JsonDecimal256Consumer((Decimal256Vector) vector));

            // Text / binary types

            case VARCHAR:
                return new JsonScalarConsumer<>(new JsonVarCharConsumer((VarCharVector) vector));

            // Temporal types

            case DATEDAY:
                return new JsonScalarConsumer<>(new JsonDateDayConsumer((DateDayVector) vector));
            case TIMESTAMPMILLI:
                return new JsonScalarConsumer<>(new JsonTimestampMilliConsumer((TimeStampMilliVector) vector));

            // Composite types

            case LIST:
                var listVector = (ListVector) vector;
                var itemVector = listVector.getDataVector();
                var itemConsumer = createConsumer(itemVector, dictionaryFields, dictionaries, staging);
                var listConsumer = new JsonListConsumer(listVector, itemConsumer);
                return new JsonScalarConsumer<>(listConsumer);

            // TODO: Fixed size list

            case MAP:
                var mapVector = (MapVector) vector;
                var entryVector = (StructVector) mapVector.getDataVector();
                var keyType = entryVector.getChildrenFromFields().get(0).getMinorType();
                if (keyType != Types.MinorType.VARCHAR) {
                    throw new IllegalArgumentException("MAP key type must be VARCHAR for text decoding");
                }
                var keyVector = (VarCharVector) entryVector.getChildrenFromFields().get(0);
                var valueVector = entryVector.getChildrenFromFields().get(1);
                var valueConsumer = createConsumer(valueVector, dictionaryFields, dictionaries, staging);
                var mapConsumer = new JsonMapConsumer(mapVector, keyVector, valueConsumer);
                return new JsonScalarConsumer<>(mapConsumer);

            case STRUCT:
                var structVector = (StructVector) vector;
                var childVectors = structVector.getChildrenFromFields();
                var childConsumers = new ArrayList<IJsonConsumer<?>>(childVectors.size());
                for (FieldVector childVector : childVectors) {
                    var childConsumer = createConsumer(childVector, dictionaryFields, dictionaries, staging);
                    childConsumers.add(childConsumer);
                }
                var structConsumer = new JsonStructConsumer(structVector, childConsumers);
                return new JsonScalarConsumer<>(structConsumer);

        }

        throw new RuntimeException("Unsupported vector type: " + vector.getMinorType());

    }
}
