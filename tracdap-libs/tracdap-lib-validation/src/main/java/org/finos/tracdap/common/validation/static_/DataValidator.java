/*
 * Copyright 2022 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.metadata.PartKeys;
import org.finos.tracdap.common.validation.ValidationConstants;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.Map;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class DataValidator {

    private static final Descriptors.Descriptor DATA_DEFINITION;
    private static final Descriptors.OneofDescriptor DD_SCHEMA_SPECIFIER;
    private static final Descriptors.FieldDescriptor DD_SCHEMA_ID;
    private static final Descriptors.FieldDescriptor DD_SCHEMA;
    private static final Descriptors.FieldDescriptor DD_PARTS;
    private static final Descriptors.FieldDescriptor DD_STORAGE_ID;

    private static final Descriptors.Descriptor PART_KEY;
    private static final Descriptors.FieldDescriptor PK_OPAQUE_KEY;
    private static final Descriptors.FieldDescriptor PK_PART_TYPE;
    private static final Descriptors.FieldDescriptor PK_PART_VALUES;
    private static final Descriptors.FieldDescriptor PK_PART_RANGE_MIN;
    private static final Descriptors.FieldDescriptor PK_PART_RANGE_MAX;

    private static final Descriptors.Descriptor DATA_PART;
    private static final Descriptors.FieldDescriptor DP_PART_KEY;
    private static final Descriptors.FieldDescriptor DP_SNAP;

    private static final Descriptors.Descriptor DATA_SNAP;
    private static final Descriptors.FieldDescriptor DS_SNAP_INDEX;
    private static final Descriptors.FieldDescriptor DS_DELTAS;

    private static final Descriptors.Descriptor DATA_DELTA;
    private static final Descriptors.FieldDescriptor DD_DELTA_INDEX;
    private static final Descriptors.FieldDescriptor DD_DATA_ITEM;

    static {

        DATA_DEFINITION = DataDefinition.getDescriptor();
        DD_SCHEMA_SPECIFIER = field(DATA_DEFINITION, DataDefinition.SCHEMAID_FIELD_NUMBER).getContainingOneof();
        DD_SCHEMA_ID = field(DATA_DEFINITION, DataDefinition.SCHEMAID_FIELD_NUMBER);
        DD_SCHEMA = field(DATA_DEFINITION, DataDefinition.SCHEMA_FIELD_NUMBER);
        DD_PARTS = field(DATA_DEFINITION, DataDefinition.PARTS_FIELD_NUMBER);
        DD_STORAGE_ID = field(DATA_DEFINITION, DataDefinition.STORAGEID_FIELD_NUMBER);

        PART_KEY = PartKey.getDescriptor();
        PK_OPAQUE_KEY = field(PART_KEY, PartKey.OPAQUEKEY_FIELD_NUMBER);
        PK_PART_TYPE = field(PART_KEY, PartKey.PARTTYPE_FIELD_NUMBER);
        PK_PART_VALUES = field(PART_KEY, PartKey.PARTVALUES_FIELD_NUMBER);
        PK_PART_RANGE_MIN = field(PART_KEY, PartKey.PARTRANGEMIN_FIELD_NUMBER);
        PK_PART_RANGE_MAX = field(PART_KEY, PartKey.PARTRANGEMAX_FIELD_NUMBER);

        DATA_PART = DataDefinition.Part.getDescriptor();
        DP_PART_KEY = field(DATA_PART, DataDefinition.Part.PARTKEY_FIELD_NUMBER);
        DP_SNAP = field(DATA_PART, DataDefinition.Part.SNAP_FIELD_NUMBER);

        DATA_SNAP = DataDefinition.Snap.getDescriptor();
        DS_SNAP_INDEX = field(DATA_SNAP, DataDefinition.Snap.SNAPINDEX_FIELD_NUMBER);
        DS_DELTAS = field(DATA_SNAP, DataDefinition.Snap.DELTAS_FIELD_NUMBER);

        DATA_DELTA = DataDefinition.Delta.getDescriptor();
        DD_DELTA_INDEX = field(DATA_DELTA, DataDefinition.Delta.DELTAINDEX_FIELD_NUMBER);
        DD_DATA_ITEM = field(DATA_DELTA, DataDefinition.Delta.DATAITEM_FIELD_NUMBER);
    }

    @Validator
    @SuppressWarnings({"unchecked", "RedundantSuppression"})
    public static ValidationContext dataDefinition(DataDefinition msg, ValidationContext ctx) {

        ctx = ctx.pushOneOf(DD_SCHEMA_SPECIFIER)
                .apply(CommonValidators::required)
                .applyOneOf(DD_SCHEMA_ID, ObjectIdValidator::tagSelector, TagSelector.class)
                .applyOneOf(DD_SCHEMA_ID, ObjectIdValidator::selectorType, TagSelector.class, ObjectType.SCHEMA)
                .applyOneOf(DD_SCHEMA_ID, ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .applyOneOf(DD_SCHEMA, SchemaValidator::schema, SchemaDefinition.class)
                .pop();

        // Using apply() with Map.class produces an unchecked type warning, hence the suppression on this function

        ctx = ctx.pushMap(DD_PARTS, DataDefinition::getPartsMap)
                .apply(CommonValidators::mapNotEmpty)
                .applyMapKeys(DataValidator::opaqueKey)
                .applyMapValues(DataValidator::dataPart, DataDefinition.Part.class)
                .apply(DataValidator::partMapIsConsistent, Map.class)
                .pop();

        ctx = ctx.push(DD_STORAGE_ID)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.STORAGE)
                .apply(ObjectIdValidator::selectorForLatest, TagSelector.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext dataPart(DataDefinition.Part msg, ValidationContext ctx) {

        ctx = ctx.push(DP_PART_KEY)
                .apply(CommonValidators::required)
                .apply(DataValidator::partKey, PartKey.class)
                .pop();

        ctx = ctx.push(DP_SNAP)
                .apply(CommonValidators::required)
                .apply(DataValidator::dataSnap, DataDefinition.Snap.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext dataSnap(DataDefinition.Snap msg, ValidationContext ctx) {

        ctx = ctx.push(DS_SNAP_INDEX)
                .apply(CommonValidators::notNegative, Integer.class)
                .pop();

        ctx = ctx.pushRepeated(DS_DELTAS)
                .apply(CommonValidators::listNotEmpty)
                .applyRepeated(DataValidator::dataDelta, DataDefinition.Delta.class)
                .applyRepeated(DataValidator::deltaMatchesIndex, DataDefinition.Delta.class, msg)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext dataDelta(DataDefinition.Delta msg, ValidationContext ctx) {

        ctx = ctx.push(DD_DELTA_INDEX)
                .apply(CommonValidators::notNegative, Integer.class)
                .pop();

        ctx = ctx.push(DD_DATA_ITEM)
                .apply(CommonValidators::required)
                .apply(StorageValidator::dataItemKey)
                .pop();

        var snap = (DataDefinition.Snap) ctx.parentMsg();

        if (snap.getDeltasCount() <= msg.getDeltaIndex() || snap.getDeltas(msg.getDeltaIndex()) != msg) {

            var err = String.format("Unexpected delta index [%d] (should match list index)", msg.getDeltaIndex());
            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationContext deltaMatchesIndex(DataDefinition.Delta msg, DataDefinition.Snap snap, ValidationContext ctx) {

        if (snap.getDeltasCount() <= msg.getDeltaIndex() || snap.getDeltas(msg.getDeltaIndex()) != msg) {

            var err = String.format("Unexpected delta index [%d] (should match list index in the snap)", msg.getDeltaIndex());
            return ctx.error(err);
        }

        return ctx;
    }

    @Validator
    public static ValidationContext partKey(PartKey msg, ValidationContext ctx) {

        ctx = ctx.push(PK_PART_TYPE)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::recognizedEnum, PartType.class)
                .pop();

        var partByValueQualifier =  String.format("%s == %s", PK_PART_TYPE.getName(), PartType.PART_BY_VALUE.name());
        var partByRangeQualifier =  String.format("%s == %s", PK_PART_TYPE.getName(), PartType.PART_BY_RANGE.name());

        ctx = ctx.pushRepeated(PK_PART_VALUES)
                .apply(CommonValidators.ifAndOnlyIf(msg.getPartType() == PartType.PART_BY_VALUE, partByValueQualifier))
                .apply(CommonValidators::listNotEmpty)
                .applyRepeated(TypeSystemValidator::value, Value.class)
                .pop();

        ctx = ctx.push(PK_PART_RANGE_MIN)
                .apply(CommonValidators.ifAndOnlyIf(msg.getPartType() == PartType.PART_BY_RANGE, partByRangeQualifier))
                .apply(TypeSystemValidator::value, Value.class)
                .pop();

        ctx = ctx.push(PK_PART_RANGE_MAX)
                .apply(CommonValidators.ifAndOnlyIf(msg.getPartType() == PartType.PART_BY_RANGE, partByRangeQualifier))
                .apply(TypeSystemValidator::value, Value.class)
                .pop();

        var keyFieldsOk = !ctx.failed();

        ctx = ctx.push(PK_OPAQUE_KEY)
                .apply(CommonValidators::required)
                .apply(DataValidator::opaqueKey)
                // Do not try to check opaque key match if the part key has already failed validation
                .applyIf(keyFieldsOk, DataValidator::opaqueKeyMatchesPart, String.class, msg)
                .pop();

        // Type of values and range min/max constraints needs to match schema
        // Should that check be a referential check? Because data def does not always contain the schema...

        return ctx;
    }

    private static ValidationContext opaqueKey(String opaqueKey, ValidationContext ctx) {

        var matcher = ValidationConstants.OPAQUE_PART_KEY.matcher(opaqueKey);

        if (!matcher.matches()) {

            var err = String.format("Invalid part key [%s]", opaqueKey);
            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationContext opaqueKeyMatchesPart(String opaqueKey, PartKey partKey, ValidationContext ctx) {

        var expectedOpaqueKey = PartKeys.opaqueKey(partKey);

        if (!opaqueKey.equals(expectedOpaqueKey)) {

            var err = String.format("Part key does not match part definition (expected [%s], got [%s])",
                    expectedOpaqueKey, opaqueKey);

            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationContext partMapIsConsistent(Map<String, DataDefinition.Part> partMap, ValidationContext ctx) {

        for (var partEntry : partMap.entrySet()) {

            var partKey = partEntry.getKey();
            var part = partEntry.getValue().getPartKey().getOpaqueKey();

            if (!partKey.equals(part)) {

                var err = String.format("Part key [%s] does not match part definition for [%s]",
                        partKey, part);

                return ctx.error(err);
            }

            return ctx;
        }

        return ctx;
    }

}
