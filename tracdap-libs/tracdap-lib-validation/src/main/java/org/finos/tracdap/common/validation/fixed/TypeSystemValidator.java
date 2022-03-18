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

package org.finos.tracdap.common.validation.fixed;

import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.metadata.*;
import com.google.protobuf.Descriptors;


public class TypeSystemValidator {

    private static final Descriptors.Descriptor DATE_VALUE;
    private static final Descriptors.FieldDescriptor DATEV_ISO_DATE;

    private static final Descriptors.Descriptor DATETIME_VALUE;
    private static final Descriptors.FieldDescriptor DTV_ISO_DATETIME;


    static {

        DATE_VALUE = DateValue.getDescriptor();
        DATEV_ISO_DATE = field(DATE_VALUE, DateValue.ISODATE_FIELD_NUMBER);

        DATETIME_VALUE = DatetimeValue.getDescriptor();
        DTV_ISO_DATETIME = field(DATETIME_VALUE, DatetimeValue.ISODATETIME_FIELD_NUMBER);
    }

    static Descriptors.FieldDescriptor field(Descriptors.Descriptor msg, int fieldNo) {
        return msg.findFieldByNumber(fieldNo);
    }


    public static ValidationContext primitive(BasicType basicType, ValidationContext ctx) {

        if (!TypeSystem.isPrimitive(basicType)) {
            var err = String.format("Type specified in [%s] is not a primitive type: [%s]", ctx.fieldName(), basicType);
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext dateValue(DatetimeValue msg, ValidationContext ctx) {

        return ctx.push(DATEV_ISO_DATE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::isoDate)
                .pop();
    }

    public static ValidationContext datetimeValue(DatetimeValue msg, ValidationContext ctx) {

        return ctx.push(DTV_ISO_DATETIME)
                .apply(CommonValidators::required)
                .apply(CommonValidators::isoDatetime)
                .pop();
    }


}
