/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.data.validation;

import com.accenture.trac.metadata.TagSelector;
import com.accenture.trac.metadata.TagUpdate;
import com.google.protobuf.ProtocolMessageEnum;


public class MetadataValidator {

    public static ValidationContext validateTagUpdate(TagUpdate msg, ValidationContext ctx) {

        ctx = ctx.push(msg, "attrName")
                .apply(Validation::identifier)
                .apply(Validation::notTracReserved)
                .pop();

        ctx = ctx.push(msg, "operation")
                //.apply(this::validateEnum)
                .pop();

        // TODO: Validation for values and the TRAC type system

        return ctx;
    }


    public void validateTagSelector(TagSelector msg, ValidationContext ctx) {

        // if (msg.has)
    }

    public ValidationContext validateEnum(
            ProtocolMessageEnum enum_,
            ValidationContext ctx) {

        try {
            var value = enum_.getNumber();

            if (value == 0) {

                var typeName = enum_.getDescriptorForType().getName();
                var message = String.format("Value not set for [%s]", typeName);
                return ctx.error(message);
            }
        }
        catch (IllegalStateException e) {

            var typeName = enum_.getDescriptorForType().getName();
            var message = String.format("Invalid value for [%s]", typeName);
            return ctx.error(message);
        }

        return ctx;
    }

}
