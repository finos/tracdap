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

package org.finos.tracdap.gateway.proxy.rest.translate;

import org.finos.tracdap.common.exception.EInputValidation;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.finos.tracdap.gateway.proxy.rest.RestApiErrors;

import java.util.List;


public class EnumTranslator implements IRequestTranslator<String> {

    private final Descriptors.FieldDescriptor fieldDescriptor;

    public EnumTranslator(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public List<String> fields(){
        return List.of(fieldDescriptor.getName());
    }

    @Override
    public Message.Builder translate(Message.Builder builder, String value) {

        var enumType = fieldDescriptor.getEnumType();
        var enumValue = enumType.findValueByName(value.toUpperCase());

        if (enumValue == null) {
            var message = String.format(RestApiErrors.INVALID_REQUEST_ENUM_VALUE, value, fieldDescriptor.getName());
            throw new EInputValidation(message);
        }

        if (fieldDescriptor.isRepeated())
            return builder.addRepeatedField(fieldDescriptor, enumValue);
        else
            return builder.setField(fieldDescriptor, enumValue);
    }
}
