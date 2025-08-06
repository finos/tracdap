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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.List;
import java.util.stream.Collectors;


public class SubFieldTranslator<T> implements IRequestTranslator<T> {

    private final Descriptors.FieldDescriptor fieldDescriptor;
    private final IRequestTranslator<T> delegate;

    private final List<String> fields;

    public SubFieldTranslator(
            Descriptors.FieldDescriptor fieldDescriptor,
            IRequestTranslator<T> delegate) {

        this.fieldDescriptor = fieldDescriptor;
        this.delegate = delegate;

        // Build translated fields list
        this.fields = delegate.fields().stream()
                .map(childField -> qualifiedFieldName(fieldDescriptor, childField))
                .collect(Collectors.toList());
    }

    private String qualifiedFieldName(Descriptors.FieldDescriptor fieldDescriptor, String childField) {

        if (childField.equals("*"))
            return fieldDescriptor.getName();
        else
            return fieldDescriptor.getName() + "." + childField;
    }

    @Override
    public List<String> fields(){
        return fields;
    }

    @Override
    public Message.Builder translate(Message.Builder builder, T value) {
        var childBuilder = builder.getFieldBuilder(fieldDescriptor);
        return delegate.translate(childBuilder, value);
    }
}
