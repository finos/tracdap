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
import io.netty.buffer.ByteBuf;


public class SubFieldResponseTranslator<TOutput> implements IResponseTranslator<Message> {

    private final Descriptors.FieldDescriptor fieldDescriptor;
    private final IResponseTranslator<TOutput> delegate;

    public SubFieldResponseTranslator(
            Descriptors.FieldDescriptor fieldDescriptor,
            IResponseTranslator<TOutput> delegate){

        this.fieldDescriptor = fieldDescriptor;
        this.delegate = delegate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ByteBuf translate(Message msg) {
        var child = (TOutput) msg.getField(fieldDescriptor);
        return delegate.translate(child);
    }
}
