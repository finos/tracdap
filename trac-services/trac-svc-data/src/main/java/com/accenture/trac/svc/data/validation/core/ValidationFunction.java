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

package com.accenture.trac.svc.data.validation.core;

import com.google.protobuf.Message;


public class ValidationFunction<T> {

    @FunctionalInterface
    public interface Basic { ValidationContext validate(ValidationContext ctx); }

    @FunctionalInterface
    public interface Typed<T> { ValidationContext validate(T value, ValidationContext ctx); }

    @FunctionalInterface
    public interface Version<T> { ValidationContext validate(T current, T prior, ValidationContext ctx); }


    private final Class<T> targetClass;
    private final ValidationFunction.Basic basic;
    private final ValidationFunction.Typed<T> typed;

    public ValidationFunction(ValidationFunction.Basic validator, Class<T> targetClass) {
        this.targetClass = targetClass;
        this.basic = validator;
        this.typed = null;
    }

    public ValidationFunction(ValidationFunction.Typed<T> validator, Class<T> targetClass) {
        this.targetClass = targetClass;
        this.basic = null;
        this.typed = validator;
    }

    public Class<T> targetClass() {
        return targetClass;
    }

    public boolean isBasic() {
        return basic != null;
    }

    public ValidationFunction.Basic basic() {
        return basic;
    }

    public boolean isTyped() {
        return typed != null;
    }

    public ValidationFunction.Typed<T> typed() {
        return typed;
    }
}
