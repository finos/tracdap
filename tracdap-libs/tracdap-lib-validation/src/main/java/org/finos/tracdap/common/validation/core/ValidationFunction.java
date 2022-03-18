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

package org.finos.tracdap.common.validation.core;


import com.google.protobuf.Message;
import org.finos.tracdap.common.exception.EUnexpected;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class ValidationFunction<T> {

    @FunctionalInterface
    public interface Basic { ValidationContext apply(ValidationContext ctx); }

    @FunctionalInterface
    public interface Typed<T> { ValidationContext apply(T value, ValidationContext ctx); }

    @FunctionalInterface
    public interface TypedArg<T, U> { ValidationContext apply(T value, U arg, ValidationContext ctx); }

    @FunctionalInterface
    public interface Version<T> { ValidationContext apply(T current, T prior, ValidationContext ctx); }

    public static <S> ValidationFunction<S> makeTyped(Typed<S> func, Class<S> targetClass) {
        return new ValidationFunction<>(func, targetClass);
    }

    public static <S> ValidationFunction<S> makeTyped(Method method, Class<S> targetClass) {
        return makeTyped(invokeTyped(method), targetClass);
    }

    public static <S> ValidationFunction<S> makeVersion(Version<S> func, Class<S> targetClass) {
        return new ValidationFunction<>(func, targetClass);
    }

    public static <S> ValidationFunction<S> makeVersion(Method method, Class<S> targetClass) {
        return makeVersion(invokeVersion(method), targetClass);
    }

    private final Class<T> targetClass;
    private final ValidationFunction.Basic basic;
    private final ValidationFunction.Typed<T> typed;
    private final ValidationFunction.Version<T> version;

    public ValidationFunction(ValidationFunction.Basic validator, Class<T> targetClass) {
        this.targetClass = targetClass;
        this.basic = validator;
        this.typed = null;
        this.version = null;
    }

    public ValidationFunction(ValidationFunction.Typed<T> validator, Class<T> targetClass) {
        this.targetClass = targetClass;
        this.basic = null;
        this.typed = validator;
        this.version = null;
    }

    public ValidationFunction(ValidationFunction.Version<T> validator, Class<T> targetClass) {
        this.targetClass = targetClass;
        this.basic = null;
        this.typed = null;
        this.version = validator;
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

    public boolean isVersion() {
        return version != null;
    }

    public ValidationFunction.Version<T> version() {
        return version;
    }


    private static <S> Typed<S> invokeTyped(Method method) {

        var isStatic = Modifier.isStatic(method.getModifiers());
        var paramTypes = method.getParameterTypes();
        var returnType = method.getReturnType();

        var signatureMatch =
                isStatic &&
                returnType.equals(ValidationContext.class) &&
                paramTypes.length == 2 &&
                Message.class.isAssignableFrom(paramTypes[0]) &&
                paramTypes[1].equals(ValidationContext.class);

        if (!signatureMatch)
            throw new EUnexpected();

        return (msg, ctx) -> {

            try {
                return (ValidationContext) method.invoke(null, msg, ctx);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new EUnexpected();
            }
        };
    }

    private static <S> Version<S> invokeVersion(Method method) {

        var isStatic = Modifier.isStatic(method.getModifiers());
        var paramTypes = method.getParameterTypes();
        var returnType = method.getReturnType();

        var signatureMatch =
                isStatic &&
                returnType.equals(ValidationContext.class) &&
                paramTypes.length == 3 &&
                Message.class.isAssignableFrom(paramTypes[0]) &&
                paramTypes[1].equals(paramTypes[0]) &&
                paramTypes[2].equals(ValidationContext.class);

        if (!signatureMatch)
            throw new EUnexpected();

        return (msg, prior, ctx) -> {

            try {
                return (ValidationContext) method.invoke(null, msg, prior, ctx);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new EUnexpected();
            }
        };
    }
}
