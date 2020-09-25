/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.Collection;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;


public class InterfaceLogging implements InvocationHandler {

    private final Object impl;
    private final Logger log;

    @SuppressWarnings("unchecked")
    public static <I, T extends I>
    I wrap(T impl, Class<I> iface) {

        var logger = LoggerFactory.getLogger(impl.getClass());
        var handler = new InterfaceLogging(impl, logger);
        var proxy = Proxy.newProxyInstance(impl.getClass().getClassLoader(), new Class[]{iface}, handler);

        return (I) proxy;
    }

    private InterfaceLogging(Object impl, Logger log) {
        this.impl = impl;
        this.log = log;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        var params = method.getParameters();
        var paramInfo = new String[params.length];

        for (var paramIndex = 0; paramIndex < params.length; paramIndex++)
            paramInfo[paramIndex] = params[paramIndex].getName() + " = " + argToString(args[paramIndex]);

        try {

            log.info("START: {} {}", method.getName(), String.join(", ", paramInfo));

            var result = method.invoke(impl, args);

            // For async methods, logging success/fail status must be applied after the async operation completes
            if (CompletionStage.class.isAssignableFrom(method.getReturnType()))
                return applyAsyncLogging(method, (CompletionStage<?>) result);

            // For synchronous methods, we can log success as soon as the method returns
            log.info("SUCCEEDED: {}", method.getName());
            return result;
        }

        // If the method blows up before returning a value, log a failure for both async and synchronous methods
        // In the async case, a completion stage has not been successfully created

        catch (InvocationTargetException e) {

            log.error("FAILED: {} {}", method.getName(), e.getTargetException().getMessage(), e.getTargetException());
            throw e.getTargetException();
        }
        catch (Throwable e) {

            log.error("FAILED: {} Unexpected error logging method status ({})", method.getName(), e.getMessage(), e);
            throw e;
        }
    }

    private String argToString(Object arg) {

        if (arg == null)
            return "(null)";

        // Do not log full contents of generated protobuf classes, they can be big!
        if (arg instanceof com.google.protobuf.Message)
            return String.format("(metadata %s)", arg.getClass().getSimpleName());

        // Do not log the contents of collections either!
        if (arg instanceof Collection)
            return String.format("(collection %s, size=%d)", arg.getClass().getSimpleName(), ((Collection<?>) arg).size());

        return arg.toString();
    }

    private <T> CompletionStage<T> applyAsyncLogging(Method method, CompletionStage<T> asyncOperation) {

        return asyncOperation
            .thenApply(x -> {
                log.info("SUCCEEDED: {}", method.getName());
                return x;
            })
            .exceptionally(e -> {

                if (e instanceof CompletionException) {
                    log.error("FAILED: {} {}", method.getName(), e.getCause().getMessage(), e.getCause());
                    throw (CompletionException) e;
                }
                else {
                    log.error("FAILED: {} {}", method.getName(), e.getMessage(), e);
                    throw new CompletionException(e);
                }
            });
    }
}
