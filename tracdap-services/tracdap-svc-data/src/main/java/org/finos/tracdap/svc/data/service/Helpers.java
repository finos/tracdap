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

package org.finos.tracdap.svc.data.service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;


class Helpers {

    static Void reportError(
            Throwable error,
            CompletableFuture<?> definition,
            Flow.Subscriber<?> content) {

        if (!definition.isDone())
            definition.completeExceptionally(error);

        else {

            try {
                content.onSubscribe(new Flow.Subscription() {
                    @Override public void request(long n) {}
                    @Override public void cancel() {}
                });
            }
            catch (IllegalStateException e) {

                // Already subscribed
            }

            content.onError(error);
        }

        return null;
    }
}
