/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.plugins.azure.storage;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;


public class AzureScheduling {

    private static final ConcurrentMap<Object, Scheduler> SCHEDULERS = new ConcurrentHashMap<>();

    public static Scheduler schedulerFor(ExecutorService executor) {

        var existingScheduler = SCHEDULERS.get(executor);

        if (existingScheduler != null)
            return existingScheduler;

        var newScheduler = Schedulers.fromExecutorService(executor);
        var priorScheduler = SCHEDULERS.putIfAbsent(executor, newScheduler);

        return priorScheduler != null ? priorScheduler : newScheduler;
    }
}
