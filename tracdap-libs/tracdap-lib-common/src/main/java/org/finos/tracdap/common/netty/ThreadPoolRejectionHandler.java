/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;


class ThreadPoolRejectionHandler implements RejectedExecutionHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String threadPoolName;

    ThreadPoolRejectionHandler(String threadPoolName) {
        this.threadPoolName = threadPoolName;
    }

    @Override
    public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {

        String message;

        if (executor.isShutdown()) {

            message = String.format(
                    "Task rejected for [%s], the executor is shutting down",
                    threadPoolName);
        }
        else {

            message = String.format(
                    "Task rejected for [%s], active tasks = %d, queued tasks = %d, remaining queue capacity = %d",
                    threadPoolName, executor.getActiveCount(),
                    executor.getQueue().size(),
                    executor.getQueue().remainingCapacity());
        }

        log.error(message);
        throw new RejectedExecutionException(message);
    }
}
