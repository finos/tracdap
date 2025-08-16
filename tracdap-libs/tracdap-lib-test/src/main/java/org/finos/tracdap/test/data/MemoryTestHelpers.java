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

package org.finos.tracdap.test.data;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class MemoryTestHelpers {

    private static final Logger log = LoggerFactory.getLogger(MemoryTestHelpers.class);

    public static class AllocationLogger implements AllocationListener {

        @Override
        public void onPreAllocation(long size) {

            var stack = Thread.currentThread().getStackTrace();
            var frame = Arrays.stream(stack)
                    .filter(f -> f.getClassName().startsWith("org.finos.tracdap."))
                    .filter(f -> ! f.getMethodName().equals("onPreAllocation"))
                    .findFirst();

            log.info("ALLOCATE: {} - {}", size, frame.orElse(null));
        }

        @Override
        public void onRelease(long size) {

            var stack = Thread.currentThread().getStackTrace();
            var frame = Arrays.stream(stack)
                    .filter(f -> f.getClassName().startsWith("org.finos.tracdap."))
                    .filter(f -> ! f.getMethodName().equals("onRelease"))
                    .findFirst();

            log.info("RELEASE: {} - {}", size, frame.orElse(null));
        }
    }

    public static BufferAllocator testAllocator(boolean debugLogging) {

        var config = RootAllocator.configBuilder();

        if (debugLogging)
            config.listener(new AllocationLogger());

        return new RootAllocator(config.build());
    }
}
