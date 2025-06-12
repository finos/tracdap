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

package org.finos.tracdap.svc.data.api;

import org.apache.arrow.memory.BufferAllocator;
import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.netty.EventLoopResolver;
import org.finos.tracdap.common.util.LoggingHelpers;

import org.slf4j.Logger;


public class DataContextHelpers {

    private static final long DEFAULT_INITIAL_ALLOCATION = 16 * 1024 * 1024;
    private static final long DEFAULT_MAX_ALLOCATION = 128 * 1024 * 1024;

    private final Logger log;

    private final EventLoopResolver eventLoopResolver;
    private final BufferAllocator rootAllocator;

    private final long reqInitAllocation;
    private final long reqMaxAllocation;

    DataContextHelpers(Logger log, EventLoopResolver eventLoopResolver,  BufferAllocator rootAllocator) {

        this.log = log;
        this.eventLoopResolver = eventLoopResolver;
        this.rootAllocator = rootAllocator;

        this.reqInitAllocation = DEFAULT_INITIAL_ALLOCATION;
        this.reqMaxAllocation = DEFAULT_MAX_ALLOCATION;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Common scaffolding for client and server streaming
    // -----------------------------------------------------------------------------------------------------------------

    DataContext prepareDataContext(RequestMetadata requestMetadata) {

        // Enforce strict requirement on the event loop
        // All processing for the request must happen on the EL originally assigned to the request

        var requestId = requestMetadata.requestId();
        var eventLoop = eventLoopResolver.currentEventLoop(/* strict = */ true);
        var allocator = rootAllocator.newChildAllocator(requestId, reqInitAllocation, reqMaxAllocation);

        log.info("OPEN data context for [{}]", requestId);

        return new DataContext(eventLoop, allocator);
    }

    void closeDataContext(IDataContext dataContext) {

        // this method is normally triggered by the last onComplete or onError event in the pipeline
        // However there can be clean-up that still needs to execute, often in finally blocks
        // Posting back to the event loop lets clean-up complete before the context is closed

        var eventLoop = dataContext.eventLoopExecutor();
        eventLoop.submit(() -> closeDataContextLater(dataContext));
    }

    void closeDataContextLater(IDataContext dataContext) {

        try (var allocator = dataContext.arrowAllocator()) {

            var peak = allocator.getPeakMemoryAllocation();
            var retained = allocator.getAllocatedMemory();

            if (retained == 0)
                log.info("CLOSE data context for [{}], peak = [{}], retained = [{}]",
                        allocator.getName(),
                        LoggingHelpers.formatFileSize(peak),
                        LoggingHelpers.formatFileSize(retained));
            else
                log.warn("CLOSE data context for [{}], peak = [{}], retained = [{}] (memory leak)",
                        allocator.getName(),
                        LoggingHelpers.formatFileSize(peak),
                        LoggingHelpers.formatFileSize(retained));
        }
    }
}
