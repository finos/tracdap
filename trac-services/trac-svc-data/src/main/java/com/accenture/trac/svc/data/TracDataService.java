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

package com.accenture.trac.svc.data;

import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.service.CommonServiceBase;
import com.accenture.trac.svc.data.api.TracDataApi;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class TracDataService extends CommonServiceBase {

    private static final short DATA_SERVICE_PORT = 8082;

    private static final int WORKER_POOL_SIZE = 6;
    private static final int OVERFLOW_SIZE = 10;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConfigManager configManager;
    private Server server;

    public TracDataService(ConfigManager config) {
        this.configManager = config;
    }

    @Override
    protected void doStartup(Duration startupTimeout) {

        try {

            var publicApi = new TracDataApi();

            // Create the main server

            var executor = createPrimaryExecutor();

            this.server = ServerBuilder
                    .forPort(DATA_SERVICE_PORT)
                    .addService(publicApi)
                    .executor(executor)
                    .build();

            // Good to go, let's start!
            server.start();
        }
        catch (IOException e) {

            throw new EStartup(e.getMessage(), e);
        }
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) throws InterruptedException {

        server.shutdown();
        server.awaitTermination(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);

        if (server.isTerminated())
            return 0;

        server.shutdownNow();
        return -1;
    }

    private ExecutorService createPrimaryExecutor() {

        // TODO: Review executor settings

        var HEADROOM_THREADS = 1;
        var HEADROOM_THREADS_TIMEOUT = 60;
        var HEADROOM_THREADS_TIMEOUT_UNIT = TimeUnit.SECONDS;

        var threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("worker-%d")
                .setPriority(Thread.NORM_PRIORITY)
                .build();

        var overflowQueue = new ArrayBlockingQueue<Runnable>(OVERFLOW_SIZE);

        var executor = new ThreadPoolExecutor(
                WORKER_POOL_SIZE, WORKER_POOL_SIZE + HEADROOM_THREADS,
                HEADROOM_THREADS_TIMEOUT, HEADROOM_THREADS_TIMEOUT_UNIT,
                overflowQueue, threadFactory);

        executor.prestartAllCoreThreads();
        executor.allowCoreThreadTimeOut(false);

        return executor;
    }

    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracDataService.class, args);
    }
}
