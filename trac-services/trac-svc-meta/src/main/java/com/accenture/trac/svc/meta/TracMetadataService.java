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

package com.accenture.trac.svc.meta;

import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.config.StandardArgsProcessor;
import com.accenture.trac.common.db.JdbcSetup;
import com.accenture.trac.common.exception.*;
import com.accenture.trac.common.util.InterfaceLogging;
import com.accenture.trac.svc.meta.api.MetadataPublicWriteApi;
import com.accenture.trac.svc.meta.api.MetadataReadApi;
import com.accenture.trac.svc.meta.api.MetadataSearchApi;
import com.accenture.trac.svc.meta.api.MetadataTrustedWriteApi;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcMetadataDal;
import com.accenture.trac.svc.meta.logic.MetadataReadLogic;
import com.accenture.trac.svc.meta.logic.MetadataSearchLogic;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.*;


public class TracMetadataService {

    // This is a quick implementation of the service scaffold, it will need to be re-visited!
    // All the components are created in start()

    // Because we are using JDBC, there is no option for fully async execution
    // We will need a thread pool that can handle the maximum number of concurrent requests
    // Since requests will sit on the JDBC thread pool anyway, we can use thread-per-request
    // A primary executor is given to gRPC for handling incoming requests
    // Then use Runnable::run to execute JDBC calls directly, i.e. no hand-off to a secondary pool

    // We do set up a blocking queue as an overflow
    // It would be good to tie this into health reporting and load balancing
    // That is not for this first quick implementation!

    private static final String PORT_CONFIG_KEY = "trac.svc.meta.api.port";
    private static final String DB_CONFIG_ROOT = "trac.svc.meta.db.sql";
    private static final String POOL_SIZE_KEY = DB_CONFIG_ROOT + ".pool.size";
    private static final String POOL_OVERFLOW_KEY = DB_CONFIG_ROOT + ".pool.overflow";

    private static final int DEFAULT_POOL_SIZE = 20;
    private static final int DEFAULT_OVERFLOW_SIZE = 10;

    private final Logger log;

    private final Properties properties;

    private JdbcMetadataDal dal;
    private ExecutorService executor;
    private Server server;

    TracMetadataService(Properties properties) {

        this.log = LoggerFactory.getLogger(getClass());

        this.properties = properties;
    }

    void start() throws IOException {

        log.info("TRAC metadata service is starting...");

        // Use the -db library to set up a datasource
        // Handles different SQL dialects and authentication mechanisms etc.
        var dialect = JdbcSetup.selectDialect(properties, DB_CONFIG_ROOT);
        var dataSource = JdbcSetup.createDatasource(properties, DB_CONFIG_ROOT);

        // Construct the DAL using a direct executor, as per the comments above
        dal = new JdbcMetadataDal(dialect, dataSource, Runnable::run);
        dal.startup();

        executor = createPrimaryExecutor(properties);

        // Set up services and APIs
        var dalWithLogging = InterfaceLogging.wrap(dal, IMetadataDal.class);

        var readLogic = new MetadataReadLogic(dalWithLogging);
        var writeLogic = new MetadataWriteLogic(dalWithLogging);
        var searchLogic = new MetadataSearchLogic(dalWithLogging);

        var readApi = new MetadataReadApi(readLogic);
        var publicWriteApi = new MetadataPublicWriteApi(writeLogic);
        var trustedWriteApi = new MetadataTrustedWriteApi(writeLogic);
        var searchApi = new MetadataSearchApi(searchLogic);

        // Create the main server

        var servicePort = readConfigInt(properties, PORT_CONFIG_KEY, null);

        this.server = ServerBuilder
                .forPort(servicePort)
                .addService(readApi)
                .addService(publicWriteApi)
                .addService(trustedWriteApi)
                .addService(searchApi)
                .executor(executor)
                .build();

        // Register a shutdown hook, so SIGTERM triggers a clean shutdown
        var mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            try {
                log.info("Shutdown request received");

                this.stop();
                mainThread.join();

                log.info("Normal shutdown complete");
                System.out.println("Normal shutdown complete");
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        }, "shutdown"));

        // Good to go, let's start!
        server.start();
    }

    ExecutorService createPrimaryExecutor(Properties properties) {

        // Headroom threads - these threads get used after the core pool and the overflow queue is full
        // That is not the behaviour we want, we want to fill up the pool first, then start queuing
        // So, we just use the core pool

        // A small number of headroom threads might be useful for admin tasks to avoid starvation
        // Although, to actually do anything useful with that prioritization would be needed

        var HEADROOM_THREADS = 1;
        var HEADROOM_THREADS_TIMEOUT = 60;
        var HEADROOM_THREADS_TIMEOUT_UNIT = TimeUnit.SECONDS;

        try {

            // Use the DB pool settings to create the primary executor
            // As per comments at the top of this file

            var poolSize = readConfigInt(properties, POOL_SIZE_KEY, DEFAULT_POOL_SIZE);
            var overflowSize = readConfigInt(properties, POOL_OVERFLOW_KEY, DEFAULT_OVERFLOW_SIZE);

            var threadFactory = new ThreadFactoryBuilder()
                    .setNameFormat("worker-%d")
                    .setPriority(Thread.NORM_PRIORITY)
                    .build();

            var overflowQueue = new ArrayBlockingQueue<Runnable>(overflowSize);

            var executor = new ThreadPoolExecutor(
                    poolSize, poolSize + HEADROOM_THREADS,
                    HEADROOM_THREADS_TIMEOUT, HEADROOM_THREADS_TIMEOUT_UNIT,
                    overflowQueue, threadFactory);

            executor.prestartAllCoreThreads();
            executor.allowCoreThreadTimeOut(false);

            return executor;
        }
        catch (NumberFormatException e) {

            var message = "Pool size and overflow must be integers: " + e.getMessage();
            log.error(message);
            throw new EStartup(message, e);
        }
    }

    private int readConfigInt(Properties props, String propKey, Integer propDefault) {

        // TODO: Reading config needs to be centralised
        // Standard methods for handling defaults, valid ranges etc.
        // One option is to use proto to define config objects and automate parsing
        // This would work well where configs need to be sent between components, e.g. the TRAC executor

        var propValue = props.getProperty(propKey);

        if (propValue == null || propValue.isBlank()) {

            if (propDefault == null) {

                var message = "Missing required config property: " + propKey;
                log.error(message);
                throw new EStartup(message);
            }
            else
                return propDefault;
        }

        try {
            return Integer.parseInt(propValue);
        }
        catch (NumberFormatException e) {

            var message = "Config property must be an integer: " + propKey + ", got value '" + propValue + "'";
            log.error(message);
            throw new EStartup(message);
        }
    }

    void stop() {

        try {
            log.info("TRAC Metadata service is going down");

            // Request the server shutdown first, this will stop new connections being accepted
            // Wait for the server to drain
            // Once there are no active requests, clean up internal resources

            server.shutdown();
            server.awaitTermination(30, TimeUnit.SECONDS);

            dal.shutdown();
            executor.shutdown();

            System.out.println("TRAC Metadata service will exit normally");
            log.info("TRAC Metadata service will exit normally");
        }
        catch (InterruptedException e) {

            System.err.println("TRAC Metadata service was interrupted during shutdown");
            log.warn("TRAC Metadata service was interrupted during shutdown");

            Thread.currentThread().interrupt();
        }
    }

    void blockUntilShutdown() throws InterruptedException {

        try {
            log.info("TRAC Metadata service is up on port " + server.getPort());

            server.awaitTermination();

            System.out.println("Going down in main");

            log.info("TRAC Metadata service is going down");
        }
        catch (InterruptedException e) {

            System.out.println("Going down in main int");

            log.info("TRAC Metadata service has been interrupted");
            throw e;
        }
    }


    public static void main(String[] args) {

        try {

            System.out.println(">>> TRAC Metadata Service " + "[DEVELOPMENT VERSION]");

            var standardArgs = StandardArgsProcessor.processArgs(args);

            System.out.println(">>> Working directory: " + standardArgs.getWorkingDir());
            System.out.println(">>> Config file: " + standardArgs.getConfigFile());
            System.out.println();

            var configManager = new ConfigManager(standardArgs);
            configManager.initConfigPlugins();
            configManager.initLogging();

            var properties = configManager.loadRootProperties();
            var service = new TracMetadataService(properties);
            service.start();
            service.blockUntilShutdown();

            System.exit(0);
        }
        catch (EStartup e) {

            if (e.isQuiet())
                System.exit(e.getExitCode());

            System.err.println("The service failed to start: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(e.getExitCode());
        }
        catch (Exception e) {

            System.err.println("There was an unexpected error on the main thread: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(-1);
        }
    }
}
