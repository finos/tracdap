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
import com.accenture.trac.common.config.IConfigLoader;
import com.accenture.trac.common.config.StandardArgsProcessor;
import com.accenture.trac.common.db.JdbcSetup;
import com.accenture.trac.common.exception.*;
import com.accenture.trac.svc.meta.api.MetadataPublicWriteApi;
import com.accenture.trac.svc.meta.api.MetadataReadApi;
import com.accenture.trac.svc.meta.api.MetadataSearchApi;
import com.accenture.trac.svc.meta.api.MetadataTrustedWriteApi;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcMetadataDal;
import com.accenture.trac.svc.meta.logic.MetadataReadLogic;
import com.accenture.trac.svc.meta.logic.MetadataSearchLogic;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class TracMetadataService {

    private static final int DEFAULT_PORT = 8081;

    private final Logger log;

    private final Properties properties;
    private final ConfigManager configManager;

    private ThreadPoolExecutor dalExecutor;
    private JdbcMetadataDal dal;
    private Server server;

    TracMetadataService(Properties properties, ConfigManager configManager) {

        this.log = LoggerFactory.getLogger(getClass());

        this.properties = properties;
        this.configManager = configManager;
    }

    void start() throws IOException {

        log.info("TRAC metadata service is starting...");

        var DB_CONFIG_ROOT = "trac.svc.meta.db.sql";
        var dialect = JdbcSetup.selectDialect(properties, DB_CONFIG_ROOT);
        var dataSource = JdbcSetup.createDatasource(properties, DB_CONFIG_ROOT);

        dalExecutor = null;  //new ThreadPoolExecutor(0, 100, 30, TimeUnit.SECONDS, null);
        var dal = new JdbcMetadataDal(dialect, dataSource, dalExecutor);

        var readLogic = new MetadataReadLogic(dal);
        var writeLogic = new MetadataWriteLogic(dal);
        var searchLogic = new MetadataSearchLogic(dal);

        var readApi = new MetadataReadApi(readLogic);
        var publicWriteApi = new MetadataPublicWriteApi(writeLogic);
        var trustedWriteApi = new MetadataTrustedWriteApi(writeLogic);
        var searchApi = new MetadataSearchApi(searchLogic);

        this.server = ServerBuilder
                .forPort(DEFAULT_PORT)
                .addService(readApi)
                .addService(publicWriteApi)
                .addService(trustedWriteApi)
                .addService(searchApi)
                .directExecutor()
                .build();

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

        server.start();
    }

    void stop() {

        try {
            log.info("TRAC Metadata service is going down");

            server.shutdown();
            server.awaitTermination(30, TimeUnit.SECONDS);

            dal.shutdown();

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
            log.info("TRAC Metadata service is up");

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
            var service = new TracMetadataService(properties, configManager);
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
