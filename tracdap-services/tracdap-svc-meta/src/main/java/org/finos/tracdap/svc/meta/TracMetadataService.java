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

package org.finos.tracdap.svc.meta;

import org.finos.tracdap.common.auth.AuthInterceptor;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.CommonServiceBase;
import org.finos.tracdap.common.util.InterfaceLogging;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.svc.meta.dal.IMetadataDal;
import org.finos.tracdap.svc.meta.dal.jdbc.JdbcMetadataDal;
import org.finos.tracdap.svc.meta.services.MetadataReadService;
import org.finos.tracdap.svc.meta.services.MetadataSearchService;
import org.finos.tracdap.svc.meta.services.MetadataWriteService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.finos.tracdap.svc.meta.api.TracMetadataApi;
import org.finos.tracdap.svc.meta.api.TrustedMetadataApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.*;


public class TracMetadataService extends CommonServiceBase {

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

    private static final String POOL_SIZE_KEY = "pool.size";
    private static final String POOL_OVERFLOW_KEY = "pool.overflow";

    private static final int DEFAULT_POOL_SIZE = 20;
    private static final int DEFAULT_OVERFLOW_SIZE = 10;

    private final Logger log;

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private DataSource dataSource;
    private ExecutorService executor;
    private JdbcMetadataDal dal;
    private Server server;

    public TracMetadataService(PluginManager pluginManager, ConfigManager configManager) {

        this.log = LoggerFactory.getLogger(getClass());

        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) {

        try {

            // Use the -db library to set up a datasource
            // Handles different SQL dialects and authentication mechanisms etc.
            var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);

            var metaDbConfig = platformConfig.getMetadata().getDatabase();
            var dalProps = new Properties();
            dalProps.putAll(metaDbConfig.getPropertiesMap());

            var dialect = JdbcSetup.getSqlDialect(dalProps, "");
            dataSource = JdbcSetup.createDatasource(dalProps, "");

            // Construct the DAL using a direct executor, as per the comments above
            dal = new JdbcMetadataDal(dialect, dataSource);
            dal.startup();

            executor = createPrimaryExecutor(dalProps);

            // Set up services and APIs
            var dalWithLogging = InterfaceLogging.wrap(dal, IMetadataDal.class);

            var readService = new MetadataReadService(dalWithLogging, platformConfig);
            var writeService = new MetadataWriteService(dalWithLogging);
            var searchService = new MetadataSearchService(dalWithLogging);

            var publicApi = new TracMetadataApi(readService, writeService, searchService);
            var trustedApi = new TrustedMetadataApi(readService, writeService, searchService);

            var authentication = AuthInterceptor.setupAuth(
                    platformConfig.getAuthentication(),
                    configManager);

            // Create the main server

            var metaSvcConfig = platformConfig.getServices().getMeta();
            var servicePort = metaSvcConfig.getPort();

            this.server = ServerBuilder
                    .forPort(servicePort)
                    .intercept(authentication)
                    .addService(publicApi)
                    .addService(trustedApi)
                    .executor(executor)
                    .build();

            // Good to go, let's start!
            server.start();

        }
        catch (IOException e) {

            // Wrap startup errors in an EStartup
            throw new EStartup(e.getMessage(), e);
        }
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) throws InterruptedException {

        // Request the server shutdown first, this will stop new connections being accepted
        // Wait for the server to drain
        // Once there are no active requests, clean up internal resources

        server.shutdown();
        server.awaitTermination(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);

        if (!server.isTerminated())
            server.shutdownNow();

        dal.shutdown();
        executor.shutdown();

        JdbcSetup.destroyDatasource(dataSource);

        return 0;
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

    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracMetadataService.class, args);
    }
}
