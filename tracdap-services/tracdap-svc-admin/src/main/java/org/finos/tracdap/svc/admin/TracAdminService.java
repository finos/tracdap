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

package org.finos.tracdap.svc.admin;

import org.finos.tracdap.api.AdminServiceProto;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.metadata.dal.IMetadataDal;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.netty.NettyHelpers;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.TracServiceBase;
import org.finos.tracdap.common.service.TracServiceConfig;
import org.finos.tracdap.common.validation.ValidationConcern;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.svc.admin.api.TracAdminApi;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.finos.tracdap.svc.admin.services.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


public class TracAdminService extends TracServiceBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private ExecutorService executor;
    private IMetadataDal dal;
    private Server server;


    public static void main(String[] args) {
        TracServiceBase.svcMain(TracAdminService.class, args);
    }

    public TracAdminService(PluginManager pluginManager, ConfigManager configManager) {
        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) {

        try {
            var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
            var serviceConfig = platformConfig.getServicesOrThrow(ConfigKeys.ADMIN_SERVICE_KEY);
            var servicePort = serviceConfig.getPort();

            var executor = NettyHelpers.threadPoolExecutor("admin-svc", 10, 10);
            this.executor = executor;
            executor.prestartAllCoreThreads();
            executor.allowCoreThreadTimeOut(false);

            // Load the DAL service using the plugin loader mechanism
            var metaDbConfig = platformConfig.getMetadata().getDatabase();
            dal = pluginManager.createService(IMetadataDal.class, metaDbConfig, configManager);
            dal.start();

            var configService = new ConfigService(dal);

            var adminApi = new TracAdminApi(configService);

            // Common framework for cross-cutting concerns
            var commonConcerns = buildCommonConcerns();

            var serverBuilder = ServerBuilder
                    .forPort(servicePort)
                    .executor(executor)
                    .addService(adminApi);

            // Apply common concerns
            this.server = commonConcerns
                    .configureServer(serverBuilder)
                    .build();

            // Good to go, let's start!
            this.server.start();
        }
        catch (IOException e) {

            // Wrap startup errors in an EStartup
            throw new EStartup(e.getMessage(), e);
        }
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) throws InterruptedException {

        server.shutdown();
        server.awaitTermination(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);

        if (!server.isTerminated())
            server.shutdownNow();

        dal.stop();
        executor.shutdown();

        return 0;
    }
    private GrpcConcern buildCommonConcerns() {

        var commonConcerns = TracServiceConfig.coreConcerns(TracAdminService.class);

        // Validation concern for the APIs being served
        var validationConcern = new ValidationConcern(AdminServiceProto.getDescriptor());
        commonConcerns = commonConcerns.addAfter(TracServiceConfig.TRAC_PROTOCOL, validationConcern);

        // Additional cross-cutting concerns configured by extensions
        for (var extension : pluginManager.getExtensions()) {
            commonConcerns = extension.addServiceConcerns(commonConcerns, configManager, ConfigKeys.ADMIN_SERVICE_KEY);
        }

        return commonConcerns.build();
    }
}
