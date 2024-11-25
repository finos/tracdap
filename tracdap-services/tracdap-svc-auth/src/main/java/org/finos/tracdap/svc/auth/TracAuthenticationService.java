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

package org.finos.tracdap.svc.auth;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.CommonServiceBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;


public class TracAuthenticationService extends CommonServiceBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    public static void main(String[] args) {

        CommonServiceBase.svcMain(TracAuthenticationService.class, args);
    }

    public TracAuthenticationService(PluginManager pluginManager, ConfigManager configManager) {

        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) throws InterruptedException {

    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) throws InterruptedException {
        return 0;
    }
}
