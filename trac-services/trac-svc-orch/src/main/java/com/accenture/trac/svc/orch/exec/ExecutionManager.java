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

package com.accenture.trac.svc.orch.exec;

import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.plugin.PluginManager;
import com.accenture.trac.config.ExecutorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ExecutionManager {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager plugins;
    private IBatchExecutor executor;

    public ExecutionManager(PluginManager plugins) {
        this.plugins = plugins;
    }

    // At present just initialize a single executor
    // But later support multiple, at least per-tenant, perhaps multiple within one tenant

    public void initExecutor(ExecutorConfig config) {

        var protocol = config.getExecutorType();

        if (plugins.isServiceAvailable(IBatchExecutor.class, protocol)) {

            var properties = new Properties();
            properties.putAll(config.getExecutorPropsMap());

            this.executor = plugins.createService(IBatchExecutor.class, protocol, properties);
        }
        else {

            var message = String.format("No plugin found to support executor protocol [%s]", protocol);
            var error = new EStartup(message);

            log.error(message, error);
            throw error;
        }
    }

    public IBatchExecutor getExecutor() {
        return this.executor;
    }
}
