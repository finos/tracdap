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

package org.finos.tracdap.test.helpers;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.CommonServiceBase;
import org.finos.tracdap.common.startup.StandardArgs;
import org.finos.tracdap.common.startup.Startup;
import org.finos.tracdap.tools.deploy.metadb.DeployMetaDB;
import org.finos.tracdap.svc.meta.TracMetadataService;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;


public class ServiceHelpers {

    public static void runDbDeploy(Path workingDir, URL configPath, String keystoreKey, List<StandardArgs.Task> tasks) {

        var startup = Startup.useConfigFile(DeployMetaDB.class, workingDir, configPath.toString(), keystoreKey);
        startup.runStartupSequence();

        var config = startup.getConfig();
        var deployDb = new DeployMetaDB(config);

        deployDb.runDeployment(tasks);
    }

    public static <TSvc extends CommonServiceBase> TSvc startService(
            Class<TSvc> serviceClass, Path workingDir,
            URL configPath, String keystoreKey) {

        try {

            var startup = Startup.useConfigFile(
                    TracMetadataService.class, workingDir,
                    configPath.toString(), keystoreKey);

            startup.runStartupSequence();

            var plugins = startup.getPlugins();
            var config = startup.getConfig();

            var constructor = serviceClass.getConstructor(PluginManager.class, ConfigManager.class);
            var service = constructor.newInstance(plugins, config);
            service.start();

            return service;
        }
        catch (NoSuchMethodException e) {

            var err = String.format(
                    "Service class [%s] does not provide the standard service constructor",
                    serviceClass.getSimpleName());

            throw new ETracInternal(err);
        }
        catch (InvocationTargetException | IllegalAccessException | InstantiationException e) {

            var err = String.format(
                    "Service class [%s] cannot be constructed: %s",
                    serviceClass.getSimpleName(), e.getMessage());

            throw new ETracInternal(err, e);
        }
    }
}
