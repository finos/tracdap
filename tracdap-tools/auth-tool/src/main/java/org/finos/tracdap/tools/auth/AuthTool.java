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

package org.finos.tracdap.tools.auth;


import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.startup.StandardArgs;
import org.finos.tracdap.common.startup.Startup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AuthTool {

    private final static String SIGNING_KEY_TASK = "signing_key";

    private final static List<StandardArgs.Task> AUTH_TOOL_TASKS = List.of(
            StandardArgs.task(SIGNING_KEY_TASK, List.of("ALGORITHM", "BITS"), "Create or replace the platform signing key for authentication tokens"));

    private final Logger log;
    private final ConfigManager configManager;

    /**
     * Construct a new instance of the auth tool
     * @param configManager A prepared instance of ConfigManager
     */
    public AuthTool(ConfigManager configManager) {

        this.log = LoggerFactory.getLogger(getClass());
        this.configManager = configManager;
    }

    public void runTasks(List<StandardArgs.Task> tasks) {


        for (var task : tasks) {

            if (SIGNING_KEY_TASK.equals(task.getTaskName()))
                createSigningKey(task.getTaskArg(0), task.getTaskArg(1));

            else
                throw new EStartup(String.format("Unknown task: [%s]", task.getTaskName()));
        }

        log.info("All tasks complete");

    }

    private void createSigningKey(String algorithm, String bits) {

        throw new ETracInternal("Not done yet");
    }

    /**
     * Entry point for the AuthTool utility.
     *
     * @param args Command line args
     */
    public static void main(String[] args) {

        try {

            var startup = Startup.useCommandLine(AuthTool.class, args, AUTH_TOOL_TASKS);
            startup.runStartupSequence();

            var config = startup.getConfig();
            var tasks = startup.getArgs().getTasks();

            var tool = new AuthTool(config);
            tool.runTasks(tasks);

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
