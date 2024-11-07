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

package org.finos.tracdap.common.startup;

import org.finos.tracdap.common.config.ConfigManager;

import java.nio.file.Path;
import java.util.List;


/**
 * The standard arguments needed to initialise a ConfigManager
 *
 * @see StandardArgsProcessor
 * @see ConfigManager
 */
public class StandardArgs {

    private final Path workingDir;
    private final String configFile;
    private final String secretKey;
    private final List<Task> tasks;

    public StandardArgs(Path workingDir, String configFile, String secretKey, List<Task> tasks) {
        this.workingDir = workingDir;
        this.configFile = configFile;
        this.secretKey = secretKey;
        this.tasks = tasks;
    }

    public StandardArgs(Path workingDir, String configFile, String keystoreKey) {
        this(workingDir, configFile, keystoreKey, List.of());
    }

    public Path getWorkingDir() {
        return workingDir;
    }

    public String getConfigFile() {
        return configFile;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public static Task task(String taskName, String taskDescription) {
        return new Task(taskName, List.of(), taskDescription);
    }

    public static Task task(String taskName, String taskArg, String taskDescription) {
        return new Task(taskName, taskArg, taskDescription);
    }

    public static Task task(String taskName, List<String> taskArgs, String taskDescription) {
        return new Task(taskName, taskArgs, taskDescription);
    }

    public static class Task {

        private final String taskName;
        private final List<String> taskArgs;
        private final String taskDescription;

        Task(String taskName, List<String> taskArgs, String taskDescription) {
            this.taskName = taskName;
            this.taskArgs = taskArgs != null ? taskArgs : List.of();
            this.taskDescription = taskDescription;
        }

        Task(String taskName, String taskArg, String taskDescription) {
            this (taskName, taskArg == null ? List.of() : List.of(taskArg), taskDescription);
        }

        Task(String taskName, String taskArg) {
            this(taskName, taskArg, null);
        }

        Task(String taskName) {
            this(taskName, null);
        }

        public String getTaskName() {
            return taskName;
        }

        public boolean hasArg() {
            return !taskArgs.isEmpty();
        }

        public int argCount() {
            return taskArgs.size();
        }

        public String getTaskArg() {
            return taskArgs.isEmpty() ? null : taskArgs.get(0);
        }

        public String getTaskArg(int argIndex) {
            return taskArgs.get(argIndex);
        }

        public List<String> getTaskArgList() {
            return taskArgs;
        }

        public String getTaskDescription() {
            return taskDescription;
        }
    }
}
