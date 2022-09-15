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

package org.finos.tracdap.common.startup;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.apache.commons.cli.*;

import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Process command line arguments for TRAC services and utilities
 * <p>
 *
 * The processor creates a StandardArgs object, which holds the
 * location of the primary configuration file, a key for unlocking
 * any secrets held in the configuration and the working directory
 * of the process. This is enough information to create a
 * ConfigManager, load in the primary configuration file and
 * initialise a TRAC service or utility.
 *
 * The processor has the ability to process --task options. These allow
 * one or more tasks to be specified on the command line and are
 * intended for use with command line utilities. To use task processing,
 * supply a list of available tasks to processArgs(). The processor will
 * only accept tasks that are in the list of available tasks. If an
 * available task is passed in with a parameter then tasks with this name
 * will accept a parameter, otherwise they will not.
 *
 * For an example of task processing, look in trac-tools/deploy-metadb.
 *
 * @see ConfigManager
 */
public class StandardArgsProcessor {

    /**
     * Read standard args from the command line.
     *
     * This variant of processArgs() does not enable task processing.
     *
     * @param appName Name of the application, displayed in help messages
     * @param args The command line args received on startup
     * @return A set of standard args suitable for creating a ConfigManager
     * @throws EStartup The command line args could not be parsed, or --help was specified
     */
    public static StandardArgs processArgs(String appName, String[] args) {

        return processArgs(appName, args, null);
    }

    /**
     * Read standard args from the command line.
     *
     * This variant of processArgs() can be used to enable task processing.
     *
     * @param appName Name of the application, displayed in help messages
     * @param args The command line args received on startup
     * @param availableTasks If present, enable task processing and supply the list of available tasks
     * @return A set of standard args suitable for creating a ConfigManager
     * @throws EStartup The command line args could not be parsed, or --help was specified
     */
    public static StandardArgs processArgs(String appName, String[] args, List<StandardArgs.Task> availableTasks) {

        var usingTasks = availableTasks != null && !availableTasks.isEmpty();
        var helpOptions = helpOptions(usingTasks);
        var options = standardOptions(usingTasks);

        try {

            var parser = new DefaultParser();
            var helpCommand = parser.parse(helpOptions, args, true);

            handleHelpCommands(appName, helpCommand, options, availableTasks);

            var command = parser.parse(options, args, false);
            var workingDir = Paths.get(".").toAbsolutePath().normalize();
            var configFile = command.getOptionValue("config");
            var keystoreKey = command.getOptionValue("keystore-key");

            var tasks = usingTasks
                    ? processTasks(command, availableTasks)
                    : null;

            return new StandardArgs(workingDir, configFile, keystoreKey, tasks);
        }
        catch (ParseException e) {

            var message = "Invalid command line: " + e.getMessage();
            System.err.println(message);

            var formatter = new HelpFormatter();
            formatter.printUsage(new PrintWriter(System.out), 80, appName, options);
            formatter.printHelp(appName, options);

            throw EStartup.quietShutdown(-1);
        }
    }

    private static void handleHelpCommands(
            String appName, CommandLine helpCommand,
            Options options, List<StandardArgs.Task> tasks) {

        if (helpCommand.hasOption("help")) {

            var formatter = new HelpFormatter();
            formatter.printHelp(appName, options);

            throw EStartup.quietShutdown(0);
        }

        if (helpCommand.hasOption("task-list")) {

            System.out.println(appName + " - available tasks:");

            for (var task : tasks) {

                var taskInfoFormat = "%-35s %s";
                var taskUsage = task.hasArg()
                        ? task.getTaskName() + ":" + task.getTaskArg()
                        : task.getTaskName();

                System.out.printf(taskInfoFormat, taskUsage, task.getTaskDescription());
                System.out.println();
            }

            throw EStartup.quietShutdown(0);
        }
    }

    private static List<StandardArgs.Task> processTasks(CommandLine command, List<StandardArgs.Task> availableTasks) {

        var taskMap = availableTasks.stream()
                .collect(Collectors.toMap(StandardArgs.Task::getTaskName, task -> task));

        var taskArgs = command.getOptionValues("task");
        var tasks = new ArrayList<StandardArgs.Task>();

        for (var argIndex = 0; argIndex < taskArgs.length; argIndex++) {

            var taskName = taskArgs[argIndex];

            if (!taskMap.containsKey(taskName))
                throw new EStartup(String.format("Unknown task: [%s]", taskName));

            var taskDefinition = taskMap.get(taskName);

            if (taskDefinition.hasArg()) {

                if (argIndex + 1 == taskArgs.length) {
                    var message = String.format(
                            "Task [%s] requires argument [%s]",
                            taskName, taskDefinition.getTaskArg());
                    throw new EStartup(message);
                }

                var taskArg = taskArgs[argIndex + 1];
                argIndex++;

                var task = new StandardArgs.Task(taskName, taskArg);
                tasks.add(task);
            }
            else {

                var task = new StandardArgs.Task(taskName);
                tasks.add(task);
            }
        }

        return List.copyOf(tasks);
    }

    private static Options standardOptions(boolean usingTasks) {

        return buildOOptions(usingTasks, false);
    }

    private static Options helpOptions(boolean usingTasks) {

        return buildOOptions(usingTasks, true);
    }

    private static Options buildOOptions(boolean usingTasks, boolean buildingHelp) {

        var options = new Options();

        options.addOption(Option.builder()
                .desc("Location of the service config file")
                .longOpt("config")
                .hasArg()
                .argName("config_file")
                .required(!buildingHelp)
                .build());

        options.addOption(Option.builder()
                .desc("Master key used to unlock the service keystore")
                .longOpt("keystore-key")
                .hasArg()
                .argName("keystore_key")
                .build());

        options.addOption(Option.builder()
                .desc("Display this help and then quit")
                .longOpt("help")
                .build());

        if (usingTasks) {

            options.addOption(Option.builder()
                    .desc("Perform a specific task")
                    .longOpt("task")
                    .hasArgs()
                    .argName("task")
                    .valueSeparator(':')
                    .required(!buildingHelp)
                    .build());

            options.addOption(Option.builder()
                    .desc("Display the list of available tasks and then quit")
                    .longOpt("task-list")
                    .build());
        }

        return options;
    }
}
