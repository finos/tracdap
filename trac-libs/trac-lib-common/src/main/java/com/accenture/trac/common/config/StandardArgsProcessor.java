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

package com.accenture.trac.common.config;

import com.accenture.trac.common.exception.EStartup;
import org.apache.commons.cli.*;

import java.io.PrintWriter;
import java.nio.file.Paths;


/**
 * Process command line arguments for TRAC services and utilities
 * <p>
 *
 * The processor creates a StandardArgs object, which holds the
 * location of the primary configuration file, a key for unlocking
 * any secrets held in the configuraiton and the working directory
 * of the process. This is enough information to create a
 * ConfigManager, load in the primary configuration file and
 * initialise a TRAC service or utility.
 *
 * @see ConfigManager
 */
public class StandardArgsProcessor {

    /**
     * Read standard args from the command line
     *
     * @param args The command line args received on startup
     * @return A set of standard args suitable for creating a ConfigManager
     * @throws EStartup The command line args could not be parsed, or --help was specified
     */
    public static StandardArgs processArgs(String[] args) {

        var helpOptions = helpOptions();
        var options = standardOptions();

        try {

            var parser = new DefaultParser();
            var helpCommand = parser.parse(helpOptions, args, true);

            if (helpCommand.hasOption("help")) {

                // TODO: get $0
                var formatter = new HelpFormatter();
                formatter.printHelp("TRAC Metadata Service", options);

                throw EStartup.quietShutdown(0);
            }

            var command = parser.parse(options, args, false);
            var workingDir = Paths.get(".").toAbsolutePath().normalize();
            var configFile = command.getOptionValue("config");
            var keystoreKey = command.getOptionValue("keystore-key");


            return new StandardArgs(workingDir, configFile, keystoreKey);
        }
        catch (ParseException e) {

            var message = "Invalid command line: " + e.getMessage();
            System.err.println(message);

            // TODO: Get component name
            var formatter = new HelpFormatter();
            formatter.printUsage(new PrintWriter(System.out), 80, "TRAC Metadata Service", options);
            formatter.printHelp("TRAC Metadata Service", options);

            throw EStartup.quietShutdown(-1);
        }
    }

    private static Options standardOptions() {

        var options = helpOptions();

        options.addOption(Option.builder()
                .longOpt("config")
                .desc("Location of the service config file")
                .hasArg()
                .argName("config_file")
                .required()
                .build());

        options.addOption(Option.builder()
                .longOpt("keystore-key")
                .desc("Master key used to unlock the service keystore")
                .hasArg()
                .argName("keystore_key")
                // .required()
                .build());

        return options;
    }

    private static Options helpOptions() {

        var options = new Options();

        options.addOption(Option.builder()
                .longOpt("help")
                .desc("Display this help and then quit")
                .build());

        return options;
    }
}
