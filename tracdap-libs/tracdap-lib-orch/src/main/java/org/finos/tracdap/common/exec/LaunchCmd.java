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

package org.finos.tracdap.common.exec;

import java.util.List;


public class LaunchCmd {

    private static final String TRAC_PYTHON_CMD = "python";

    private static final List<LaunchArg> TRAC_PYTHON_ARGS = List.of(
            LaunchArg.string("-m"),
            LaunchArg.string("tracdap.rt.launch"));

    private final boolean isTrac;
    private final String command;
    private final List<LaunchArg> commandArgs;

    public static LaunchCmd trac() {
        return new LaunchCmd(TRAC_PYTHON_CMD, TRAC_PYTHON_ARGS, true);
    }

    public static LaunchCmd custom(String command) {
        return new LaunchCmd(command, List.of(), false);
    }

    public static LaunchCmd custom(String command, List<LaunchArg> args) {
        return new LaunchCmd(command, args, false);
    }

    private LaunchCmd(String command, List<LaunchArg> commandArgs, boolean isTrac) {
        this.isTrac = isTrac;
        this.command = command;
        this.commandArgs = commandArgs;
    }

    public boolean isTrac() {
        return this.isTrac;
    }

    public String command() {
        return command;
    }

    public List<LaunchArg> commandArgs() {
        return commandArgs;
    }
}
