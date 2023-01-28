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

    private final boolean isTrac;

    private final String customCommand;
    private final List<LaunchArg> customArgs;

    public static LaunchCmd trac() {
        return new LaunchCmd("cp");
    }

    public static LaunchCmd custom(String command) {
        return new LaunchCmd(command, List.of());
    }

    public static LaunchCmd custom(String command, List<LaunchArg> args) {
        return new LaunchCmd(command, args);
    }

    private LaunchCmd(String cp) {
        this.isTrac = true;
        this.customCommand = null;
        this.customArgs = null;
    }

    private LaunchCmd(String command, List<LaunchArg> args) {
        this.isTrac = false;
        this.customCommand = command;
        this.customArgs = args;
    }

    public boolean isTrac() {
        return this.isTrac;
    }

    public String customCommand() {
        return customCommand;
    }

    public List<LaunchArg> customArgs() {
        return customArgs;
    }
}
