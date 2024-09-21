/*
 * Copyright 2024 Accenture Global Solutions Limited
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

import org.finos.tracdap.common.exception.ETracInternal;

import java.util.ArrayList;
import java.util.List;

public class BatchConfig {

    private final LaunchCmd launchCmd;
    private List<LaunchArg> launchArgs;

    private boolean redirectOutput;
    private LaunchArg stdOut;
    private LaunchArg stdErr;

    private BatchConfig(LaunchCmd launchCmd, List<LaunchArg> launchArgs, boolean redirectOutput, LaunchArg stdOut, LaunchArg stdErr) {
        this.launchCmd = launchCmd;
        this.launchArgs = launchArgs;
        this.redirectOutput = redirectOutput;
        this.stdOut = stdOut;
        this.stdErr = stdErr;
    }

    public static BatchConfig forCommand(LaunchCmd launchCmd, List<LaunchArg> launchArgs) {
        return new BatchConfig(launchCmd, launchArgs, false, null, null);
    }

    void addExtraArgs(List<LaunchArg> extraArgs) {

        var newLaunchArgs = new ArrayList<>(launchArgs);
        newLaunchArgs.addAll(extraArgs);

        this.launchArgs = newLaunchArgs;
    }

    void addLoggingRedirect(LaunchArg stdOut, LaunchArg stdErr) {

        if (stdOut.getArgType() != LaunchArgType.PATH || stdErr.getArgType() != LaunchArgType.PATH)
            throw new ETracInternal("Batch config for stdOut and stdErr must have arg type PATH");

        this.redirectOutput = true;
        this.stdOut = stdOut;
        this.stdErr = stdErr;
    }

    public LaunchCmd getLaunchCmd() {
        return launchCmd;
    }

    public List<LaunchArg> getLaunchArgs() {
        return launchArgs;
    }

    public boolean isRedirectOutput() {
        return redirectOutput;
    }

    public LaunchArg getStdOut() {
        return stdOut;
    }

    public LaunchArg getStdErr() {
        return stdErr;
    }
}
