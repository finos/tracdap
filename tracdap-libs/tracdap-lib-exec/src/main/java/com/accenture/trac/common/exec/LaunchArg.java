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

package com.accenture.trac.common.exec;

import com.accenture.trac.common.exception.EUnexpected;

public class LaunchArg {

    public static LaunchArg path(String volume, String path) {
        return new LaunchArg(volume, path);
    }

    public static LaunchArg string(String arg) {
        return new LaunchArg(arg);
    }

    private LaunchArg(String arg) {
        this.argType = LaunchArgType.STRING;
        this.arg = arg;
        this.volume = null;
    }

    private LaunchArg(String volume, String path) {
        this.argType = LaunchArgType.PATH;
        this.arg = path;
        this.volume = volume;
    }

    public LaunchArgType getArgType() {
        return argType;
    }

    public String getStringArg() {

        if (argType != LaunchArgType.STRING)
            throw new EUnexpected();

        return arg;
    }

    public String getPathVolume() {

        if (argType != LaunchArgType.PATH)
            throw new EUnexpected();

        return volume;
    }

    public String getPathArg() {

        if (argType != LaunchArgType.PATH)
            throw new EUnexpected();

        return arg;
    }

    private final LaunchArgType argType;
    private final String arg;
    private final String volume;
}
