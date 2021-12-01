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

package com.accenture.trac.common.exception;


/**
 * EStartup is an exception that can be raised during the startup sequence
 * to signal a startup failure. It should always be fatal.
 *
 * Examples of things that can cause EStartup exceptions are invalid configuration
 * or problems connecting to key services during startup.
 *
 * Generally EStartup will cause the main thread to exit with a stack trace. Any
 * resources that are already allocated should be closed.
 */
public class EStartup extends ETracPublic {

    private final int exitCode;
    private final boolean quiet;

    public EStartup(String message, int exitCode, Throwable cause) {
        super(message, cause);
        this.exitCode = exitCode;
        this.quiet = false;
    }

    public EStartup(String message, int exitCode) {
        this(message, exitCode, null);
    }

    public EStartup(String message, Throwable cause) {
        this(message, -1, cause);
    }

    public EStartup(String message) {
        this(message, -1);
    }

    private EStartup(int exitCode, boolean quiet) {
        super("Quiet shutdown");
        this.exitCode = exitCode;
        this.quiet = quiet;
    }

    /**
     * Using the quiet shutdown flag is a signal not to print extra error info to the console or log
     *
     * This can be useful if error information is already printed at the site where the exception is
     * raised and no further processing is needed.
     *
     * @param exitCode The exit code to use when the process terminates
     * @return An EStartup exception ready to be thrown
     */
    public static EStartup quietShutdown(int exitCode) {
        return new EStartup(exitCode, true);
    }

    public int getExitCode() {
        return exitCode;
    }

    public boolean isQuiet() {
        return quiet;
    }
}
