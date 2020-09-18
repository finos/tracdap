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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


class StandardArgsProcessorTest {

    @Test
    void testArgs_ok() {

        var command = "--config etc/my_config.props --keystore-key Mellon";
        var commandArgs = command.split("\\s");

        var standardArgs = StandardArgsProcessor.processArgs(commandArgs);

        assertEquals(System.getProperty("user.dir"), standardArgs.getWorkingDir().toString());
        assertEquals("etc/my_config.props", standardArgs.getConfigFile());
        assertEquals("Mellon", standardArgs.getKeystoreKey());
    }

    @Test
    void testArgs_empty() {

        var command = "";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(commandArgs));
        assertTrue(err.isQuiet());
        assertNotEquals(0, err.getExitCode());
    }

    @Test
    void testArgs_unknownOption() {

        var command = "--config etc/my_config.props --keystore-key Mellon --unknown option";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(commandArgs));
        assertTrue(err.isQuiet());;
        assertNotEquals(0, err.getExitCode());
    }

    @Test
    void testArgs_noConfig() {

        var command = "--keystore-key Mellon";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(commandArgs));
        assertTrue(err.isQuiet());;
        assertNotEquals(0, err.getExitCode());
    }

    @Test
    void testArgs_noConfigParam() {

        var command = "--config --keystore-key Mellon";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(commandArgs));
        assertTrue(err.isQuiet());;
        assertNotEquals(0, err.getExitCode());
    }

    @Test
    void testArgs_help() {

        var command = "--help";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(commandArgs));
        assertTrue(err.isQuiet());;
        assertEquals(0, err.getExitCode());
    }
}
