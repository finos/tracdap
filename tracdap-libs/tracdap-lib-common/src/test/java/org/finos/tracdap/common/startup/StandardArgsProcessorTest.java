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

import org.finos.tracdap.common.exception.EStartup;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


class StandardArgsProcessorTest {

    private static final String APP_NAME = "Test App";

    @Test
    void testArgs_ok() {

        var command = "--config etc/my_config.props --secret-key Mellon";
        var commandArgs = command.split("\\s");

        var standardArgs = StandardArgsProcessor.processArgs(APP_NAME, commandArgs, null);

        assertEquals(System.getProperty("user.dir"), standardArgs.getWorkingDir().toString());
        assertEquals("etc/my_config.props", standardArgs.getConfigFile());
        assertEquals("Mellon", standardArgs.getSecretKey());
    }

    @Test
    void testArgs_envVariables_ok() {

        Map<String, String> envVariables = Collections.singletonMap("SECRET_KEY", "Mellon");

        var command = "--config etc/my_config.props";
        var commandArgs = command.split("\\s");

        var standardArgs = StandardArgsProcessor.processArgs(APP_NAME, commandArgs, envVariables);

        assertEquals(System.getProperty("user.dir"), standardArgs.getWorkingDir().toString());
        assertEquals("etc/my_config.props", standardArgs.getConfigFile());
        assertEquals("Mellon", standardArgs.getSecretKey());
    }

    @Test
    void testArgs_argsBeatConfig_ok() {

        Map<String, String> envVariables = Collections.singletonMap("SECRET_KEY", "Fruit");

        var command = "--config etc/my_config.props --secret-key Mellon";
        var commandArgs = command.split("\\s");

        var standardArgs = StandardArgsProcessor.processArgs(APP_NAME, commandArgs, envVariables);

        assertEquals(System.getProperty("user.dir"), standardArgs.getWorkingDir().toString());
        assertEquals("etc/my_config.props", standardArgs.getConfigFile());
        assertEquals("Mellon", standardArgs.getSecretKey());
    }

    @Test
    void testArgs_empty() {

        var command = "";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs, null));
        assertTrue(err.isQuiet());
        assertNotEquals(0, err.getExitCode());
    }

    @Test
    void testArgs_unknownOption() {

        var command = "--config etc/my_config.props --secret-key Mellon --unknown option";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs, null));
        assertTrue(err.isQuiet());
        assertNotEquals(0, err.getExitCode());
    }

    @Test
    void testArgs_noConfig() {

        var command = "--secret-key Mellon";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs, null));
        assertTrue(err.isQuiet());
        assertNotEquals(0, err.getExitCode());
    }

    @Test
    void testArgs_noConfigParam() {

        var command = "--config --secret-key Mellon";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs, null));
        assertTrue(err.isQuiet());
        assertNotEquals(0, err.getExitCode());
    }

    @Test
    void testArgs_help() {

        var command = "--help";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs, null));
        assertTrue(err.isQuiet());
        assertEquals(0, err.getExitCode());
    }

    @Test
    void testTasks_noTasks() {

        var command = "--task_list";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs, null));
        assertTrue(err.isQuiet());
        assertNotEquals(0, err.getExitCode());

        var command2 = "--config app.conf --task do_something";
        var commandArgs2 = command2.split("\\s");

        var err2 = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs2, null));
        assertTrue(err2.isQuiet());
        assertNotEquals(0, err2.getExitCode());
    }

    @Test
    void testTasks_taskList() {

        var TASKS = List.of(StandardArgs.task("do_something", "desc"));

        var command = "--task-list";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs, TASKS, null));
        assertTrue(err.isQuiet());
        assertEquals(0, err.getExitCode());
    }

    @Test
    void testTasks_basicTask() {

        var TASKS = List.of(StandardArgs.task("do_something", "desc"));

        var command = "--config app.conf --task do_something";
        var commandArgs = command.split("\\s");

        var standardArgs = StandardArgsProcessor.processArgs(APP_NAME, commandArgs, TASKS, null);
        var tasks = standardArgs.getTasks();

        assertEquals(1, tasks.size());
        assertEquals("do_something", tasks.get(0).getTaskName());
        assertNull(tasks.get(0).getTaskArg());
    }

    @Test
    void testTasks_basicTaskUnexpectedArg() {

        var TASKS = List.of(StandardArgs.task("do_something", "desc"));

        var command = "--config app.conf --task do_something ARG";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs, TASKS, null));
        assertFalse(err.isQuiet());
        assertNotEquals(0, err.getExitCode());
    }

    @Test
    void testTasks_argTask() {

        var TASKS = List.of(StandardArgs.task("do_something", "ARG_NAME", "desc"));

        var command = "--config app.conf --task do_something ARG_VALUE";
        var commandArgs = command.split("\\s");

        var standardArgs = StandardArgsProcessor.processArgs(APP_NAME, commandArgs, TASKS, null);
        var tasks = standardArgs.getTasks();

        assertEquals(1, tasks.size());
        assertEquals("do_something", tasks.get(0).getTaskName());
        assertEquals("ARG_VALUE", tasks.get(0).getTaskArg());
    }

    @Test
    void testTasks_argTaskWithoutArg() {

        var TASKS = List.of(StandardArgs.task("do_something", "ARG_NAME", "desc"));

        var command = "--config app.conf --task do_something";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs, TASKS, null));
        assertFalse(err.isQuiet());
        assertNotEquals(0, err.getExitCode());
    }

    @Test
    void testTasks_argTaskExtraArg() {

        var TASKS = List.of(StandardArgs.task("do_something", "ARG_NAME", "desc"));

        var command = "--config app.conf --task do_something ARG1 ARG2";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs, TASKS, null));
        assertFalse(err.isQuiet());
        assertNotEquals(0, err.getExitCode());
    }

    @Test
    void testTasks_multipleTasks() {

        var TASKS = List.of(
                StandardArgs.task("do_something", "desc"),
                StandardArgs.task("do_something_else", "ARG_NAME", "desc"));

        var command = "--config app.conf --task do_something --task do_something_else ARG1 --task do_something_else ARG2";
        var commandArgs = command.split("\\s");

        var standardArgs = StandardArgsProcessor.processArgs(APP_NAME, commandArgs, TASKS, null);
        var tasks = standardArgs.getTasks();

        assertEquals(3, tasks.size());
        assertEquals("do_something", tasks.get(0).getTaskName());
        assertNull(tasks.get(0).getTaskArg());
        assertEquals("do_something_else", tasks.get(1).getTaskName());
        assertEquals("ARG1", tasks.get(1).getTaskArg());
        assertEquals("do_something_else", tasks.get(2).getTaskName());
        assertEquals("ARG2", tasks.get(2).getTaskArg());
    }

    @Test
    void testTasks_taskMultiArg() {

        var TASKS = List.of(
                StandardArgs.task("do_something", List.of("ARG1", "ARG2"), "desc"),
                StandardArgs.task("do_something_else", "ARG3", "desc"));

        var command = "--config app.conf --task do_something arg_1 arg_2 --task do_something_else arg_3";
        var commandArgs = command.split("\\s");

        var standardArgs = StandardArgsProcessor.processArgs(APP_NAME, commandArgs, TASKS, null);
        var tasks = standardArgs.getTasks();

        assertEquals(2, tasks.size());
        assertEquals("do_something", tasks.get(0).getTaskName());
        assertEquals(2, tasks.get(0).argCount());
        assertEquals("arg_1", tasks.get(0).getTaskArg(0));
        assertEquals("arg_2", tasks.get(0).getTaskArg(1));
        assertEquals("do_something_else", tasks.get(1).getTaskName());
        assertEquals(1, tasks.get(1).argCount());
        assertEquals("arg_3", tasks.get(1).getTaskArg());
    }

    @Test
    void testTasks_unknownTask() {

        var TASKS = List.of(StandardArgs.task("do_something", "ARG_NAME", "desc"));

        var command = "--config app.conf --task do_something_else";
        var commandArgs = command.split("\\s");

        var err = assertThrows(EStartup.class, () -> StandardArgsProcessor.processArgs(APP_NAME, commandArgs, TASKS, null));
        assertFalse(err.isQuiet());
        assertNotEquals(0, err.getExitCode());
    }
}
