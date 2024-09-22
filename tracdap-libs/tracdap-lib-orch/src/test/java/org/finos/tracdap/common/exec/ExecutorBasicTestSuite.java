/*
 * Copyright 2023 Accenture Global Solutions Limited
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

import org.finos.tracdap.common.util.ResourceHelpers;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


public abstract class ExecutorBasicTestSuite {

    public static final String LOREM_IPSUM_TEST_RESOURCE = "/lorem_ipsum.txt";

    protected IBatchExecutor<?> executor;

    @SuppressWarnings("unchecked")
    private <T extends Serializable> IBatchExecutor<T> stronglyTypedExecutor() {
        return (IBatchExecutor<T>) executor;
    }

    protected abstract boolean targetIsWindows();

    @Test
    void runBasicJob_ok() throws Exception {

        var jobKey = UUID.randomUUID().toString();

        // Create a new batch

        var batchExecutor = stronglyTypedExecutor();
        var batchState = batchExecutor.createBatch(jobKey);

        try {

            // Set up volumes

            batchState = batchExecutor.addVolume(jobKey, batchState, "config", BatchVolumeType.CONFIG_VOLUME);
            batchState = batchExecutor.addVolume(jobKey, batchState, "outputs", BatchVolumeType.OUTPUT_VOLUME);

            // Write a test file into the config volume

            var inputBytes = ResourceHelpers.loadResourceAsBytes(LOREM_IPSUM_TEST_RESOURCE, ExecutorBasicTestSuite.class);
            batchState = batchExecutor.addFile(jobKey, batchState, "config", "lorem_ipsum.txt", inputBytes);

            // Set up a basic copy command

            BatchConfig batchConfig;

            if (targetIsWindows()) {

                batchConfig = BatchConfig.forCommand(LaunchCmd.custom("cmd"), List.of(
                        LaunchArg.string("/C"), LaunchArg.string("copy"),
                        LaunchArg.path("config", "lorem_ipsum.txt"),
                        LaunchArg.path("outputs", "lorem_ipsum_copy.txt")));
            }
            else {

                batchConfig = BatchConfig.forCommand(LaunchCmd.custom("cp"), List.of(
                        LaunchArg.string("-v"),
                        LaunchArg.path("config", "lorem_ipsum.txt"),
                        LaunchArg.path("outputs", "lorem_ipsum_copy.txt")));
            }

            // Start the batch

            batchState = batchExecutor.submitBatch(jobKey, batchState, batchConfig);

            TimeUnit.MILLISECONDS.sleep(500);

            var result = batchExecutor.getBatchStatus(jobKey, batchState);
            Assertions.assertEquals(BatchStatusCode.SUCCEEDED, result.getStatusCode());

            var outputBytes = batchExecutor.getOutputFile(jobKey, batchState, "outputs", "lorem_ipsum_copy.txt");
            Assertions.assertArrayEquals(inputBytes, outputBytes);
        }
        finally {
            batchExecutor.deleteBatch(jobKey, batchState);
        }
    }

    @Test
    void runBasicJob_processFailure() throws Exception {

        var jobKey = UUID.randomUUID().toString();

        // Create a new batch

        var batchExecutor = stronglyTypedExecutor();
        var batchState = batchExecutor.createBatch(jobKey);

        try {

            // Set up volumes

            batchState = batchExecutor.addVolume(jobKey, batchState, "config", BatchVolumeType.CONFIG_VOLUME);
            batchState = batchExecutor.addVolume(jobKey, batchState, "outputs", BatchVolumeType.OUTPUT_VOLUME);

            // Do not prepare input file, let it be missing

            // Set up a basic copy command

            BatchConfig batchConfig;

            if (targetIsWindows()) {

                batchConfig = BatchConfig.forCommand(LaunchCmd.custom("cmd"), List.of(
                        LaunchArg.string("/C"), LaunchArg.string("copy"),
                        LaunchArg.path("config", "lorem_ipsum.txt"),
                        LaunchArg.path("outputs", "lorem_ipsum_copy.txt")));
            }
            else {

                batchConfig = BatchConfig.forCommand(LaunchCmd.custom("cp"), List.of(
                        LaunchArg.string("-v"),
                        LaunchArg.path("config", "lorem_ipsum.txt"),
                        LaunchArg.path("outputs", "lorem_ipsum_copy.txt")));
            }

            // Start the batch

            batchState = batchExecutor.submitBatch(jobKey, batchState, batchConfig);

            TimeUnit.MILLISECONDS.sleep(500);

            var result = batchExecutor.getBatchStatus(jobKey, batchState);
            Assertions.assertEquals(BatchStatusCode.FAILED, result.getStatusCode());
        }
        finally {
            batchExecutor.deleteBatch(jobKey, batchState);
        }
    }
}
