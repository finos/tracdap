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

import org.finos.tracdap.config.StorageConfig;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.function.Consumer;


public interface IBatchExecutor<TState extends Serializable> {

    // Interface for running batch jobs, i.e. a job that runs using one-shot using a one-shot process

    enum Feature {
        OUTPUT_VOLUMES,
        EXPOSE_PORT,
        STORAGE_MAPPING,
        CANCELLATION
    }

    void start();
    void stop();

    boolean hasFeature(Feature feature);

    TState createBatch(String batchKey);
    TState addVolume(String batchKey, TState batchState, String volumeName, BatchVolumeType volumeType);
    TState addFile(String batchKey, TState batchState, String volumeName, String fileName, byte[] fileContent);

    TState submitBatch(String batchKey, TState batchState, BatchConfig batchConfig);
    TState cancelBatch(String batchKey, TState batchState);
    void deleteBatch(String batchKey, TState batchState);

    BatchStatus getBatchStatus(String batchKey, TState batchState);

    // Optional feature - output volumes
    boolean hasOutputFile(String batchKey, TState batchState, String volumeName, String fileName);
    byte[] getOutputFile(String batchKey, TState batchState, String volumeName, String fileName);

    // Optional feature - expose port
    InetSocketAddress getBatchAddress(String batchKey, TState batchState);

    // Optional feature - storage mapping
    TState configureBatchStorage(
            String batchKey, TState batchState,
            StorageConfig storageConfig, Consumer<StorageConfig> storageUpdate);
}
