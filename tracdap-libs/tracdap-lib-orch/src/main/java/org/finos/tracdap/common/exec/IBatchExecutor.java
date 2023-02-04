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

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.List;
import java.util.Map;


public interface IBatchExecutor<TState extends Message> {

    // Interface for running batch jobs, i.e. a job that runs using one-shot using a one-shot process

    void start();

    void stop();

    Parser<TState> stateDecoder();

    TState createBatch(String batchKey);

    void destroyBatch(String batchKey, TState batchState);

    TState createVolume(String batchKey, TState batchState, String volumeName, ExecutorVolumeType volumeType);

    TState writeFile(String batchKey, TState batchState, String volumeName, String fileName, byte[] fileContent);

    byte[] readFile(String batchKey, TState batchState, String volumeName, String fileName);

    TState startBatch(String batchKey, TState batchState, LaunchCmd launchCmd, List<LaunchArg> launchArgs);

    ExecutorJobInfo pollBatch(String batchKey, TState batchState);

    List<ExecutorJobInfo> pollBatches(List<Map.Entry<String, TState>> batches);

}
