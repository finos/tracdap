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

package org.finos.tracdap.svc.orch.test;

import org.finos.tracdap.common.exec.*;

import java.util.List;
import java.util.Map;


public class UnitTestExecutor implements IBatchExecutor<UnitTestExecutorState> {

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Class<UnitTestExecutorState> stateClass() {
        return null;
    }

    @Override
    public UnitTestExecutorState createBatch(String batchKey) {
        return null;
    }

    @Override
    public void destroyBatch(String batchKey, UnitTestExecutorState batchState) {

    }

    @Override
    public UnitTestExecutorState createVolume(String batchKey, UnitTestExecutorState batchState, String volumeName, ExecutorVolumeType volumeType) {
        return null;
    }

    @Override
    public UnitTestExecutorState writeFile(String batchKey, UnitTestExecutorState batchState, String volumeName, String fileName, byte[] fileContent) {
        return null;
    }

    @Override
    public byte[] readFile(String batchKey, UnitTestExecutorState batchState, String volumeName, String fileName) {
        return new byte[0];
    }

    @Override
    public UnitTestExecutorState startBatch(String batchKey, UnitTestExecutorState batchState, LaunchCmd launchCmd, List<LaunchArg> launchArgs) {
        return null;
    }

    @Override
    public ExecutorJobInfo pollBatch(String batchKey, UnitTestExecutorState batchState) {
        return null;
    }

    @Override
    public List<ExecutorJobInfo> pollBatches(List<Map.Entry<String, UnitTestExecutorState>> batches) {
        return List.of();
    }
}
