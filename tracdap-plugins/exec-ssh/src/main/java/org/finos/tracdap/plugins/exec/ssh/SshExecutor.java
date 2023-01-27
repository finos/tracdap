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

package org.finos.tracdap.plugins.exec.ssh;

import com.google.protobuf.Parser;
import org.finos.tracdap.common.exec.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;


public class SshExecutor implements IBatchExecutor<SshBatchState> {

    public SshExecutor(Properties properties) {

    }

    @Override
    public void executorStatus() {

    }

    @Override
    public Parser<SshBatchState> stateDecoder() {
        return null;
    }

    @Override
    public SshBatchState createBatch(String batchKey) {
        return null;
    }

    @Override
    public void destroyBatch(String batchKey, SshBatchState batchState) {

    }

    @Override
    public SshBatchState createVolume(String batchKey, SshBatchState batchState, String volumeName, ExecutorVolumeType volumeType) {
        return null;
    }

    @Override
    public SshBatchState writeFile(String batchKey, SshBatchState batchState, String volumeName, String fileName, byte[] fileContent) {
        return null;
    }

    @Override
    public byte[] readFile(String batchKey, SshBatchState batchState, String volumeName, String fileName) {
        return new byte[0];
    }

    @Override
    public SshBatchState startBatch(String batchKey, SshBatchState batchState, LaunchCmd launchCmd, List<LaunchArg> launchArgs) {
        return null;
    }

    @Override
    public SshBatchState cancelBatch(String batchKey, SshBatchState batchState) {
        return null;
    }

    @Override
    public ExecutorPollResult<SshBatchState> pollBatch(String batchKey, SshBatchState batchState) {
        return null;
    }

    @Override
    public List<ExecutorPollResult<SshBatchState>> pollAllBatches(Map<String, SshBatchState> priorStates) {
        return null;
    }
}
