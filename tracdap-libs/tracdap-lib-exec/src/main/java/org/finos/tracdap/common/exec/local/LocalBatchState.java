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

package org.finos.tracdap.common.exec.local;

import org.finos.tracdap.common.exec.ExecutorState;

import java.util.ArrayList;
import java.util.List;


public class LocalBatchState extends ExecutorState {

    private String batchDir;
    private List<String> volumes = new ArrayList<>();
    private long pid;

    public LocalBatchState(String jobKey) {
        super(jobKey);
    }

    String getBatchDir() {
        return batchDir;
    }

    void setBatchDir(String batchDir) {
        this.batchDir = batchDir;
    }

    long getPid() {
        return pid;
    }

    void setPid(long pid) {
        this.pid = pid;
    }

    List<String> getVolumes() { return volumes; }
}
