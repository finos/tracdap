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

package org.finos.tracdap.common.exec.local;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class LocalBatchState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String batchDir;
    private final List<String> volumes;
    private final long pid;

    public LocalBatchState(String batchDir) {
        this.batchDir = batchDir;
        this.volumes = List.of();
        this.pid = 0L;
    }

    public LocalBatchState(String batchDir, List<String> volumes, long pid) {
        this.batchDir = batchDir;
        this.volumes = volumes;
        this.pid = pid;
    }

    public LocalBatchState withVolume(String newVolume) {
        var newVolumes = new ArrayList<String>(volumes.size() + 1);
        newVolumes.addAll(volumes);
        newVolumes.add(newVolume);
        return new LocalBatchState(batchDir, newVolumes, pid);
    }

    public LocalBatchState withPid(long newPid) {
        return new LocalBatchState(batchDir, volumes, newPid);
    }

    public String getBatchDir() {
        return batchDir;
    }

    public List<String> getVolumes() {
        return volumes;
    }

    public long getPid() {
        return pid;
    }
}
