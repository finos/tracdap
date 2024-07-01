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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class SshExecutorState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String remoteHost;
    private final int remotePort;
    private final String batchUser;
    private final String batchDir;
    private final List<String> volumes;

    private final long pid;

    public SshExecutorState(String remoteHost, int remotePort, String batchUser, String batchDir) {

        this(remoteHost, remotePort, batchUser, batchDir, List.of(), 0);
    }

    public SshExecutorState withVolume(String newVolume) {

        var newVolumes = new ArrayList<String>(volumes.size() + 1);
        newVolumes.addAll(volumes);
        newVolumes.add(newVolume);

        return new SshExecutorState(
                remoteHost, remotePort, batchUser, batchDir,
                Collections.unmodifiableList(newVolumes),
                pid);
    }

    public SshExecutorState withPid(long newPid) {

        return new SshExecutorState(remoteHost, remotePort, batchUser, batchDir, volumes, newPid);
    }

    private SshExecutorState(
            String remoteHost, int remotePort,
            String batchUser, String batchDir,
            List<String> volumes, long pid) {

        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.batchUser = batchUser;
        this.batchDir = batchDir;
        this.volumes = volumes;
        this.pid = pid;
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public int getRemotePort() {
        return remotePort;
    }

    public String getBatchUser() {
        return batchUser;
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
