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

package org.finos.tracdap.svc.orch.cache;

import org.finos.tracdap.api.JobRequest;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.RuntimeConfig;
import org.finos.tracdap.metadata.JobDefinition;
import org.finos.tracdap.metadata.JobType;
import org.finos.tracdap.metadata.JobStatusCode;
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.metadata.TagHeader;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


public class JobState implements Serializable, Cloneable {

    public String tenant;
    public UserInfo owner;
    public String ownerToken;

    public JobRequest jobRequest;
    public String jobKey;
    public TagHeader jobId;
    public JobType jobType;

    public JobStatusCode statusCode;
    public String statusMessage;

    public Exception exception;

    public JobDefinition definition;
    public Map<String, ObjectDefinition> resources = new HashMap<>();
    public Map<String, TagHeader> resourceMapping = new HashMap<>();
    public Map<String, TagHeader> resultMapping = new HashMap<>();

    public RuntimeConfig sysConfig;
    public JobConfig jobConfig;
    public JobResult jobResult;

    public byte[] batchState;


    @Override
    public JobState clone() {
        try {
            JobState clone = (JobState) super.clone();

            clone.resources = new HashMap<>(this.resources);
            clone.resourceMapping = new HashMap<>(this.resourceMapping);
            clone.resultMapping = new HashMap<>(this.resultMapping);

            return clone;
        }
        catch (CloneNotSupportedException e) {
            throw new EUnexpected(e);
        }
    }
}
