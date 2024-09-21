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

package org.finos.tracdap.svc.orch.service;

import org.finos.tracdap.api.JobRequest;
import org.finos.tracdap.api.internal.RuntimeJobResult;
import org.finos.tracdap.api.internal.RuntimeJobStatus;
import org.finos.tracdap.common.auth.internal.InternalCallCredentials;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.JobConfig;
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

    // Note: Do not keep exceptions in job state, not all exception classes are fully serializable
    // Serialization failures can mask real errors, especially for rare error conditions

    private final static long serialVersionUID = 1L;

    // Original request as received from the client
    UserInfo owner;
    String tenant;
    JobRequest jobRequest;

    // Internal credentials used to authenticate within the TRAC platform
    // Do not serialize to the cache, can be recreated using the internal key as a delegate
    transient InternalCallCredentials credentials;

    // Identifiers
    String jobKey;
    TagHeader jobId;
    JobType jobType;

    // Status information
    JobStatusCode tracStatus;
    String cacheStatus;
    String statusMessage;
    String errorDetail;
    int retries;

    // Job definition and resources, built up by the job logic
    JobDefinition definition;
    Map<String, ObjectDefinition> resources = new HashMap<>();
    Map<String, TagHeader> resourceMapping = new HashMap<>();
    Map<String, TagHeader> resultMapping = new HashMap<>();

    // Input / output config files for communicating with the runtime
    JobConfig jobConfig;
    RuntimeConfig sysConfig;

    RuntimeJobStatus executorStatus;
    RuntimeJobResult executorResult;

    // Executor state data
    Serializable executorState;

    @Override
    public JobState clone() {

        try {

            var clone = (JobState) super.clone();

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
