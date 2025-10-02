/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
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
import org.finos.tracdap.api.internal.RuntimeJobStatus;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.ResourceBundle;
import org.finos.tracdap.common.middleware.GrpcClientConfig;
import org.finos.tracdap.common.middleware.GrpcClientState;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.RuntimeConfig;
import org.finos.tracdap.metadata.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class JobState implements Serializable, Cloneable {

    // Job state is stored in the job cache using Java serialization
    // Updates must follow the rules of Java serialization compatability
    // Serialization failures can mask real errors, especially for rare error conditions
    // Note: Do not keep exceptions in job state, not all exception classes are fully serializable

    private final static long serialVersionUID = 1L;

    // Original request as received from the client
    String tenant;
    JobRequest jobRequest;

    RequestMetadata requestMetadata;
    UserMetadata userMetadata;

    // Middleware config for client calls
    // Client state is serialized with the job state and is always available
    // Client config is not saved but can be restored from client state when required
    GrpcClientState clientState;
    transient GrpcClientConfig clientConfig;

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

    // Job definition
    JobDefinition definition;

    // Referenced metadata and resources are restored from job config / sys config
    // Do not leak extra serialization dependencies into -lib-common
    transient MetadataBundle metadata;
    transient ResourceBundle resources;

    // No longer used
    private Map<String, TagHeader> objectMapping = null;
    private Map<String, ObjectDefinition> objects = null;
    private Map<String, Tag> tags = null;

    TagHeader resultId;
    List<TagHeader> preallocatedIds = new ArrayList<>();

    // Input / output config files for communicating with the runtime
    RuntimeConfig sysConfig;
    JobConfig jobConfig;

    // Executor state data
    Serializable executorState;

    // Status and result received back from the runtime
    RuntimeJobStatus runtimeStatus;
    JobResult runtimeResult;

    // Final result after post-processing
    JobResult jobResult;

    @Override
    public JobState clone() {

        try {

            var clone = (JobState) super.clone();
            clone.preallocatedIds = new ArrayList<>(this.preallocatedIds);

            return clone;
        }
        catch (CloneNotSupportedException e) {
            throw new EUnexpected(e);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {

        // Default deserialization
        in.defaultReadObject();

        // Restore metadata and resource bundles from job / sys config

        if (jobConfig != null) {
            var objectMapping = jobConfig.getObjectMappingMap();
            var objects = jobConfig.getObjectsMap();
            var tags = jobConfig.getTagsMap();
            this.metadata = new MetadataBundle(objectMapping, objects, tags);
        }
        else {
            this.metadata = MetadataBundle.empty();
        }

        if (sysConfig != null) {
            var resources = sysConfig.getResourcesMap();
            this.resources = new ResourceBundle(resources);
        }
        else {
            this.resources = ResourceBundle.empty();
        }
    }
}
