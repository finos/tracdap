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

package org.finos.tracdap.common.service;

import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.metadata.JobDefinition;

/**
 * A job being validated, as seen by an {@link IJobValidationHook}.
 * The metadata bundle holds every object the job refers to, including its context, keyed by the selectors in the job.
 */
public class JobValidation {

    private final String tenant;
    private final JobDefinition definition;
    private final MetadataBundle metadata;
    private final UserMetadata userMetadata;

    public JobValidation(String tenant, JobDefinition definition, MetadataBundle metadata, UserMetadata userMetadata) {
        this.tenant = tenant;
        this.definition = definition;
        this.metadata = metadata;
        this.userMetadata = userMetadata;
    }

    public String tenant() {
        return tenant;
    }

    public JobDefinition definition() {
        return definition;
    }

    public MetadataBundle metadata() {
        return metadata;
    }

    public UserMetadata userMetadata() {
        return userMetadata;
    }
}
