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

package com.accenture.trac.svc.orch.cache;

import com.accenture.trac.api.JobRequest;
import com.accenture.trac.api.JobStatusCode;
import com.accenture.trac.metadata.JobDefinition;
import com.accenture.trac.metadata.JobType;
import com.accenture.trac.metadata.ObjectDefinition;
import com.accenture.trac.metadata.TagHeader;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class JobState implements Serializable {

    public String tenant;

    public String jobKey;
    public TagHeader jobId;
    public JobType jobType;

    public JobRequest jobRequest;

    public JobDefinition definition;
    public Map<String, ObjectDefinition> resources = new HashMap<>();

    public JobStatusCode statusCode;
}
