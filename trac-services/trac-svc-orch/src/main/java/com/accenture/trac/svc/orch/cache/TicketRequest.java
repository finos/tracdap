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

import com.accenture.trac.common.metadata.MetadataUtil;
import com.accenture.trac.metadata.TagHeader;

public class TicketRequest {

    private final String jobKey;

    private TicketRequest(String jobKey) {
        this.jobKey = jobKey;
    }

    private TicketRequest(TagHeader jobId) {
        this.jobKey = MetadataUtil.objectKey(jobId);
    }

    public static TicketRequest forJob(String jobKey) {
        return new TicketRequest(jobKey);
    }

    public static TicketRequest forJob(TagHeader jobId) {
        return new TicketRequest(jobId);
    }

    public String jobKey() {
        return jobKey;
    }
}
