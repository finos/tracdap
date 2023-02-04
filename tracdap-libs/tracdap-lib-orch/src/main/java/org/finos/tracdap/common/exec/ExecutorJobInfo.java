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

package org.finos.tracdap.common.exec;

public class ExecutorJobInfo {

    private final ExecutorJobStatus status;
    private final String statusMessage;
    private final String errorDetail;

    public ExecutorJobInfo(ExecutorJobStatus status) {
        this(status, "", "");
    }

    public ExecutorJobInfo(ExecutorJobStatus status, String statusMessage, String errorDetail) {
        this.status = status;
        this.statusMessage = statusMessage;
        this.errorDetail = errorDetail;
    }

    public ExecutorJobStatus getStatus() {
        return status;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getErrorDetail() {
        return errorDetail;
    }
}
