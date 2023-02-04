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


public class CacheStatus {

    public static final String QUEUED_IN_TRAC = "QUEUED_IN_TRAC";

    public static final String SENT_TO_EXECUTOR = "SENT_TO_EXECUTOR";
    public static final String QUEUED_IN_EXECUTOR = "QUEUED_IN_EXECUTOR";
    public static final String RUNNING_IN_EXECUTOR = "RUNNING_IN_EXECUTOR";
    public static final String EXECUTOR_SUCCEEDED = "EXECUTOR_SUCCEEDED";
    public static final String EXECUTOR_FAILED = "EXECUTOR_FAILED";
    public static final String EXECUTOR_COMPLETE = "EXECUTOR_COMPLETE";

    public static final String RESULTS_RECEIVED = "RESULTS_RECEIVED";
    public static final String RESULTS_INVALID = "RESULTS_INVALID";
    public static final String RESULTS_SAVED = "RESULTS_SAVED";

    public static final String READY_TO_REMOVE = "READY_TO_REMOVE";
}
