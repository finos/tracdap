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

package org.finos.tracdap.svc.orch.jobs;

import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EValidationGap;
import org.finos.tracdap.metadata.JobType;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class JobLogic {

    private static final Map<JobType, Constructor<? extends IJobLogic>> JOB_TYPES;

    static {
        try {
            JOB_TYPES = Map.ofEntries(
                    Map.entry(JobType.IMPORT_MODEL, ImportModelJob.class.getDeclaredConstructor()),
                    Map.entry(JobType.RUN_MODEL, RunModelJob.class.getDeclaredConstructor()));
        }
        catch (NoSuchMethodException e) {

            throw new ETracInternal("Invalid job logic class, default constructor not available", e);
        }
    }

    public static IJobLogic forJobType(JobType jobType) {

        try {

            var logicClass = JOB_TYPES.get(jobType);

            // TODO: Is this the right error type?
            if (logicClass == null) {
                var err = String.format("Unrecognized job type: [%s]", jobType);
                throw new EValidationGap(err);
            }

            return logicClass.newInstance();
        }
        catch ( InstantiationException |
                IllegalAccessException |
                InvocationTargetException e) {

            throw new ETracInternal("Invalid job logic class, default constructor not available", e);
        }
    }
}
