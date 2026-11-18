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

package org.finos.tracdap.svc.orch.test;

import org.finos.tracdap.common.exception.EConsistencyValidation;
import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.service.IJobValidationHook;
import org.finos.tracdap.common.service.IMetadataWriteHook;
import org.finos.tracdap.common.service.JobValidation;
import org.finos.tracdap.common.service.ObjectWrite;
import org.finos.tracdap.metadata.ObjectType;

import com.google.protobuf.ByteString;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


public class TestJobHooks implements IJobValidationHook, IMetadataWriteHook {

    public static final ByteString REFUSED_JOB_CONTEXT = ByteString.copyFromUtf8("test_refused_job");
    public static final String REFUSED_JOB_MESSAGE = "Refused by the test job validation hook";

    public static final ByteString REFUSED_OBJECT = ByteString.copyFromUtf8("test_refused_object");
    public static final String REFUSED_OBJECT_MESSAGE = "Refused by the test metadata write hook";

    public static final List<JobValidation> VALIDATIONS = new CopyOnWriteArrayList<>();

    @Override
    public void validate(JobValidation validation) {

        VALIDATIONS.add(validation);

        var job = validation.definition();

        if (!job.hasContext())
            return;

        if (!validation.metadata().hasObject(job.getContext()))
            throw new EInputValidation("Job context is missing from the metadata bundle");

        var context = validation.metadata().getObject(job.getContext());

        if (context.getCustom().getCustomData().equals(REFUSED_JOB_CONTEXT))
            throw new EConsistencyValidation(REFUSED_JOB_MESSAGE);
    }

    @Override
    public void validateObject(ObjectWrite write) {

        if (write.objectType() == ObjectType.CUSTOM &&
            write.definition().getCustom().getCustomData().equals(REFUSED_OBJECT)) {

            throw new EConsistencyValidation(REFUSED_OBJECT_MESSAGE);
        }
    }
}
