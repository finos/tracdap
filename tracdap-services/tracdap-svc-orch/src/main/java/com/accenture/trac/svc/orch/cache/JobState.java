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

import org.finos.tracdap.api.JobRequest;
import com.accenture.trac.common.exception.EUnexpected;
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

    public byte[] executorState;



    public static <T extends Serializable> byte[] serialize(T obj){

        try (var bos = new ByteArrayOutputStream();
             var oos = new ObjectOutputStream(bos)) {

            oos.writeObject(obj);
            oos.flush();

            return bos.toByteArray();
        }
        catch (IOException e) {
            throw new EUnexpected();  // TODO
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T deserialize(byte[] bytes, Class<T> clazz){

        try (var bis = new ByteArrayInputStream(bytes);
             var ois = new ObjectInputStream(bis)) {

            var obj = ois.readObject();

            if (!clazz.isInstance(obj))
                throw new EUnexpected();  // TODO

            return (T) obj;
        }
        catch (IOException | ClassNotFoundException e) {
            throw new EUnexpected();  // TODO
        }
    }

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
            throw new AssertionError();
        }
    }
}
