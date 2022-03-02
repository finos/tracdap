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
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.config.JobConfig;
import com.accenture.trac.config.JobResult;
import com.accenture.trac.config.RuntimeConfig;
import com.accenture.trac.metadata.JobDefinition;
import com.accenture.trac.metadata.JobType;
import com.accenture.trac.metadata.ObjectDefinition;
import com.accenture.trac.metadata.TagHeader;

import java.io.*;
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
    public Map<String, TagHeader> resourceMappings = new HashMap<>();
    public Map<String, TagHeader> resultMappings = new HashMap<>();

    public RuntimeConfig sysConfig;
    public JobConfig jobConfig;
    public JobResult jobResult;

    public JobStatusCode statusCode;

    public byte[] executorState;

    public boolean recorded = false;

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
}
