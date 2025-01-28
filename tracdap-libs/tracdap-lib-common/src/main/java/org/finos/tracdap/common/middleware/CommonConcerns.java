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

package org.finos.tracdap.common.middleware;

import org.finos.tracdap.common.exception.ETracInternal;
import java.util.*;

public abstract class CommonConcerns<TConcern extends BaseConcern> implements BaseConcern {

    protected final List<String> stageOrder;
    protected final Map<String, TConcern> stages;

    private final String concernName;

    protected CommonConcerns(String concernName) {
        this.stageOrder = new ArrayList<>();
        this.stages = new HashMap<>();
        this.concernName = concernName;
    }

    protected CommonConcerns(String concernName, List<String> stageOrder, Map<String, TConcern> stages) {
        this.stageOrder = stageOrder;
        this.stages = stages;
        this.concernName = concernName;
    }

    public abstract TConcern build();

    public CommonConcerns<TConcern> addFirst(TConcern concern) {

        if (stages.containsKey(concern.concernName()))
            throw new ETracInternal("Common concerns already contains stage: " + concern.concernName());

        stageOrder.add(0, concern.concernName());
        stages.put(concern.concernName(), concern);

        return this;
    }

    public CommonConcerns<TConcern> addLast(TConcern concern) {

        if (stages.containsKey(concern.concernName()))
            throw new ETracInternal("Common concerns already contains stage: " + concern.concernName());

        stageOrder.add(concern.concernName());
        stages.put(concern.concernName(), concern);

        return this;
    }

    public CommonConcerns<TConcern> addBefore(String before, TConcern concern) {

        if (stages.containsKey(concern.concernName()))
            throw new ETracInternal("Common concerns already contains stage: " + concern.concernName());

        if (!stageOrder.contains(before))
            throw new ETracInternal("Common concerns does not contain stage: " + before);

        var beforeIndex = stageOrder.indexOf(before);

        stageOrder.add(beforeIndex, concern.concernName());
        stages.put(concern.concernName(), concern);

        return this;
    }

    public CommonConcerns<TConcern> addAfter(String after, TConcern concern) {

        if (stages.containsKey(concern.concernName()))
            throw new ETracInternal("Common concerns already contains stage: " + concern.concernName());

        if (!stageOrder.contains(after))
            throw new ETracInternal("Common concerns does not contain stage: " + after);

        var afterIndex = stageOrder.indexOf(after) + 1;

        stageOrder.add(afterIndex, concern.concernName());
        stages.put(concern.concernName(), concern);

        return this;
    }

    public List<String> stageNames() {
        return Collections.unmodifiableList(stageOrder);
    }

    @Override
    public String concernName() {
        return concernName;
    }
}
