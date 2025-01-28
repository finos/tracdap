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

import io.netty.channel.ChannelPipeline;

import java.util.Collections;
import java.util.List;
import java.util.Map;


public class CommonNettyConcerns extends CommonConcerns<NettyConcern> implements NettyConcern {

    public CommonNettyConcerns(String concernName) {
        super(concernName);
    }

    private CommonNettyConcerns(String concernName, List<String> stageOrder, Map<String, NettyConcern> stages) {
        super(concernName, stageOrder, stages);
    }

    @Override
    public NettyConcern build() {

        var stageOrder = Collections.unmodifiableList(this.stageOrder);
        var stages = Collections.unmodifiableMap(this.stages);

        return new CommonNettyConcerns(concernName(), stageOrder, stages);
    }

    @Override
    public void configureInboundChannel(ChannelPipeline pipeline, SupportedProtocol protocol) {

        for (String stageName : stageOrder) {
            var stage = stages.get(stageName);
            stage.configureInboundChannel(pipeline, protocol);
        }
    }

    @Override
    public void configureOutboundChannel(ChannelPipeline pipeline, SupportedProtocol protocol) {

        for (var stageName : stageOrder) {
            var stage = stages.get(stageName);
            stage.configureOutboundChannel(pipeline, protocol);
        }
    }
}
