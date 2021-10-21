/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.api.config;

import java.util.List;

public class InstancesConfig {

    private List<InstanceConfig> meta = List.of();
    private List<InstanceConfig> data = List.of();

    public List<InstanceConfig> getMeta() {
        return meta;
    }

    public void setMeta(List<InstanceConfig> meta) {
        this.meta = meta;
    }

    public List<InstanceConfig> getData() {
        return data;
    }

    public void setData(List<InstanceConfig> data) {
        this.data = data;
    }
}
