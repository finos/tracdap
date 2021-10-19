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

import java.util.Map;

public class DataServiceConfig {

    private short port;
    private String defaultStorage;

    private Map<String, StorageConfig> storage = Map.of();

    public short getPort() {
        return port;
    }

    public void setPort(short port) {
        this.port = port;
    }

    public String getDefaultStorage() {
        return defaultStorage;
    }

    public void setDefaultStorage(String defaultStorage) {
        this.defaultStorage = defaultStorage;
    }

    public Map<String, StorageConfig> getStorage() {
        return storage;
    }

    public void setStorage(Map<String, StorageConfig> storage) {
        this.storage = storage;
    }
}
