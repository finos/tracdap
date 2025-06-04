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

package org.finos.tracdap.svc.data.service;

import org.finos.tracdap.common.config.ISecretLoader;
import org.finos.tracdap.common.service.TenantServicesMap;
import org.finos.tracdap.common.storage.StorageManager;
import org.finos.tracdap.config.TenantConfig;


public class TenantServices {

    public static class Map extends TenantServicesMap<TenantServices> {}

    public static Map create() {
        return new Map();
    }

    private final DataService dataService;
    private final FileService fileService;
    private final StorageManager storageManager;
    private final ISecretLoader secrets;

    private final TenantConfig staticConfig;

    public TenantServices(
            TenantConfig staticConfig,
            DataService dataService, FileService fileService,
            StorageManager storageManager, ISecretLoader secrets) {

        this.staticConfig = staticConfig;
        this.dataService = dataService;
        this.fileService = fileService;
        this.storageManager = storageManager;
        this.secrets = secrets;
    }

    public TenantConfig getStaticConfig() {
        return staticConfig;
    }

    public DataService getDataService() {
        return dataService;
    }

    public FileService getFileService() {
        return fileService;
    }

    public StorageManager getStorageManager() {
        return storageManager;
    }

    public ISecretLoader getSecrets() {
        return secrets;
    }
}
