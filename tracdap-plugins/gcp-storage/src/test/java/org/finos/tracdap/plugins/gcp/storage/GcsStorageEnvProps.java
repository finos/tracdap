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

package org.finos.tracdap.plugins.gcp.storage;

import org.finos.tracdap.common.storage.IStorageManager;

import java.util.Properties;


public class GcsStorageEnvProps {

    public static final String TRAC_GCP_REGION = "TRAC_GCP_REGION";
    public static final String TRAC_GCP_PROJECT = "TRAC_GCP_PROJECT";
    public static final String TRAC_GCP_BUCKET = "TRAC_GCP_BUCKET";

    public static Properties readStorageEnvProps() {

        var region = System.getenv(TRAC_GCP_REGION);
        var project = System.getenv(TRAC_GCP_PROJECT);
        var bucket = System.getenv(TRAC_GCP_BUCKET);

        var storageProps = new Properties();
        storageProps.put(IStorageManager.PROP_STORAGE_KEY, "TEST_STORAGE");
        storageProps.put(GcsObjectStorage.REGION_PROPERTY, region);
        storageProps.put(GcsObjectStorage.PROJECT_PROPERTY, project);
        storageProps.put(GcsObjectStorage.BUCKET_PROPERTY, bucket);

        return storageProps;
    }
}
