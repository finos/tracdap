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

package org.finos.tracdap.plugins.azure.storage;

import org.finos.tracdap.common.storage.IStorageManager;

import java.util.Properties;


public class AzureStorageEnvProps {

    public static final String TRAC_AZURE_STORAGE_ACCOUNT = "TRAC_AZURE_STORAGE_ACCOUNT";
    public static final String TRAC_AZURE_CONTAINER = "TRAC_AZURE_CONTAINER";
    public static final String TRAC_AZURE_CREDENTIALS = "TRAC_AZURE_CREDENTIALS";
    public static final String TRAC_AZURE_ACCESS_KEY = "TRAC_AZURE_ACCESS_KEY";

    public static Properties readStorageEnvProps() {

        var storageAccount = System.getenv(TRAC_AZURE_STORAGE_ACCOUNT);
        var container = System.getenv(TRAC_AZURE_CONTAINER);
        var credentials = System.getenv(TRAC_AZURE_CREDENTIALS);

        var storageProps = new Properties();
        storageProps.put(IStorageManager.PROP_STORAGE_KEY, "TEST_STORAGE");
        storageProps.put(AzureBlobStorage.STORAGE_ACCOUNT_PROPERTY, storageAccount);
        storageProps.put(AzureBlobStorage.CONTAINER_PROPERTY, container);

        if (credentials != null)
            storageProps.put(AzureBlobStorage.CREDENTIALS_PROPERTY, credentials);

        if (AzureBlobStorage.CREDENTIALS_ACCESS_KEY.equalsIgnoreCase(credentials)) {
            var accessKey = System.getenv(TRAC_AZURE_ACCESS_KEY);
            storageProps.put(AzureBlobStorage.ACCESS_KEY_PROPERTY, accessKey);
        }

        return storageProps;
    }
}
