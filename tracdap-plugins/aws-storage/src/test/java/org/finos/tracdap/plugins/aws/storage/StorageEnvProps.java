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

package org.finos.tracdap.plugins.aws.storage;

import org.finos.tracdap.common.storage.IStorageManager;

import java.util.Properties;

public class StorageEnvProps {

    public static final String AWS_REGION = "AWS_REGION";
    public static final String AWS_S3_BUCKET = "AWS_S3_BUCKET";
    public static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
    public static final String AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";

    public static Properties readStorageEnvProps() {

        var region = System.getenv(AWS_REGION);
        var bucket = System.getenv(AWS_S3_BUCKET);
        var accessKeyId = System.getenv(AWS_ACCESS_KEY_ID);
        var secretAccessKey = System.getenv(AWS_SECRET_ACCESS_KEY);

        var storageProps = new Properties();
        storageProps.put(IStorageManager.PROP_STORAGE_KEY, "TEST_STORAGE");
        storageProps.put(S3ObjectStorage.REGION_PROPERTY, region);
        storageProps.put(S3ObjectStorage.BUCKET_PROPERTY, bucket);
        storageProps.put(S3ObjectStorage.ACCESS_KEY_ID_PROPERTY, accessKeyId);
        storageProps.put(S3ObjectStorage.SECRET_ACCESS_KEY_PROPERTY, secretAccessKey);

        return storageProps;
    }
}
