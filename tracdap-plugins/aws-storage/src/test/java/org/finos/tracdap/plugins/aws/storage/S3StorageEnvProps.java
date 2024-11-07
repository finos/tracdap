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

package org.finos.tracdap.plugins.aws.storage;

import org.finos.tracdap.common.storage.IStorageManager;

import java.util.Properties;

public class S3StorageEnvProps {

    public static final String TRAC_AWS_REGION = "TRAC_AWS_REGION";
    public static final String TRAC_AWS_BUCKET = "TRAC_AWS_BUCKET";
    public static final String TRAC_AWS_CREDENTIALS = "TRAC_AWS_CREDENTIALS";
    public static final String TRAC_AWS_ACCESS_KEY_ID = "TRAC_AWS_ACCESS_KEY_ID";
    public static final String TRAC_AWS_SECRET_ACCESS_KEY = "TRAC_AWS_SECRET_ACCESS_KEY";

    public static Properties readStorageEnvProps() {

        var region = System.getenv(TRAC_AWS_REGION);
        var bucket = System.getenv(TRAC_AWS_BUCKET);
        var credentials = System.getenv(TRAC_AWS_CREDENTIALS);

        var storageProps = new Properties();
        storageProps.put(IStorageManager.PROP_STORAGE_KEY, "TEST_STORAGE");
        storageProps.put(S3ObjectStorage.REGION_PROPERTY, region);
        storageProps.put(S3ObjectStorage.BUCKET_PROPERTY, bucket);

        if (credentials != null)
            storageProps.put(S3ObjectStorage.CREDENTIALS_PROPERTY, credentials);

        if (S3ObjectStorage.CREDENTIALS_STATIC.equals(credentials)) {

            var accessKeyId = System.getenv(TRAC_AWS_ACCESS_KEY_ID);
            var secretAccessKey = System.getenv(TRAC_AWS_SECRET_ACCESS_KEY);

            storageProps.put(S3ObjectStorage.ACCESS_KEY_ID_PROPERTY, accessKeyId);
            storageProps.put(S3ObjectStorage.SECRET_ACCESS_KEY_PROPERTY, secretAccessKey);
        }

        return storageProps;
    }
}
