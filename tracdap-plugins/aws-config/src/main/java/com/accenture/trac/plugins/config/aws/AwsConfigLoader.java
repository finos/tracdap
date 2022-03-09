/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.plugins.config.aws;

import com.accenture.trac.common.config.IConfigLoader;
import com.accenture.trac.common.exception.EStartup;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;


/**
 * A config loader implementation for loading from AWS S3.
 */
public class AwsConfigLoader implements IConfigLoader {

    @Override
    public String loaderName() {
        return "AWS S3";
    }

    @Override
    public List<String> protocols() {
        return List.of("s3");
    }

    @Override
    public String loadTextFile(URI uri) {

        var ERROR_MSG_TEMPLATE = "Failed to load config file from S3: %2$s [%1$s]";

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
        String path = uri.getPath();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        String output;
        try {
            S3Object o = s3.getObject(uri.getHost(), path);
            try (S3ObjectInputStream s3is = o.getObjectContent()) {
                try (ByteArrayOutputStream fos = new ByteArrayOutputStream()) {
                    byte[] read_buf = new byte[1024];
                    int read_len;
                    while ((read_len = s3is.read(read_buf)) > 0) {
                        fos.write(read_buf, 0, read_len);
                    }
                    output = fos.toString();
                }
            }
            return output;
        } catch (AmazonS3Exception | IOException e) {

            var message = String.format(ERROR_MSG_TEMPLATE, path, e.getMessage());
            throw new EStartup(message, e);
        }
    }
}
