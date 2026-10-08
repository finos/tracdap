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

package org.finos.tracdap.svc.orch.jobs;

import org.finos.tracdap.metadata.ExternalLocation;
import org.finos.tracdap.metadata.ResourceDefinition;
import org.finos.tracdap.metadata.ResourceType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;


public class ExternalLocationDetailsTest {

    private static final ExternalLocation LOCATION = ExternalLocation.newBuilder()
            .setStorageKey("staging_data")
            .setStoragePath("2026-09/loans.csv")
            .build();

    private static ResourceDefinition storage(String protocol, Map<String, String> properties) {

        return ResourceDefinition.newBuilder()
                .setResourceType(ResourceType.EXTERNAL_STORAGE)
                .setProtocol(protocol)
                .putAllProperties(properties)
                .build();
    }

    private static void assertRecorded(String protocol, Map<String, String> expectedDetails, ExternalLocation recorded) {

        Assertions.assertEquals(LOCATION.getStorageKey(), recorded.getStorageKey());
        Assertions.assertEquals(LOCATION.getStoragePath(), recorded.getStoragePath());
        Assertions.assertEquals(protocol, recorded.getProtocol());
        Assertions.assertEquals(expectedDetails, recorded.getLocationDetailsMap());
    }

    @Test
    void localStorage() {

        var resource = storage("LOCAL", Map.of("rootPath", "/mnt/trac/external", "runtimeFs", "native"));
        var recorded = ExternalLocationDetails.recordLocationDetails(LOCATION, resource);

        assertRecorded("LOCAL", Map.of("rootPath", "/mnt/trac/external"), recorded);
    }

    @Test
    void s3Storage() {

        var resource = storage("S3", Map.of(
                "bucket", "trac-staging", "prefix", "inbound/", "region", "eu-west-2", "endpoint", "https://s3.example.com",
                "credentials", "static", "accessKeyId", "AKIA_TEST", "secretAccessKey", "SECRET_TEST"));

        var recorded = ExternalLocationDetails.recordLocationDetails(LOCATION, resource);

        assertRecorded("S3", Map.of(
                "bucket", "trac-staging", "prefix", "inbound/", "region", "eu-west-2", "endpoint", "https://s3.example.com"),
                recorded);
    }

    @Test
    void azureBlobStorage() {

        var resource = storage("BLOB", Map.of(
                "storageAccount", "tracstaging", "container", "inbound", "prefix", "loans/",
                "credentials", "access_key", "accountKey", "KEY_TEST", "sasToken", "SAS_TEST"));

        var recorded = ExternalLocationDetails.recordLocationDetails(LOCATION, resource);

        assertRecorded("BLOB", Map.of("storageAccount", "tracstaging", "container", "inbound", "prefix", "loans/"), recorded);
    }

    @Test
    void gcsStorage() {

        var resource = storage("GCS", Map.of(
                "project", "trac-project", "bucket", "trac-staging", "prefix", "inbound/",
                "region", "europe-west2", "credentials", "SERVICE_ACCOUNT_JSON_TEST"));

        var recorded = ExternalLocationDetails.recordLocationDetails(LOCATION, resource);

        assertRecorded("GCS", Map.of(
                "project", "trac-project", "bucket", "trac-staging", "prefix", "inbound/", "region", "europe-west2"),
                recorded);
    }

    @Test
    void protocolMatchIgnoresCase() {

        var resource = storage("s3", Map.of("bucket", "trac-staging", "secretAccessKey", "SECRET_TEST"));
        var recorded = ExternalLocationDetails.recordLocationDetails(LOCATION, resource);

        assertRecorded("s3", Map.of("bucket", "trac-staging"), recorded);
    }

    @Test
    void unknownProtocol() {

        var resource = storage("SFTP", Map.of("host", "sftp.example.com", "password", "SECRET_TEST"));
        var recorded = ExternalLocationDetails.recordLocationDetails(LOCATION, resource);

        assertRecorded("SFTP", Map.of(), recorded);
    }

    @Test
    void existingDetailsReplaced() {

        var location = LOCATION.toBuilder()
                .setProtocol("S3")
                .putLocationDetails("bucket", "old-bucket")
                .build();

        var resource = storage("LOCAL", Map.of("rootPath", "/mnt/trac/external"));
        var recorded = ExternalLocationDetails.recordLocationDetails(location, resource);

        assertRecorded("LOCAL", Map.of("rootPath", "/mnt/trac/external"), recorded);
    }
}
