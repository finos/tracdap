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

import org.finos.tracdap.common.metadata.ResourceBundle;
import org.finos.tracdap.metadata.ExternalLocation;
import org.finos.tracdap.metadata.ResourceDefinition;

import java.util.List;
import java.util.Locale;
import java.util.Map;


final class ExternalLocationDetails {

    // Resource properties that describe where a storage location points, by storage protocol
    // Anything else in a resource's properties may be a credential and is never copied
    private static final Map<String, List<String>> LOCATION_PROPERTIES = Map.of(
            "local", List.of("rootPath"),
            "file", List.of("rootPath"),
            "s3", List.of("bucket", "prefix", "region", "endpoint"),
            "blob", List.of("storageAccount", "container", "prefix"),
            "gcs", List.of("project", "bucket", "prefix", "region", "endpoint"));

    private ExternalLocationDetails() {}

    static ExternalLocation recordLocationDetails(ExternalLocation location, ResourceBundle resources) {

        var resource = resources.getResource(location.getStorageKey());

        return recordLocationDetails(location, resource);
    }

    static ExternalLocation recordLocationDetails(ExternalLocation location, ResourceDefinition resource) {

        var protocol = resource.getProtocol();
        var locationProperties = LOCATION_PROPERTIES.getOrDefault(protocol.toLowerCase(Locale.ROOT), List.of());

        var recorded = location.toBuilder()
                .setProtocol(protocol)
                .clearLocationDetails();

        for (var property : locationProperties) {
            if (resource.containsProperties(property))
                recorded.putLocationDetails(property, resource.getPropertiesOrThrow(property));
        }

        return recorded.build();
    }
}
