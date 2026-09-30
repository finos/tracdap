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

package org.finos.tracdap.gateway.proxy.rest;

import org.finos.tracdap.api.AdminServiceProto;
import org.finos.tracdap.api.MetadataServiceProto;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.URI;
import java.util.List;


public class RestApiMethodOrderTest {

    private static final List<RestApiMethod> ADMIN_METHODS = RestApiMethodBuilder.buildService(
            AdminServiceProto.getDescriptor().findServiceByName("TracAdminApi"),
            "/trac-admin/api/v1", RestApiMethodOrderTest.class.getClassLoader());

    private static final List<RestApiMethod> METADATA_METHODS = RestApiMethodBuilder.buildService(
            MetadataServiceProto.getDescriptor().findServiceByName("TracMetadataApi"),
            "/trac-meta/api/v1", RestApiMethodOrderTest.class.getClassLoader());

    @ParameterizedTest
    @CsvSource({
            "platform/create-config-object, createPlatformConfigObject",
            "platform/update-config-object, updatePlatformConfigObject",
            "platform/delete-config-object, deletePlatformConfigObject",
            "platform/read-config-object, readPlatformConfigObject",
            "platform/read-config-batch, readPlatformConfigBatch",
            "platform/list-config-entries, listPlatformConfigEntries",
            "ACME_CORP/create-config-object, createConfigObject",
            "ACME_CORP/read-config-batch, readConfigBatch"})
    void literalSegmentWinsOverVariable(String path, String expectedMethod) {

        var request = new RestApiRequest("POST", URI.create("/trac-admin/api/v1/" + path));

        assertFirstMatch(ADMIN_METHODS, request, expectedMethod);
    }

    @ParameterizedTest
    @CsvSource({
            "versions/1/tags/2, getObject",
            "versions/1/tags/latest, getLatestTag",
            "versions/latest/tags/latest, getLatestObject"})
    void versionVariablesOnlyMatchNumbers(String path, String expectedMethod) {

        var request = new RestApiRequest("GET", URI.create("/trac-meta/api/v1/ACME_CORP/FLOW/" +
                "00000000-0000-0000-0000-000000000000/" + path));

        assertFirstMatch(METADATA_METHODS, request, expectedMethod);
    }

    private void assertFirstMatch(List<RestApiMethod> methods, RestApiRequest request, String expectedMethod) {

        var method = methods.stream()
                .filter(m -> m.requestMatcher.matches(request))
                .findFirst();

        Assertions.assertTrue(method.isPresent());
        Assertions.assertEquals(expectedMethod, method.get().methodDescriptor.getName());
    }
}
