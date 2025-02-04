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

package org.finos.tracdap.gateway.proxy.grpc;

import org.finos.tracdap.api.ListTenantsRequest;
import org.finos.tracdap.api.ListTenantsResponse;
import org.finos.tracdap.api.PlatformInfoRequest;
import org.finos.tracdap.api.PlatformInfoResponse;
import org.finos.tracdap.gateway.TracPlatformGateway;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.Message;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;

import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;


public class GrpcWebProxyTest {

    public static final short TEST_GW_PORT = 9100;
    private static final long TEST_TIMEOUT = 10 * 1000;  // 10 second timeout

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
            .runDbDeploy(true)
            .addTenant(TEST_TENANT)
            .startService(TracMetadataService.class)
            .startService(TracPlatformGateway.class)
            .build();

    private static HttpClient client;

    @BeforeAll
    static void setupClient() {
        client = HttpClient.newHttpClient();
    }

    @Test
    void platformInfo() throws Exception {

        var method = "/tracdap.api.TracMetadataApi/platformInfo";

        var requestMessage = PlatformInfoRequest.newBuilder().build();
        var requestBytes = wrapLpm(requestMessage);

        var readRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestBytes.array()))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + method))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/grpc-web+proto")
                .header("accept", "application/grpc-web+proto")
                .header("x-user-agent", "trac-gateway-test")
                .header("x-grpc-web", "1")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var readResponse = client.send(readRequest, HttpResponse.BodyHandlers.ofByteArray());
        Assertions.assertEquals(200, readResponse.statusCode());

        var responseBytes = ByteBuffer.wrap(readResponse.body());
        var responseMessage = unwrapLpm(responseBytes, PlatformInfoResponse.class);

        // This is set in trac-unit.yaml
        Assertions.assertEquals("TEST_ENVIRONMENT", responseMessage.getEnvironment());
        Assertions.assertFalse(responseMessage.getProduction());

        System.out.println("Platform info says TRAC version = " + responseMessage.getTracVersion());
    }

    @Test
    void listTenants() throws Exception {

        var method = "/tracdap.api.TracMetadataApi/listTenants";

        var requestMessage = ListTenantsRequest.newBuilder().build();
        var requestBytes = wrapLpm(requestMessage);

        var readRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestBytes.array()))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + method))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/grpc-web+proto")
                .header("accept", "application/grpc-web+proto")
                .header("x-user-agent", "trac-gateway-test")
                .header("x-grpc-web", "1")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var readResponse = client.send(readRequest, HttpResponse.BodyHandlers.ofByteArray());
        Assertions.assertEquals(200, readResponse.statusCode());

        var responseBytes = ByteBuffer.wrap(readResponse.body());
        var responseMessage = unwrapLpm(responseBytes, ListTenantsResponse.class);

        // There should be at least one tenant and the ACME_CORP tenant used in the test setup should exist

        var tenants = responseMessage.getTenantsList();
        var acmeTenant = tenants.stream().filter(t -> t.getTenantCode().equals("ACME_CORP")).findFirst();
        Assertions.assertFalse(tenants.isEmpty());
        Assertions.assertTrue(acmeTenant.isPresent());

        System.out.println("List tenants found the testing tenant: " + acmeTenant.get().getDescription());
    }

    private <T extends Message> ByteBuffer wrapLpm(T msg) {

        var msgBytes = msg.toByteArray();
        var lpm = ByteBuffer.allocate(5 + msgBytes.length);
        lpm.order(ByteOrder.LITTLE_ENDIAN);
        lpm.put((byte) 0x00);
        lpm.putInt(msgBytes.length);
        lpm.put(msgBytes);

        return lpm;
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> T unwrapLpm(ByteBuffer buf, Class<T> msgClass) throws Exception {

        try {

            var builder = (T.Builder) msgClass.getMethod("newBuilder").invoke(null);

            var flag = buf.get();
            var length = buf.getInt();

            if ((flag & (byte) 0x01) != (byte) 0x00)
                throw new RuntimeException("LPM compression not supported in gateway tests");

            var msgBytes = new byte[length];
            buf.get(msgBytes);

            return (T) builder.mergeFrom(msgBytes).build();
        }
        finally {
            buf.clear();
        }
    }
}
