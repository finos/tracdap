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

package org.finos.tracdap.gateway.proxy.rest;

import org.finos.tracdap.api.ListTenantsResponse;
import org.finos.tracdap.api.PlatformInfoResponse;
import org.finos.tracdap.gateway.proxy.http.Http1Client;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpScheme;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.InputStreamReader;
import java.util.Map;

import static io.netty.util.NetUtil.LOCALHOST;
import static org.finos.tracdap.test.meta.TestData.TEST_TENANT;


public class RestProxyTest {

    public static final short TEST_GW_PORT = 8080;
    private static final long TEST_TIMEOUT = 10 * 1000;  // 10 second timeout

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TRAC_GW_CONFIG_UNIT = "config/trac-gw-unit.yaml";

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT, TRAC_GW_CONFIG_UNIT)
            .addTenant(TEST_TENANT)
            .startMeta()
            .startGateway()
            .build();

    @Test
    void platformInfo() throws Exception {

        var method = "/trac-meta/api/v1/trac/platform-info";

        var headers = Map.<CharSequence, Object>ofEntries(
                Map.entry(HttpHeaderNames.CONTENT_TYPE, "application/json"),
                Map.entry(HttpHeaderNames.ACCEPT, "application/json"));

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var call = client.getRequest(method);
        call.await(TEST_TIMEOUT);

        Assertions.assertTrue(call.isDone());
        Assertions.assertTrue(call.isSuccess());

        var response = call.getNow();
        var responseMessage = parseJson(response.content(), PlatformInfoResponse.class);

        // This is set in trac-unit.yaml
        Assertions.assertEquals("TEST_ENVIRONMENT", responseMessage.getEnvironment());
        Assertions.assertFalse(responseMessage.getProduction());

        System.out.println("Platform info says TRAC version = " + responseMessage.getTracVersion());
    }

    @Test
    void listTenants() throws Exception {

        var method = "/trac-meta/api/v1/trac/list-tenants";

        var headers = Map.<CharSequence, Object>ofEntries(
                Map.entry(HttpHeaderNames.CONTENT_TYPE, "application/json"),
                Map.entry(HttpHeaderNames.ACCEPT, "application/json"));

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var call = client.getRequest(method);
        call.await(TEST_TIMEOUT);

        Assertions.assertTrue(call.isDone());
        Assertions.assertTrue(call.isSuccess());

        var response = call.getNow();
        var responseMessage = parseJson(response.content(), ListTenantsResponse.class);

        // There should be at least one tenant and the ACME_CORP tenant used in the test setup should exist

        var tenants = responseMessage.getTenantsList();
        var acmeTenant = tenants.stream().filter(t -> t.getTenantCode().equals("ACME_CORP")).findFirst();
        Assertions.assertTrue(tenants.size() > 0);
        Assertions.assertTrue(acmeTenant.isPresent());

        System.out.println("List tenants found the testing tenant: " + acmeTenant.get().getDescription());
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> T parseJson(ByteBuf buffer, Class<T> msgClass) throws Exception {

        var builder = (T.Builder) msgClass.getMethod("newBuilder").invoke(null);

        try (var jsonStream = new ByteBufInputStream(buffer);
             var jsonReader = new InputStreamReader(jsonStream)) {

            var jsonParser = JsonFormat.parser();
            jsonParser.merge(jsonReader, builder);

            return (T) builder.build();
        }
        finally {
            buffer.release();
        }
    }
}
