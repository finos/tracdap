/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.gateway;

import org.finos.tracdap.api.MetadataSearchRequest;
import org.finos.tracdap.api.TracMetadataApiGrpc;

import org.finos.tracdap.common.startup.Startup;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;



public class SmokeTests {

//    @Test
//    void http1SimpleProxy_get() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http1SimpleProxy_post() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http1SimpleProxy_head() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http1SimpleProxy_redirect() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http1SimpleProxy_404() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http1SimpleProxy_serverDown() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http1SimpleProxy_serverTimeout() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http2SimpleProxy_get() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http2SimpleProxy_post() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http2SimpleProxy_head() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http2SimpleProxy_redirect() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http2SimpleProxy_404() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http2SimpleProxy_serverDown() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void http2SimpleProxy_serverTimeout() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void grpcProxy_ok() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void grpcProxy_inBandError() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void grpcProxy_outOfBandError() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void grpcProxy_serverDown() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void grpcWebProxy_ok() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void grpcWebProxy_inBandError() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void grpcWebProxy_outOfBandError() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void grpcWebProxy_serverDown() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void restApi_ok() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void restApi_inBandError() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void restApi_404() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void restApi_translationFailed() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void restApi_serverDown() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void routingError_noMatch() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void routingError_wrongContentType() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void routingError_wrongHttpVersion() throws Exception {
//        Assertions.fail();
//    }
//
//    @Test
//    void configHandling() throws Exception {
//        Assertions.fail();
//    }




    @Test @Disabled
    void grpcSmokeTest() throws Exception {

        var startup = Startup.useConfigFile(TracPlatformGateway.class, "etc/trac-devlocal-gw.properties");
        startup.runStartupSequence();

        var plugins = startup.getPlugins();
        var config = startup.getConfig();

        var gw = new TracPlatformGateway(plugins, config);

        var log = LoggerFactory.getLogger(this.getClass());
        log.info("Before run");

        gw.start();

        var cb = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext();
        var c = cb.build();
        var cliStub = TracMetadataApiGrpc.newBlockingStub(c);

        try {

            var searchResult = cliStub.search(MetadataSearchRequest.newBuilder().build());

        }
        finally {

            gw.stop();
        }
    }
}
