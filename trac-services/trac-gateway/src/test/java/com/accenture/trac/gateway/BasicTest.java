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

import com.accenture.trac.api.MetadataSearchRequest;
import com.accenture.trac.api.TracMetadataApiGrpc;
import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.config.StandardArgs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;


public class BasicTest {

    @Test
    void basicTest() throws Exception {

        var args = new StandardArgs(Paths.get("."), "etc/trac-devlocal-gw.properties", "");
        var config = new ConfigManager(args);
        config.initConfigPlugins();
        config.initLogging();

        var gw = new TracPlatformGateway(config);

        var log = LoggerFactory.getLogger(this.getClass());
        log.info("Before run");

        gw.start();
        var testUrl = new URL("http", "localhost", 8080, "/");

        try(var stream = testUrl.openStream(); var reader = new BufferedReader(new InputStreamReader(stream))) {

            var data = reader
                    .lines()
                    .collect(Collectors.joining("\n"));
        }
        finally {

            gw.stop();
        }
    }

    @Test
    void basicTest2() throws Exception {

        var args = new StandardArgs(Paths.get("."), "etc/trac-devlocal-gw.properties", "");
        var config = new ConfigManager(args);
        config.initConfigPlugins();
        config.initLogging();

        var gw = new TracPlatformGateway(config);

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
