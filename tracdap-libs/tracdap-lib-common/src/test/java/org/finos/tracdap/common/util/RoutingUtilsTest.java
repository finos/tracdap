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

package org.finos.tracdap.common.util;

import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.exception.EConfig;
import org.finos.tracdap.config.ServiceConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


class RoutingUtilsTest {

    @Test
    void serviceBindAddress_unsetDefaultsToWildcard() {

        var config = ServiceConfig.newBuilder()
                .setPort(8081)
                .build();

        assertNull(RoutingUtils.serviceBindHost(config));

        var address = RoutingUtils.serviceBindAddress(config);
        assertEquals(8081, address.getPort());
        assertTrue(address.getAddress().isAnyLocalAddress());
    }

    @Test
    void serviceBindAddress_blankDefaultsToWildcard() {

        var config = ServiceConfig.newBuilder()
                .setPort(8082)
                .putProperties(ConfigKeys.NETWORK_BIND_ADDRESS, "   ")
                .build();

        assertNull(RoutingUtils.serviceBindHost(config));

        var address = RoutingUtils.serviceBindAddress(config);
        assertEquals(8082, address.getPort());
        assertTrue(address.getAddress().isAnyLocalAddress());
    }

    @Test
    void serviceBindAddress_loopbackBindsSpecificInterface() {

        var config = ServiceConfig.newBuilder()
                .setPort(8083)
                .putProperties(ConfigKeys.NETWORK_BIND_ADDRESS, " 127.0.0.1 ")
                .build();

        assertEquals("127.0.0.1", RoutingUtils.serviceBindHost(config));

        var address = RoutingUtils.serviceBindAddress(config);
        assertEquals(8083, address.getPort());
        assertFalse(address.getAddress().isAnyLocalAddress());
        assertTrue(address.getAddress().isLoopbackAddress());
        assertEquals("127.0.0.1", address.getAddress().getHostAddress());
    }

    @Test
    void serviceBindAddress_invalidHostThrowsEConfig() {

        var config = ServiceConfig.newBuilder()
                .setPort(8084)
                .putProperties(ConfigKeys.NETWORK_BIND_ADDRESS, "nonexistent.invalid.")
                .build();

        assertThrows(EConfig.class, () -> RoutingUtils.serviceBindAddress(config));
    }
}
