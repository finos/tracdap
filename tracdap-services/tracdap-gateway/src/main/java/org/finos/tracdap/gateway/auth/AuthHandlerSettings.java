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

package org.finos.tracdap.gateway.auth;

import org.finos.tracdap.common.config.ConfigDefaults;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.util.RoutingUtils;
import org.finos.tracdap.config.AuthenticationConfig;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.RoutingTarget;

import java.time.Duration;


public class AuthHandlerSettings {

    public static final String RETURN_PATH_VARIABLE = "${returnPath}";
    public static final int REFRESH_TIMEOUT_MILLIS = 500;

    private final RoutingTarget authTarget;
    private final AuthenticationConfig authConfig;

    private final String publicLoginPrefix;
    private final String publicLoginUrl;
    private final String publicReturnPath;
    private final String refreshPath;
    private final Duration refreshInterval;
    private final Duration refreshTimeout;

    public AuthHandlerSettings(PlatformConfig platformConfig) {

        this.authTarget = RoutingUtils.serviceTarget(platformConfig, ConfigKeys.AUTHENTICATION_SERVICE_KEY);
        this.authConfig = platformConfig.getAuthentication();

        // TODO: Get these from config and tie into routing setup
        var authPrefix = "/trac-auth/";
        var loginPrefix = "/login/";

        var loginPath = ConfigDefaults.readOrDefault(authConfig.getLoginPath(), ConfigDefaults.DEFAULT_LOGIN_PATH);
        var refreshPath = ConfigDefaults.readOrDefault(authConfig.getRefreshPath(), ConfigDefaults.DEFAULT_REFRESH_PATH);
        var returnPath = ConfigDefaults.readOrDefault(authConfig.getReturnPath(), ConfigDefaults.DEFAULT_RETURN_PATH);
        var jwtRefresh = ConfigDefaults.readOrDefault(authConfig.getJwtRefresh(), ConfigDefaults.DEFAULT_JWT_REFRESH);

        this.publicLoginPrefix = joinPathSections(authPrefix, loginPrefix);
        this.publicLoginUrl = joinPathSections(authPrefix, loginPath);
        this.publicReturnPath = returnPath;
        this.refreshPath = refreshPath;
        this.refreshInterval = Duration.ofSeconds(jwtRefresh);
        this.refreshTimeout = Duration.ofMillis(REFRESH_TIMEOUT_MILLIS);
    }

    public RoutingTarget authTarget() {
        return authTarget;
    }

    public AuthenticationConfig authConfig() {
        return authConfig;
    }

    public String publicLoginPrefix() {
        return publicLoginPrefix;
    }

    public String publicLoginUrl() {
        return publicLoginUrl;
    }

    public String publicReturnPath() {
        return publicReturnPath;
    }

    public String refreshPath() {
        return refreshPath;
    }

    public Duration refreshInterval() {
        return refreshInterval;
    }

    public Duration refreshTimeout() {
        return refreshTimeout;
    }

    private String joinPathSections(String first, String second) {

        if (first.endsWith("/")) {
            if (second.startsWith("/"))
                return first + second.substring(1);
            else
                return first + second;
        }
        else {
            if (second.startsWith("/"))
                return first + second;
            else
                return first + "/" + second;
        }
    }
}
