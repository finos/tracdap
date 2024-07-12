/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.gateway.builders;

import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.WebServerRedirect;
import org.finos.tracdap.gateway.exec.IRouteMatcher;
import org.finos.tracdap.gateway.exec.Redirect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;


public class RedirectBuilder {

    private static final Logger log = LoggerFactory.getLogger(RedirectBuilder.class);

    private int nextRouteIndex;

    public RedirectBuilder() {
        nextRouteIndex = 0;
    }

    public List<Redirect> buildRedirects(PlatformConfig platformConfig) {

        var gatewayConfig = platformConfig.getGateway();

        if (gatewayConfig.getRedirectsCount() == 0) {
            log.info("Gateway redirects are not enabled");
            return List.of();
        }

        log.info("Building gateway redirects...");

        var redirects = new ArrayList<Redirect>(gatewayConfig.getRedirectsCount());

        for (var redirectConfig : gatewayConfig.getRedirectsList()) {
            var redirect = buildRedirect(redirectConfig);
            redirects.add(redirect);
        }

        for (var redirect : redirects) {

            log.info("[{}] {} -> {} ({})",
                    redirect.getIndex(),
                    redirect.getConfig().getSource(),
                    redirect.getConfig().getTarget(),
                    redirect.getConfig().getStatus());
        }

        return redirects;
    }

    private Redirect buildRedirect(WebServerRedirect redirectConfig) {

        var index = nextRouteIndex++;

        var pattern = Pattern.compile(redirectConfig.getSource());
        var matcher = (IRouteMatcher) (method, uri) -> pattern.matcher(uri.getPath()).matches();

        return new Redirect(index, redirectConfig, matcher);
    }
}
