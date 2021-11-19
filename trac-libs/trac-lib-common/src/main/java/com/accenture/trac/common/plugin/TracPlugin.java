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

package com.accenture.trac.common.plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public abstract class TracPlugin implements ITracPlugin {

    protected abstract <T> T createService(String serviceName, Properties properties);

    @Override
    public final List<String> protocols(Class<?> service) {

        var psi = serviceInfo();

        var matchingPsi = psi.stream()
                .filter(si -> si.serviceClass() == service)
                .collect(Collectors.toList());

        if (matchingPsi.isEmpty())
            throw new IllegalArgumentException();

        return matchingPsi.stream()
                .map(PluginServiceInfo::protocols)
                .reduce(new ArrayList<>(), (ps, p) -> {ps.addAll(p); return ps;});
    }

    @Override
    public final <T> T createService(Class<T> serviceClass, String protocol, Properties properties) {

        var psi = serviceInfo();

        var matchingPsi = psi.stream()
                .filter(psi_ -> psiMatch(psi_, serviceClass, protocol))
                .findFirst();

        if (matchingPsi.isEmpty())
            throw new IllegalArgumentException();

        return createService(matchingPsi.get().serviceName(), properties);
    }

    protected static boolean psiMatch(PluginServiceInfo psi, Class<?> service, String protocol) {

        if (psi.serviceClass() != service)
            return false;

        for (var psiProtocol : psi.protocols()) {

            if (psiProtocol.equalsIgnoreCase(protocol))
                return true;
        }

        return false;
    }
}
