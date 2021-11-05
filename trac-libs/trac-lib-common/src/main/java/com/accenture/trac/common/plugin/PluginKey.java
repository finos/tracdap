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

import javax.annotation.Nonnull;
import java.util.Objects;


class PluginKey {

    private final Class<?> service;
    private final String protocol;

    public PluginKey(@Nonnull Class<?> service, @Nonnull String protocol) {
        this.service = service;
        this.protocol = protocol.toLowerCase();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PluginKey pluginKey = (PluginKey) o;
        return service.equals(pluginKey.service) && protocol.equals(pluginKey.protocol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(service, protocol);
    }
}
