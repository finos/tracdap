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

package org.finos.tracdap.gateway.exec;

import org.finos.tracdap.config.WebServerRedirect;

public class Redirect {

    private final int index;
    private final WebServerRedirect config;
    private final IRouteMatcher matcher;

    public Redirect(int index, WebServerRedirect config, IRouteMatcher matcher) {
        this.index = index;
        this.config = config;
        this.matcher = matcher;
    }

    public int getIndex() {
        return index;
    }

    public WebServerRedirect getConfig() {
        return config;
    }

    public IRouteMatcher getMatcher() {
        return matcher;
    }
}
