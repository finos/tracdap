/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.auth.external;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class Http1AuthHeaders implements IAuthHeaders {

    private final HttpHeaders headers;

    public Http1AuthHeaders() {
        this.headers = new DefaultHttpHeaders();
    }

    public Http1AuthHeaders(HttpHeaders headers) {
        this.headers = headers;
    }

    public HttpHeaders headers() {
        return this.headers;
    }

    @Override
    public void add(CharSequence name, CharSequence value) {
        headers.add(name, value);
    }

    @Override
    public boolean contains(CharSequence name) {
        return headers.contains(name);
    }

    @Override
    public String get(CharSequence name) {
        return headers.get(name);
    }

    @Override
    public List<? extends CharSequence> getAll(CharSequence name) {
        return headers.getAll(name);
    }

    @Override
    public Iterator<Map.Entry<CharSequence, CharSequence>> iterator() {
        return headers.iteratorCharSequence();
    }
}
