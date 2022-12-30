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
import java.util.Spliterator;
import java.util.function.Consumer;


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
    public void add(CharSequence header, CharSequence value) {
        headers.add(header, value);
    }

    @Override
    public boolean contains(CharSequence header) {
        return headers.contains(header);
    }

    @Override
    public String get(CharSequence header) {
        return headers.get(header);
    }

    @Override
    public List<String> getAll(CharSequence header) {
        return headers.getAll(header);
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return headers.iteratorAsString();
    }

    @Override
    public Spliterator<Map.Entry<String, String>> spliterator() {
        return headers.spliterator();
    }

    @Override
    public void forEach(Consumer<? super Map.Entry<String, String>> action) {
        headers.forEach(action);
    }
}
