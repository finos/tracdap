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

package org.finos.tracdap.common.http;

import io.netty.handler.codec.CharSequenceValueConverter;
import io.netty.handler.codec.Headers;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

import javax.annotation.Nonnull;
import java.util.*;


public class Http1Headers implements Headers<CharSequence, CharSequence, Http1Headers> {

    private static final CharSequenceValueConverter converter = new CharSequenceValueConverter();

    private final HttpHeaders headers;

    public Http1Headers() {
        this.headers = new DefaultHttpHeaders();
    }

    public Http1Headers(HttpHeaders headers) {
        this.headers = headers;
    }

    public static Http1Headers fromGenericHeaders(Headers<CharSequence, CharSequence, ?> genericHeaders) {

        if (genericHeaders instanceof Http1Headers)
            return (Http1Headers) genericHeaders;
        else
            return new Http1Headers().setAll(genericHeaders);
    }

    public static Http1Headers fromHttpHeaders(HttpHeaders genericHeaders) {

        return new Http1Headers(genericHeaders);
    }

    public HttpHeaders toHttpHeaders() {
        return headers;
    }

    @Override
    public CharSequence get(CharSequence name) {
        return headers.get(name);
    }

    @Override
    public CharSequence get(CharSequence name, CharSequence defaultValue) {
        return headers.get(name, defaultValue.toString());
    }

    @Override
    public CharSequence getAndRemove(CharSequence name) {
        var value = headers.get(name);
        headers.remove(name);
        return value;
    }

    @Override
    public CharSequence getAndRemove(CharSequence name, CharSequence defaultValue) {
        var value = headers.get(name, defaultValue.toString());
        headers.remove(name);
        return value;
    }

    @Override
    public List<CharSequence> getAll(CharSequence name) {
        return new ArrayList<>(headers.getAll(name));
    }

    @Override
    public List<CharSequence> getAllAndRemove(CharSequence name) {
        var value = new ArrayList<CharSequence>(headers.getAll(name));
        headers.remove(name);
        return value;
    }

    @Override
    public Boolean getBoolean(CharSequence name) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToBoolean(rawValue) : null;
    }

    @Override
    public boolean getBoolean(CharSequence name, boolean defaultValue) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToBoolean(rawValue) : defaultValue;
    }

    @Override
    public Byte getByte(CharSequence name) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToByte(rawValue) : null;
    }

    @Override
    public byte getByte(CharSequence name, byte defaultValue) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToByte(rawValue) : defaultValue;
    }

    @Override
    public Character getChar(CharSequence name) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToChar(rawValue) : null;
    }

    @Override
    public char getChar(CharSequence name, char defaultValue) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToChar(rawValue) : defaultValue;
    }

    @Override
    public Short getShort(CharSequence name) {
        return headers.getShort(name);
    }

    @Override
    public short getShort(CharSequence name, short defaultValue) {
        return headers.getShort(name, defaultValue);
    }

    @Override
    public Integer getInt(CharSequence name) {
        return headers.getInt(name);
    }

    @Override
    public int getInt(CharSequence name, int defaultValue) {
        return headers.getInt(name, defaultValue);
    }

    @Override
    public Long getLong(CharSequence name) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToLong(rawValue) : null;
    }

    @Override
    public long getLong(CharSequence name, long defaultValue) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToLong(rawValue) : defaultValue;
    }

    @Override
    public Float getFloat(CharSequence name) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToFloat(rawValue) : null;
    }

    @Override
    public float getFloat(CharSequence name, float defaultValue) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToFloat(rawValue) : defaultValue;
    }

    @Override
    public Double getDouble(CharSequence name) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToDouble(rawValue) : null;
    }

    @Override
    public double getDouble(CharSequence name, double defaultValue) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToDouble(rawValue) : defaultValue;
    }

    @Override
    public Long getTimeMillis(CharSequence name) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToTimeMillis(rawValue) : null;
    }

    @Override
    public long getTimeMillis(CharSequence name, long defaultValue) {
        var rawValue = headers.get(name);
        return rawValue != null ? converter.convertToTimeMillis(rawValue) : defaultValue;
    }

    @Override
    public Boolean getBooleanAndRemove(CharSequence name) {
        var value = getBoolean(name);
        headers.remove(name);
        return value;
    }

    @Override
    public boolean getBooleanAndRemove(CharSequence name, boolean defaultValue) {
        var value = getBoolean(name, defaultValue);
        headers.remove(name);
        return value;
    }

    @Override
    public Byte getByteAndRemove(CharSequence name) {
        var value = getByte(name);
        headers.remove(name);
        return value;
    }

    @Override
    public byte getByteAndRemove(CharSequence name, byte defaultValue) {
        var value = getByte(name, defaultValue);
        headers.remove(name);
        return value;
    }

    @Override
    public Character getCharAndRemove(CharSequence name) {
        var value = getChar(name);
        headers.remove(name);
        return value;
    }

    @Override
    public char getCharAndRemove(CharSequence name, char defaultValue) {
        var value = getChar(name, defaultValue);
        headers.remove(name);
        return value;
    }

    @Override
    public Short getShortAndRemove(CharSequence name) {
        var value = getShort(name);
        headers.remove(name);
        return value;
    }

    @Override
    public short getShortAndRemove(CharSequence name, short defaultValue) {
        var value = getShort(name, defaultValue);
        headers.remove(name);
        return value;
    }

    @Override
    public Integer getIntAndRemove(CharSequence name) {
        var value = getInt(name);
        headers.remove(name);
        return value;
    }

    @Override
    public int getIntAndRemove(CharSequence name, int defaultValue) {
        var value = getInt(name, defaultValue);
        headers.remove(name);
        return value;
    }

    @Override
    public Long getLongAndRemove(CharSequence name) {
        var value = getLong(name);
        headers.remove(name);
        return value;
    }

    @Override
    public long getLongAndRemove(CharSequence name, long defaultValue) {
        var value = getLong(name, defaultValue);
        headers.remove(name);
        return value;
    }

    @Override
    public Float getFloatAndRemove(CharSequence name) {
        var value = getFloat(name);
        headers.remove(name);
        return value;
    }

    @Override
    public float getFloatAndRemove(CharSequence name, float defaultValue) {
        var value = getFloat(name, defaultValue);
        headers.remove(name);
        return value;
    }

    @Override
    public Double getDoubleAndRemove(CharSequence name) {
        var value = getDouble(name);
        headers.remove(name);
        return value;
    }

    @Override
    public double getDoubleAndRemove(CharSequence name, double defaultValue) {
        var value = getDouble(name, defaultValue);
        headers.remove(name);
        return value;
    }

    @Override
    public Long getTimeMillisAndRemove(CharSequence name) {
        var value = getTimeMillis(name);
        headers.remove(name);
        return value;
    }

    @Override
    public long getTimeMillisAndRemove(CharSequence name, long defaultValue) {
        var value = getTimeMillis(name, defaultValue);
        headers.remove(name);
        return value;
    }

    @Override
    public boolean contains(CharSequence name) {
        return headers.contains(name);
    }

    @Override
    public boolean contains(CharSequence name, CharSequence value) {
        return headers.contains(name, value.toString(), false);
    }

    @Override
    public boolean containsObject(CharSequence name, Object value) {
        return contains(name, converter.convertObject(value));
    }

    @Override
    public boolean containsBoolean(CharSequence name, boolean value) {
        return contains(name, converter.convertBoolean(value));
    }

    @Override
    public boolean containsByte(CharSequence name, byte value) {
        return contains(name, converter.convertByte(value));
    }

    @Override
    public boolean containsChar(CharSequence name, char value) {
        return contains(name, converter.convertChar(value));
    }

    @Override
    public boolean containsShort(CharSequence name, short value) {
        return contains(name, converter.convertShort(value));
    }

    @Override
    public boolean containsInt(CharSequence name, int value) {
        return contains(name, converter.convertInt(value));
    }

    @Override
    public boolean containsLong(CharSequence name, long value) {
        return contains(name, converter.convertLong(value));
    }

    @Override
    public boolean containsFloat(CharSequence name, float value) {
        return contains(name, converter.convertFloat(value));
    }

    @Override
    public boolean containsDouble(CharSequence name, double value) {
        return contains(name, converter.convertDouble(value));
    }

    @Override
    public boolean containsTimeMillis(CharSequence name, long value) {
        return contains(name, converter.convertTimeMillis(value));
    }

    @Override
    public int size() {
        return headers.size();
    }

    @Override
    public boolean isEmpty() {
        return headers.isEmpty();
    }

    @Override
    public Set<CharSequence> names() {
        return new HashSet<>(headers.names());
    }

    @Override
    public Http1Headers add(CharSequence name, CharSequence value) {
        headers.add(name, value);
        return this;
    }

    @Override
    public Http1Headers add(CharSequence name, Iterable<? extends CharSequence> values) {
        headers.add(name, values);
        return this;
    }

    @Override
    public Http1Headers add(CharSequence name, CharSequence... values) {
        headers.add(name, Arrays.asList(values));
        return this;
    }

    @Override
    public Http1Headers addObject(CharSequence name, Object value) {
        headers.add(name, value);
        return this;
    }

    @Override
    public Http1Headers addObject(CharSequence name, Iterable<?> values) {
        headers.add(name, values);
        return this;
    }

    @Override
    public Http1Headers addObject(CharSequence name, Object... values) {
        headers.add(name, Arrays.asList(values));
        return this;
    }

    @Override
    public Http1Headers addBoolean(CharSequence name, boolean value) {
        headers.add(name, converter.convertBoolean(value));
        return this;
    }

    @Override
    public Http1Headers addByte(CharSequence name, byte value) {
        headers.add(name, converter.convertByte(value));
        return this;
    }

    @Override
    public Http1Headers addChar(CharSequence name, char value) {
        headers.add(name, converter.convertChar(value));
        return this;
    }

    @Override
    public Http1Headers addShort(CharSequence name, short value) {
        headers.add(name, value);
        return this;
    }

    @Override
    public Http1Headers addInt(CharSequence name, int value) {
        headers.add(name, value);
        return this;
    }

    @Override
    public Http1Headers addLong(CharSequence name, long value) {
        headers.add(name, converter.convertLong(value));
        return this;
    }

    @Override
    public Http1Headers addFloat(CharSequence name, float value) {
        headers.add(name, converter.convertFloat(value));
        return this;
    }

    @Override
    public Http1Headers addDouble(CharSequence name, double value) {
        headers.add(name, converter.convertDouble(value));
        return this;
    }

    @Override
    public Http1Headers addTimeMillis(CharSequence name, long value) {
        headers.add(name, converter.convertTimeMillis(value));
        return this;
    }

    @Override
    public Http1Headers add(Headers<? extends CharSequence, ? extends CharSequence, ?> headers) {
        headers.forEach(header -> add(header.getKey(), header.getValue()));
        return this;
    }

    @Override
    public Http1Headers set(CharSequence name, CharSequence value) {
        headers.set(name, value);
        return this;
    }

    @Override
    public Http1Headers set(CharSequence name, Iterable<? extends CharSequence> values) {
        headers.set(name, values);
        return this;
    }

    @Override
    public Http1Headers set(CharSequence name, CharSequence... values) {
        headers.set(name, Arrays.asList(values));
        return this;
    }

    @Override
    public Http1Headers setObject(CharSequence name, Object value) {
        headers.set(name, value);
        return this;
    }

    @Override
    public Http1Headers setObject(CharSequence name, Iterable<?> values) {
        headers.set(name, values);
        return this;
    }

    @Override
    public Http1Headers setObject(CharSequence name, Object... values) {
        headers.set(name, Arrays.asList(values));
        return this;
    }

    @Override
    public Http1Headers setBoolean(CharSequence name, boolean value) {
        headers.set(name, converter.convertBoolean(value));
        return this;
    }

    @Override
    public Http1Headers setByte(CharSequence name, byte value) {
        headers.set(name, converter.convertByte(value));
        return this;
    }

    @Override
    public Http1Headers setChar(CharSequence name, char value) {
        headers.set(name, converter.convertChar(value));
        return this;
    }

    @Override
    public Http1Headers setShort(CharSequence name, short value) {
        headers.set(name, value);
        return this;
    }

    @Override
    public Http1Headers setInt(CharSequence name, int value) {
        headers.set(name, value);
        return this;
    }

    @Override
    public Http1Headers setLong(CharSequence name, long value) {
        headers.set(name, converter.convertLong(value));
        return this;
    }

    @Override
    public Http1Headers setFloat(CharSequence name, float value) {
        headers.set(name, converter.convertFloat(value));
        return this;
    }

    @Override
    public Http1Headers setDouble(CharSequence name, double value) {
        headers.set(name, converter.convertDouble(value));
        return this;
    }

    @Override
    public Http1Headers setTimeMillis(CharSequence name, long value) {
        headers.set(name, converter.convertTimeMillis(value));
        return this;
    }

    @Override
    public Http1Headers set(Headers<? extends CharSequence, ? extends CharSequence, ?> headers) {
        return clear().setAll(headers);
    }

    @Override
    public Http1Headers setAll(Headers<? extends CharSequence, ? extends CharSequence, ?> headers) {
        headers.forEach(header -> set(header.getKey(), header.getValue()));
        return this;
    }

    @Override
    public boolean remove(CharSequence name) {
        if (headers.contains(name)) {
            headers.remove(name);
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public Http1Headers clear() {
        headers.clear();
        return this;
    }

    @Override @Nonnull
    public Iterator<Map.Entry<CharSequence, CharSequence>> iterator() {
        return headers.iteratorCharSequence();
    }
}
