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

package org.finos.tracdap.common.cache;

import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.time.Instant;

public class CacheHelpers {

    private static final Logger log = LoggerFactory.getLogger(CacheHelpers.class);

    public static void checkValidTicket(CacheTicket ticket, String operation, Instant operationTime) {

        if (ticket == null)
            throw new ETracInternal("Cache ticket is null");

        if (ticket.missing()) {
            var message = String.format("Cannot %s [%s], item is not in the cache", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }

        if (ticket.superseded()) {
            var message = String.format("Cannot %s [%s], ticket has been superseded", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }

        if (operationTime.isAfter(ticket.expiry())) {
            var message = String.format("Failed to %s [%s], ticket has expired", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }
    }

    public static void checkValidStatus(CacheTicket ticket, String status) {

        if (status == null)
            throw new ETracInternal(String.format("Cache ticket status is null for [%s]", ticket.key()));

        if (!MetadataConstants.VALID_IDENTIFIER.matcher(status).matches()) {
            var message = String.format("Cannot set status [%s] for [%s] (status must be a valid identifier)", status, ticket.key());
            log.error(message);
            throw new ECacheOperation(message);
        }

        if (MetadataConstants.TRAC_RESERVED_IDENTIFIER.matcher(status).matches()) {
            var message = String.format("Cannot set status [%s] for [%s] (status cannot be a reserved identifier)r", status, ticket.key());
            log.error(message);
            throw new ECacheOperation(message);
        }
    }

    public static <TValue extends Serializable>
    void checkValidValue(CacheTicket ticket, TValue value) {

        if (value == null)
            throw new ETracInternal(String.format("Cache value is null for [%s]", ticket.key()));
    }

    public static void checkKey(String key) {

        if (key == null)
            throw new IllegalArgumentException(new NullPointerException());

        if (!IJobCache.VALID_KEY.matcher(key).matches())
            throw new IllegalArgumentException();

        if (MetadataConstants.TRAC_RESERVED_IDENTIFIER.matcher(key).matches())
            throw new IllegalArgumentException();
    }

    public static void checkRevision(int revision) {

        if (revision < 0)
            throw new IllegalArgumentException();
    }

    public static void checkDuration(Duration duration) {

        if (duration == null)
            throw new IllegalArgumentException(new NullPointerException());

        if (duration.isZero() || duration.isNegative())
            throw new IllegalArgumentException();
    }

    public static void checkTicket(CacheTicket ticket) {

        if (ticket == null)
            throw new IllegalArgumentException(new NullPointerException());
    }

    public static void checkMaxDuration(String key, Duration duration) {

        if (duration.compareTo(CacheTicket.MAX_DURATION) > 0) {

            var message = String.format(
                    "Requested ticket duration of [%s] for [%s] exceeds the maximum grant time",
                    duration, key);

            throw new ECacheTicket(message);
        }
    }

    public static <TValue extends Serializable>
    byte[] encodeValue(TValue value) {

        try (var stream = new ZCByteArrayOutputStream(); var serialize = new ObjectOutputStream(stream)) {

            serialize.writeObject(value);
            serialize.flush();

            return stream.toByteArray();
        }
        catch (IOException e) {
            throw new EUnexpected(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <TValue extends Serializable>
    TValue decodeValue(byte[] encodedValue) throws ECacheCorruption {

        try (var stream = new ByteArrayInputStream(encodedValue); var deserialize = new ObjectInputStream(stream)) {

            var object = deserialize.readObject();

            return (TValue) object;
        }
        catch (InvalidClassException e) {
            var message = "The job cache references an old code version for class [" + e.classname + "]";
            throw new ECacheCorruption(message, e);
        }
        catch (ClassNotFoundException e) {
            var message = "The job cache references code for an unknown class [" + e.getMessage() + "]";
            throw new ECacheCorruption(message, e);
        }
        catch (IOException e) {
            var message = "The job cache contains a corrupt entry that cannot be decoded";
            throw new ECacheCorruption(message, e);
        }
    }

    private static class ZCByteArrayOutputStream extends ByteArrayOutputStream {
        @Override
        public byte[] toByteArray() {
            return this.buf;
        }
    }
}
