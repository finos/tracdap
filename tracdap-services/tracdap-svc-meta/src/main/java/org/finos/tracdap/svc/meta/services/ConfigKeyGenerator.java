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

package org.finos.tracdap.svc.meta.services;

import org.finos.tracdap.common.exception.EMetadataDuplicate;

import java.security.SecureRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;


class ConfigKeyGenerator {

    // Crockford base32, which excludes I, L, O and U
    static final String KEY_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    static final String KEY_PREFIX = "U";
    static final int KEY_LENGTH = 6;
    static final int MAX_ATTEMPTS = 5;

    private static final SecureRandom RANDOM = new SecureRandom();

    private final Supplier<String> candidates;

    ConfigKeyGenerator() {
        this(ConfigKeyGenerator::randomKey);
    }

    ConfigKeyGenerator(Supplier<String> candidates) {
        this.candidates = candidates;
    }

    String generateKey(Predicate<String> isFree) {

        for (var attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {

            var candidate = candidates.get();

            if (isFree.test(candidate))
                return candidate;
        }

        throw new EMetadataDuplicate("Failed to generate a unique config key, please retry");
    }

    private static String randomKey() {

        var key = new StringBuilder(KEY_PREFIX);

        for (var i = 0; i < KEY_LENGTH; i++)
            key.append(KEY_ALPHABET.charAt(RANDOM.nextInt(KEY_ALPHABET.length())));

        return key.toString();
    }
}
