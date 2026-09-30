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
import org.finos.tracdap.common.validation.ValidationConstants;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;


class ConfigKeyGeneratorTest {

    private static final Pattern KEY_FORMAT = Pattern.compile("\\AU[0-9A-HJKMNP-TV-Z]{6}\\Z");

    @Test
    void generatedKeyFormat() {

        var generator = new ConfigKeyGenerator();
        var keys = new HashSet<String>();

        for (var i = 0; i < 1000; i++) {

            var key = generator.generateKey(k -> true);

            assertTrue(KEY_FORMAT.matcher(key).matches(), key);
            assertTrue(ValidationConstants.CONFIG_KEY.matcher(key).matches(), key);
            keys.add(key);
        }

        assertTrue(keys.size() > 990);
    }

    @Test
    void retriesOnClash() {

        var candidates = List.of("UAAAAAA", "UBBBBBB", "UCCCCCC").iterator();
        var generator = new ConfigKeyGenerator(candidates::next);

        var key = generator.generateKey(k -> k.equals("UCCCCCC"));

        assertEquals("UCCCCCC", key);
    }

    @Test
    void failsAfterMaxAttempts() {

        var generator = new ConfigKeyGenerator(() -> "UAAAAAA");

        assertThrows(EMetadataDuplicate.class, () -> generator.generateKey(k -> false));
    }
}
