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

package org.finos.tracdap.svc.orch.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;


public class JobProcessorHelpersTest {

    @Test
    void truncateErrorMessage_short() {

        var message = "Model failed\nsecond line";

        Assertions.assertEquals(message, JobProcessorHelpers.truncateErrorMessage(message));
    }

    @Test
    void truncateErrorMessage_exactLimit() {

        var message = "x".repeat(JobProcessorHelpers.ERROR_MESSAGE_MAX_BYTES);

        Assertions.assertEquals(message, JobProcessorHelpers.truncateErrorMessage(message));
    }

    @Test
    void truncateErrorMessage_long() {

        var message = "x".repeat(JobProcessorHelpers.ERROR_MESSAGE_MAX_BYTES + 1);
        var truncated = JobProcessorHelpers.truncateErrorMessage(message);

        Assertions.assertEquals(JobProcessorHelpers.ERROR_MESSAGE_MAX_BYTES, utf8Length(truncated));
        Assertions.assertTrue(truncated.endsWith(JobProcessorHelpers.ERROR_MESSAGE_TRUNCATED));
        Assertions.assertTrue(message.startsWith(truncated.substring(0, truncated.length() - JobProcessorHelpers.ERROR_MESSAGE_TRUNCATED.length())));
    }

    @Test
    void truncateErrorMessage_multiByteCharacters() {

        // 3-byte and 4-byte (surrogate pair) characters must not be split
        var message = "你好𝜌".repeat(1000);
        var truncated = JobProcessorHelpers.truncateErrorMessage(message);
        var kept = truncated.substring(0, truncated.length() - JobProcessorHelpers.ERROR_MESSAGE_TRUNCATED.length());

        Assertions.assertTrue(utf8Length(truncated) <= JobProcessorHelpers.ERROR_MESSAGE_MAX_BYTES);
        Assertions.assertTrue(message.startsWith(kept));
        Assertions.assertFalse(Character.isHighSurrogate(kept.charAt(kept.length() - 1)));
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }
}
