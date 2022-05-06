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

package org.finos.tracdap.test.helpers;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class GitHelpers {

    public static String getCurrentCommit() throws Exception {

        var pb = new ProcessBuilder();
        pb.command("git", "rev-parse", "HEAD");

        var proc = pb.start();

        try {
            proc.waitFor(10, TimeUnit.SECONDS);

            var procResult = proc.getInputStream().readAllBytes();
            return new String(procResult, StandardCharsets.UTF_8).strip();
        }
        finally {
            proc.destroy();
        }
    }
}
