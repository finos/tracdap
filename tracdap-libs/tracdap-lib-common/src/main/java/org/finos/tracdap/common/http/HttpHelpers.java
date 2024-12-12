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

import java.util.Arrays;

public class HttpHelpers {

    public static String joinPathSegments(String... segments) {

        if (segments.length == 0)
            throw new IllegalArgumentException("Zero path segments for joining");

        if (segments.length == 1)
            return segments[0];

        var result = Arrays.stream(segments)
                .reduce(HttpHelpers::joinPathSegments);

        return result.get();
    }

    public static String joinPathSegments(String segment1, String segment2) {

        if (segment1.endsWith("/")) {
            if (segment2.startsWith("/"))
                return segment1 + segment2.substring(1);
            else
                return segment1 + segment2;
        }
        else {
            if (segment2.startsWith("/"))
                return segment1 + segment2;
            else
                return segment1 + "/" + segment2;
        }
    }
}
