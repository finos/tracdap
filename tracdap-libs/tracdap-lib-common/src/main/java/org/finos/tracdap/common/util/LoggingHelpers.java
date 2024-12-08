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

package org.finos.tracdap.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class LoggingHelpers {

    public static String formatFileSize(long size) {

        if (size < 1024) {

            if (size == 0)
                return "0 bytes";
            else if (size == 1)
                return "1 byte";
            else
                return String.format("%d bytes", size);
        }

        if (size < 1024 * 1024) {
            var kb = ((double) size) / 1024.0;
            return String.format("%.1f KB", kb);
        }

        if (size < 1024 * 1024 * 1024) {
            var mb = ((double) size) / (1024.0 * 1024.0);
            return String.format("%.1f MB", mb);
        }

        var gb = ((double) size) / (1024.0 * 1024.0 * 1024.0);
        return String.format("%.1f GB", gb);
    }

    public static Logger threadLocalLogger(Object obj, ThreadLocal<Logger> logMap) {

        var log = logMap.get();

        if (log == null) {
            log = LoggerFactory.getLogger(obj.getClass());
            logMap.set(log);
        }

        return log;
    }

    public static Logger typedThreadLocalLogger(Object obj, ThreadLocal<Map<Class<?>, Logger>> logMap) {

        var map = logMap.get();

        if (map == null) {
            map = new HashMap<>();
            logMap.set(map);
        }

        var log = map.get(obj.getClass());

        if (log == null) {
            log = LoggerFactory.getLogger(obj.getClass());
            map.put(obj.getClass(), log);
        }

        return log;
    }
}
