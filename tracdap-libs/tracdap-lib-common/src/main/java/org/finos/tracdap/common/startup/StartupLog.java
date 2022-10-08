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

package org.finos.tracdap.common.startup;

import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;


public class StartupLog {

    private static boolean logSystemActive = false;

    public static void setLogSystemActive() {
        logSystemActive = true;
    }

    public static void log(Object obj, Level level, String message) {

        if (logSystemActive) {

            var log = LoggerFactory.getLogger(obj.getClass());

            switch (level) {

                case ERROR:
                    log.error(message);
                    break;

                case WARN:
                    log.warn(message);
                    break;

                case DEBUG:
                    log.debug(message);
                    break;

                case TRACE:
                    log.trace(message);
                    break;

                case INFO:
                default:
                    log.info(message);
            }
        }
        else {

            if (level.toInt() <= Level.INFO.toInt())
                System.out.println(message);
            else
                System.err.println(message);
        }
    }
}
